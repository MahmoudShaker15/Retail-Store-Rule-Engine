import java.io.FileWriter
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import scala.io.Source
import scala.util.{Failure, Success, Try}
import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.collection.parallel.CollectionConverters._

object RuleEngine extends App {

  // The three possible log levels we can use when writing to the log file
  trait LogLevel
  case object INFO  extends LogLevel
  case object WARN  extends LogLevel
  case object ERROR extends LogLevel

  // Represents one raw transaction exactly as it comes from the CSV
  case class Transaction(
     timestamp: LocalDateTime,
     productName: String,
     expiryDate: LocalDate,
     quantity: Int,
     unitPrice: Double,
     channel: String,
     paymentMethod: String
     )

  // Represents transaction with calculated columns
  case class ProcessedTransaction(
    transaction: Transaction,
    finalDiscount: Double,
    finalPrice: Double
    )

  // logger object to use it when I want
  object Logger {

    // Defines the format of timestamp in each log line
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss")

    def log(level: LogLevel, message: String): Unit = {
      // Capture the current moment and build the full log line
      val timestamp = LocalDateTime.now().format(formatter)
      val line = s"$timestamp  $level  $message"
      // Try to write to the file, and if it fails print the error to terminal
      Try {
        val writer = new FileWriter("src/main/scala/Retail-Store-Rule-Engine/rules_engine.log", true)
        writer.write(line + "\n")
        writer.close()
      } match {
        case Success(_) => ()
        case Failure(e) => println(s"LOGGING ERROR: ${e.getMessage}")
      }
    }
  }

  // Parse columns in CSV to handle those data types
  def parseLine(line: String): Either[String, Transaction] = {
    // Split the line into columns, -1 keeps empty fields instead of dropping them
    val cols = line.split(",", -1)

    if (cols.length != 7)
      return Left(s"Expected 7 columns but got ${cols.length} in row: $line")

    // Give each column a meaningful name instead of using indexes
    val Array(tsStr, productName, expStr, qtyStr, priceStr, channel, paymentMethod) = cols

    // Try to convert each column to its correct type
    val tsResult = Try(LocalDateTime.parse(tsStr.trim, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")))
      .toEither.left.map(e => s"Bad timestamp '$tsStr': ${e.getMessage}")
    val expResult = Try(LocalDate.parse(expStr.trim, DateTimeFormatter.ofPattern("yyyy-MM-dd")))
      .toEither.left.map(e => s"Bad expiry '$expStr': ${e.getMessage}")
    val qtyResult = Try(qtyStr.trim.toInt)
      .toEither.left.map(e => s"Bad quantity '$qtyStr': ${e.getMessage}")
    val priceResult = Try(priceStr.trim.toDouble)
      .toEither.left.map(e => s"Bad price '$priceStr': ${e.getMessage}")

    // If all 4 conversions succeed build the Transaction
    for {
      ts <- tsResult
      exp <- expResult
      qty <- qtyResult
      price <- priceResult
    } yield Transaction(ts, productName.trim, exp, qty, price, channel.trim, paymentMethod.trim)
  }

  // Load CSV file from path to List of transactions based on each line
  def loadTransactions(filePath: String): Either[String, Unit] = {
    Logger.log(INFO, s"Opening file: $filePath")

    Try(Source.fromFile(filePath)) match {
      case Failure(e) =>
        Logger.log(ERROR, s"Cannot open file '$filePath': ${e.getMessage}")
        Left(s"Cannot open file '$filePath': ${e.getMessage}")

      case Success(source) =>
        val lines = source.getLines()
          .filterNot(line => line.trim.isEmpty || line.startsWith("timestamp"))

        // Process and save chunk by chunk — never load full file into memory
        lines
          .grouped(2500000)                         // take 100k lines at a time
          .foreach { chunk =>
            // Parse chunk in parallel
            val transactions = chunk.par.flatMap { line =>
              parseLine(line) match {
                case Right(tx) => Some(tx)
                case Left(_) => None
              }
            }.toList

            Logger.log(INFO, s"Parsed ${transactions.size} transactions, processing...")

            // Process chunk in parallel
            val processed = transactions.par
              .map(tx => processTransaction(tx))
              .toList

            Logger.log(INFO, s"Processed ${processed.size} transactions, saving to DB...")

            // Write this chunk to DB immediately then discard it from memory
            saveTransactions(processed)
          }

        source.close()
        Logger.log(INFO, "All chunks processed and saved successfully")
        Right(())
    }
  }

  // Identify type for rules (qualifies and calculations)
  type Rule = (Transaction => Boolean, Transaction => Double)

  // Rule 1 - Expiry Proximity
  def isExpiryQualified(tx: Transaction): Boolean = {
    val daysRemaining = ChronoUnit.DAYS.between(tx.timestamp.toLocalDate, tx.expiryDate)
    daysRemaining > 0 && daysRemaining < 30
  }

  def expiryDiscount(tx: Transaction): Double = {
    val daysRemaining = ChronoUnit.DAYS.between(tx.timestamp.toLocalDate, tx.expiryDate)
    (30 - daysRemaining) / 100.0
  }

  // Rule 2 - Product Category
  def isCategoryQualified(tx: Transaction): Boolean = {
    val name = tx.productName.toLowerCase
    name.contains("cheese") || name.contains("wine")
  }

  def categoryDiscount(tx: Transaction): Double =
    if (tx.productName.toLowerCase.contains("cheese")) 0.10
    else 0.05

  // Rule 3 - Special Day
  def isSpecialDayQualified(tx: Transaction): Boolean = {
    val date = tx.timestamp.toLocalDate
    date.getMonthValue == 3 && date.getDayOfMonth == 23
  }

  def specialDayDiscount(tx: Transaction): Double = 0.50

  // Rule 4 - Bulk Quantity
  def isQuantityQualified(tx: Transaction): Boolean = tx.quantity > 5

  def quantityDiscount(tx: Transaction): Double = tx.quantity match {
    case q if q >= 6  && q <= 9  => 0.05
    case q if q >= 10 && q <= 14 => 0.07
    case _ => 0.10
  }

  // Rule 5 - App Channel
  def isAppQualified(tx: Transaction): Boolean = tx.channel.toLowerCase == "app"

  def appDiscount(tx: Transaction): Double = {
    val roundedUp = math.ceil(tx.quantity / 5.0) * 5
    (roundedUp / 5) * 0.05
  }

  // Rule 6 - Visa Card Payment
  def isVisaQualified(tx: Transaction): Boolean = tx.paymentMethod.toLowerCase == "visa"

  def visaDiscount(tx: Transaction): Double = 0.05

  // All rules in one list so we can apply them all with a single map and filter
  val allRules: List[Rule] = List(
    (isExpiryQualified, expiryDiscount),
    (isCategoryQualified, categoryDiscount),
    (isSpecialDayQualified, specialDayDiscount),
    (isQuantityQualified, quantityDiscount),
    (isAppQualified, appDiscount),
    (isVisaQualified, visaDiscount)
  )

  // Discount calculator
  def calculateDiscount(tx: Transaction): Double = {
    // Apply every rule to the transaction, keep only the qualifying ones and get their discount values
    val qualifiedDiscounts = allRules
      .filter(rule => rule._1(tx))
      .map(rule => rule._2(tx))
      .sorted
      .reverse

    // No qualifying rules = 0%, one rule = use it directly, more than one = average the top 2
    qualifiedDiscounts match {
      case Nil => 0.0
      case head :: Nil => head
      case top2 =>
        BigDecimal(top2.take(2).sum / 2)
          .setScale(4, BigDecimal.RoundingMode.HALF_UP)
          .toDouble
    }
  }

  // function that process transaction and return processed table with discount and final price
  def processTransaction(tx: Transaction): ProcessedTransaction = {
    val discount = calculateDiscount(tx)
    // Multiply quantity by unit price then apply the discount, rounded to 2 decimal places for currency
    val finalPrice = BigDecimal(tx.quantity * tx.unitPrice * (1 - discount))
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble

    ProcessedTransaction(tx, discount, finalPrice)
  }

  // Opens a connection to the SQLite database file
  def getConnection(): Either[String, Connection] = {
    Logger.log(INFO, "Connecting to database...")
    Try(DriverManager.getConnection("jdbc:sqlite:src/main/scala/Retail-Store-Rule-Engine/retail_engine.db"))
      .toEither
      .left.map(e => s"Cannot connect to DB: ${e.getMessage}")
  }

  // Creates the output table only if it doesn't already exist
  def createTable(conn: Connection): Either[String, Unit] = {
    Try {
      val stmt = conn.createStatement()
      stmt.execute(
        """CREATE TABLE IF NOT EXISTS processed_transactions (
          | timestamp      TEXT,
          | product_name   TEXT,
          | expiry_date    TEXT,
          | quantity       INTEGER,
          | unit_price     REAL,
          | channel        TEXT,
          | payment_method TEXT,
          | final_discount REAL,
          | final_price    REAL
          |)""".stripMargin
      )
      stmt.close()
    }.toEither.left.map(e => s"Cannot create table: ${e.getMessage}")
  }

  def saveTransactions(records: List[ProcessedTransaction]): Either[String, Unit] = {
    getConnection() match {
      case Left(err) => Left(err)
      case Right(conn) =>
        createTable(conn) match {
          case Left(err) => Left(err)
          case Right(_) =>
            Try {
              Logger.log(INFO, "Writing to database...")
              val sql =
                """INSERT INTO processed_transactions
                  |(timestamp, product_name, expiry_date, quantity, unit_price,
                  | channel, payment_method, final_discount, final_price)
                  |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin

              val ps = conn.prepareStatement(sql)

              // Wrap everything in one transaction — SQLite commits once at the end
              conn.setAutoCommit(false)

              // Add every record to the batch without executing yet
              records.foreach { ptx =>
                ps.setString(1, ptx.transaction.timestamp.toString)
                ps.setString(2, ptx.transaction.productName)
                ps.setString(3, ptx.transaction.expiryDate.toString)
                ps.setInt(4, ptx.transaction.quantity)
                ps.setDouble(5, ptx.transaction.unitPrice)
                ps.setString(6, ptx.transaction.channel)
                ps.setString(7, ptx.transaction.paymentMethod)
                ps.setDouble(8, ptx.finalDiscount)
                ps.setDouble(9, ptx.finalPrice)
                ps.addBatch()
              }

              ps.executeBatch() // <-- sends ALL rows to DB in one shot
              conn.commit()     // <-- SQLite writes everything to disk once
              ps.close()

            }.toEither.left.map { e =>
              Try(conn.rollback()) // if anything fails, undo the whole chunk
              s"Batch insert failed: ${e.getMessage}"
            } match {
              case Left(err) =>
                conn.close()
                Logger.log(WARN, s"Chunk failed to save: $err")
                Left(err)
              case Right(_) =>
                conn.close()
                Logger.log(INFO, s"Successfully saved ${records.size} records to database")
                Right(())
            }
        }
    }
  }

  // Main Runner
  Logger.log(INFO, "=== Rule Engine Started ===")

  loadTransactions("src/main/resources/TRX10M.csv") match {
    case Left(err) => Logger.log(ERROR, s"Engine stopped: $err")
    case Right(_) => Logger.log(INFO, "=== Rule Engine Finished Successfully ===")
  }

}