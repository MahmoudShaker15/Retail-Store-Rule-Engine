import java.io.FileWriter
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import scala.io.Source
import scala.util.{Failure, Success, Try}

object RuleEngine extends App {

  // Identify log levels
  sealed trait LogLevel
  case object INFO  extends LogLevel
  case object WARN  extends LogLevel
  case object ERROR extends LogLevel

  // Create data model for transaction before and after processing
  case class Transaction(
     timestamp: LocalDateTime,
     productName: String,
     expiryDate: LocalDate,
     quantity: Int,
     unitPrice: Double,
     channel: String,
     paymentMethod: String
     )

  case class ProcessedTransaction(
    transaction: Transaction,
    finalDiscount: Double,
    finalPrice: Double
    )

  // logger object to use it when I want
  object Logger {

    // format the date and time
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

    // function that write logs in file
    def log(level: LogLevel, message: String): Unit = {
      val timestamp = LocalDateTime.now().format(formatter)
      val line = s"$timestamp  $level  $message"
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
    val cols = line.split(",", -1)

    if (cols.length != 7)
      return Left(s"Expected 7 columns but got ${cols.length} in row: $line")

    val Array(tsStr, productName, expStr, qtyStr, priceStr, channel, paymentMethod) = cols

    val tsResult = Try(LocalDateTime.parse(tsStr.trim, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")))
      .toEither.left.map(e => s"Bad timestamp '$tsStr': ${e.getMessage}")
    val expResult = Try(LocalDate.parse(expStr.trim, DateTimeFormatter.ofPattern("yyyy-MM-dd")))
      .toEither.left.map(e => s"Bad expiry '$expStr': ${e.getMessage}")
    val qtyResult = Try(qtyStr.trim.toInt)
      .toEither.left.map(e => s"Bad quantity '$qtyStr': ${e.getMessage}")
    val priceResult = Try(priceStr.trim.toDouble)
      .toEither.left.map(e => s"Bad price '$priceStr': ${e.getMessage}")

    for {
      ts <- tsResult
      exp <- expResult
      qty <- qtyResult
      price <- priceResult
    } yield Transaction(ts, productName.trim, exp, qty, price, channel.trim, paymentMethod.trim)
  }

  // Load CSV file from path to List of transactions based on each line
  def loadTransactions(filePath: String): Either[String, List[Transaction]] = {
    Logger.log(INFO, s"Opening file: $filePath")

    Try(Source.fromFile(filePath)) match {
      case Failure(e) =>
        Logger.log(ERROR, s"Cannot open file '$filePath': ${e.getMessage}")
        Left(s"Cannot open file '$filePath': ${e.getMessage}")

      case Success(source) =>
        val lines = source.getLines().toList
        source.close()

        val dataLines = lines.filter(line =>
          line.trim.nonEmpty && !line.startsWith("timestamp")
        )

        Logger.log(INFO, s"Found ${dataLines.length} rows to process")

        val transactions = dataLines.flatMap { line =>
          parseLine(line) match {
            case Right(tx) => Some(tx)
            case Left(err) =>
              Logger.log(WARN, s"Skipping bad row: $err")
              None
          }
        }

        Logger.log(INFO, s"Successfully parsed ${transactions.length} transactions")
        Right(transactions)
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

  // Rules list
  val allRules: List[Rule] = List(
    (isExpiryQualified, expiryDiscount),
    (isCategoryQualified, categoryDiscount),
    (isSpecialDayQualified, specialDayDiscount),
    (isQuantityQualified, quantityDiscount)
  )

  // Discount calculator
  def calculateDiscount(tx: Transaction): Double = {
    val qualifiedDiscounts = allRules
      .filter(rule => rule._1(tx))
      .map(rule => rule._2(tx))
      .sorted
      .reverse

    qualifiedDiscounts match {
      case Nil => 0.0
      case head :: Nil => head
      case top2 => top2.take(2).sum / 2
    }
  }

  // function that process transaction and return processed table with discount and final price
  def processTransaction(tx: Transaction): ProcessedTransaction = {
    val discount = calculateDiscount(tx)
    val finalPrice = BigDecimal(tx.quantity * tx.unitPrice * (1 - discount))
      .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble

    Logger.log(INFO, s"Processed: ${tx.productName} | discount: ${discount * 100}% | final price: $finalPrice")

    ProcessedTransaction(tx, discount, finalPrice)
  }

  // ── CSV Writer ──────────────────────────────────────────────────
  def saveToCSV(records: List[ProcessedTransaction]): Either[String, Unit] = {
    Logger.log(INFO, "Writing results to results.csv...")
    Try {
      val writer = new FileWriter("src/main/scala/Retail-Store-Rule-Engine/results.csv")
      // Write the header row first
      writer.write("timestamp,product_name,expiry_date,quantity,unit_price,channel,payment_method,final_discount,final_price\n")

      // Write one row per processed transaction
      records.foreach { ptx =>
        val tx = ptx.transaction
        val row = s"${tx.timestamp},${tx.productName},${tx.expiryDate},${tx.quantity},${tx.unitPrice},${tx.channel},${tx.paymentMethod},${ptx.finalDiscount},${ptx.finalPrice}\n"
        writer.write(row)
      }

      writer.close()
    }.toEither.left.map(e => s"Cannot write results: ${e.getMessage}")
  }

  // ── Main Runner ─────────────────────────────────────────────────
  Logger.log(INFO, "=== Rule Engine Started ===")

  loadTransactions("src/main/resources/TRX1000.csv") match {
    case Left(err) =>
      Logger.log(ERROR, s"Engine stopped: $err")

    case Right(transactions) =>
      Logger.log(INFO, s"Processing ${transactions.length} transactions...")
      val processed = transactions.map(tx => processTransaction(tx))

      saveToCSV(processed) match {
        case Left(err) =>
          Logger.log(ERROR, s"Failed to save to database: $err")
        case Right(_) =>
          Logger.log(INFO, "=== Rule Engine Finished Successfully ===")
      }
  }

}