# Retail Store Discount Rule Engine
A functional rule engine built with Scala that processes retail transactions, applies discount rules, and stores the results in a database.
## Overview

The engine reads transactions from a CSV file, evaluates each one against a set of discount rules, calculates the final price, and saves the results to a SQLite database, all while logging every step to a log file.

<img width="1753" height="517" alt="Flow of Engine" src="https://github.com/user-attachments/assets/acfb38fb-9129-4f6f-bb7f-c3ad8f39c0a5" />

## Key Features

- **6 Discount Rules:** expiry proximity, product category (cheese/wine), special day (March 23rd), bulk quantity, app channel, and Visa card payment
- **Higher Order Functions:** rules are stored as a list of function pairs and applied with map and filter, making it easy to add new rules without changing any other part of the engine
- **Functional Style:** pure functions, immutable data, no loops, no vars
- **Smart Aggregation:** if a transaction qualifies for multiple discounts, the top 2 are averaged
- **Chunk-Based Streaming:** the CSV is read and processed in chunks of 250,000 rows at a time so the full file is never loaded into memory, making it safe to run on files of any size
- **Parallel Processing:** reducing total runtime on large datasets by parse and process each chunk in parallel across all available CPU cores.
- **Batch Database Writes:** processed records are inserted into SQLite in a single batched transaction per chunk instead of row by row
- **Automatic Logging:** every engine event is recorded in rules_engine.log with timestamp and severity level
- **Database Output:** all processed transactions with their discounts and final prices are saved to retail_engine.db
