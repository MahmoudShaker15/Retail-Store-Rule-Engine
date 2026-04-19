# Retail Store Discount Rule Engine
A functional rule engine built with Scala that processes retail transactions, applies discount rules, and stores the results in a database.
## Overview

The engine reads transactions from a CSV file, evaluates each one against a set of discount rules, calculates the final price, and saves the results to a SQLite database, all while logging every step to a log file.

## Key Features

- **4 Discount Rules:** expiry proximity, product category (cheese/wine), special day (March 23rd), and bulk quantity
- **Higher Order Functions:** rules are stored as a list of function pairs and applied with map and filter, making it easy to add new rules without changing any other part of the engine
- **Functional Style:** pure functions, immutable data, no loops, no vars
- **Smart Aggregation:** if a transaction qualifies for multiple discounts, the top 2 are averaged
- **Safe Error Handling:** uses Either and Try instead of exceptions, bad CSV rows - are skipped and logged without stopping the engine
- **Automatic Logging:** every engine event is recorded in rules_engine.log with timestamp and severity level
- **Database Output:** all processed transactions with their discounts and final prices are saved to retail_engine.db