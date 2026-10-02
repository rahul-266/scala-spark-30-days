# Day 22 — E-commerce Batch Mini Project

## Overview
An end-to-end batch data pipeline using Scala and Spark SQL.
The pipeline cleans e-commerce transactions, joins customer
and product data, aggregates daily sales and writes Parquet.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.6
- SBT 2.0.7
- Parquet

## Features
- Raw CSV ingestion
- Invalid transaction validation
- Data cleaning and type conversion
- Customer and product joins
- Unmatched reference handling
- Revenue calculations
- Daily sales aggregations
- Year/month/day partitioned Parquet
- Output file count verification

## Project Structure
- data/raw_transactions.csv
- data/customers.csv
- data/products.csv
- src/main/scala/EcommerceBatchPipeline.scala
- build.sbt
- README.md

## Compile
sbt -batch compile

## Run
sbt -batch "runMain EcommerceBatchPipeline"

## Output Directories
- output/rejected_transactions
- output/clean_transactions
- output/unmatched_references
- output/enriched_transactions
- output/daily_sales_partitioned

## Daily Sales Partitioning
The aggregated output is partitioned by year, month
and day using Parquet.

## Sample Data
The sample has 14 transactions, including invalid records
and a transaction with unmatched customer/product references.

The pipeline reports raw, invalid, clean, unmatched
and enriched record counts.
