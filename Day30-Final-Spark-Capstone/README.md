# Day 30 - Final Spark Capstone

## Project
Banking Transaction Analytics using Scala and Apache Spark.

## Features
- CSV ingestion and data cleaning
- Duplicate transaction removal
- Pair RDD account-level counting
- Account and city analytics
- High-value transaction alerts
- Spark SQL reporting
- MEMORY_AND_DISK persistence
- CSV report generation

## Requirements
- Java 17
- Scala 2.12.18
- Apache Spark 3.5.6
- sbt

## Run
sbt compile
sbt "runMain com.capstone.CapstoneApp"

## Output
- output/account_summary
- output/suspicious_alerts
- output/city_summary
- output/sql_report

Note: This version is a batch analytics capstone, not a live Kafka streaming pipeline.
