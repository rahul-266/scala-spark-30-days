# Day 20 — File Formats and Output

## Overview
This project demonstrates how to read and write CSV,
JSON and Parquet files using Scala and Apache Spark.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.6
- SBT 2.0.7

## Features
- CSV read and write
- JSON read and write
- Parquet read and write
- Daily sales aggregation
- Partitioned Parquet output
- Repartition before writing
- Data-file count verification

## Scenario
Store daily sales data using year, month and day
as partition columns.

## Project Structure
- data/daily_sales.csv
- src/main/scala/FileFormatsPractice.scala
- build.sbt
- README.md

## Compile
sbt -batch compile

## Run
sbt -batch "runMain FileFormatsPractice"

## Output
- output/sales_csv
- output/sales_json
- output/sales_parquet
- output/sales_partitioned

## Partitioning
The partitioned Parquet output uses year, month
and day directory names.

## Repartition
repartition(4, year, month, day) redistributes rows
into four Spark partitions using the date keys.
The number of generated data files depends on
the partition distribution and output tasks.

## Formats
- CSV: Text-based tabular format.
- JSON: Record-oriented, semi-structured format.
- Parquet: Columnar format, useful for analytical workloads.
