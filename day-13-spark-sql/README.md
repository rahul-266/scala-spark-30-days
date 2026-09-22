# Day 13 — Spark SQL Basics

## Overview

This project demonstrates the fundamentals of **Apache Spark SQL** using customer data stored in CSV and JSON formats.

The project uses Spark DataFrames to load, inspect, transform, filter, and analyze customer information. It also demonstrates how a DataFrame can be registered as a temporary SQL view and queried using standard SQL.

The final output is a **customer analytics report** containing customer and city-level spending information.

---

## Objective

The main objectives of this practice are:

- Create a Spark DataFrame from CSV data.
- Create a Spark DataFrame from JSON data.
- Inspect DataFrame schemas.
- Select required columns.
- Filter customer records.
- Add derived columns using `withColumn()`.
- Use Spark column expressions.
- Register a DataFrame as a temporary view.
- Execute SQL queries using Spark SQL.
- Build a customer analytics report.

---

## Scenario

A company maintains customer information from different data sources.

Customer data is received in:

- CSV format
- JSON format

The data needs to be processed using Spark and combined into a common DataFrame.

The analytics team wants to identify:

- Active customers
- High-value customers
- Customer spending categories
- Average spending by city
- Total spending by city
- Overall customer statistics

Spark SQL is used to perform these analytical operations.

---

## Technologies Used

| Technology | Version / Purpose |
|------------|------------------|
| Scala | 2.12.18 |
| Apache Spark | 3.5.6 |
| Spark SQL | DataFrame and SQL processing |
| SBT | 2.0.7 |
| Java | 17 |
| File Formats | CSV, JSON |

---

## Project Architecture

```text
              Customer Data
             /             \
            /               \
       CSV File           JSON File
          |                   |
          v                   v
   Spark DataFrame     Spark DataFrame
          \                   /
           \                 /
            ------ Combine ------
                    |
                    v
             Unified DataFrame
                    |
        +-----------+-----------+
        |           |           |
        v           v           v
     Select      Filter      withColumn
                                |
                                v
                       Enriched DataFrame
                                |
                                v
                     Temporary SQL View
                                |
                                v
                         Spark SQL Queries
                                |
                                v
                     Customer Analytics
