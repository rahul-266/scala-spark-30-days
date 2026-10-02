# Day 17 — Window Functions

## Overview
Practice of Spark SQL Window Functions using Scala.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.6
- SBT 2.0.7

## Features
- row_number(), rank() and dense_rank()
- Partitioning by department, course and route
- Latest policy per customer
- lag() and lead()
- Top 3 students per course
- Employee salary ranking
- Route-wise fare ranking

## Project Structure

day-17-window-functions/
├── data/
│   ├── students.csv
│   ├── employees.csv
│   ├── customer_policies.csv
│   └── route_bookings.csv
├── project/
│   └── build.properties
├── src/main/scala/
│   └── WindowFunctionsPractice.scala
├── build.sbt
└── README.md

## How to Run

Compile:
sbt -batch compile

Run:
sbt -batch "runMain WindowFunctionsPractice"

## Scenarios
1. Find the top 3 students per course.
2. Rank employees by salary within each department.
3. Find the latest policy for each customer.
4. Compare previous and next policy premiums using lag and lead.
5. Rank booking fares within each route.

## Window Functions
- row_number(): Assigns a unique sequential number.
- rank(): Assigns ranks and leaves gaps after ties.
- dense_rank(): Assigns ranks without gaps.
- lag(): Gets a value from a preceding row.
- lead(): Gets a value from a following row.

## Data Files
- students.csv: Student scores by course.
- employees.csv: Employee salaries by department.
- customer_policies.csv: Customer policy history.
- route_bookings.csv: Route booking history.
