# Day 14 — DataFrame and Dataset

## Objective
Practice DataFrame and Dataset operations using Apache Spark
through an employee payroll scenario.

## Concepts Covered
- DataFrame to Dataset conversion
- Dataset to DataFrame conversion
- RDD vs DataFrame vs Dataset
- Type safety
- Catalyst query optimization
- DataFrame filtering and aggregation
- Employee payroll analysis

## Scenario
Build a typed employee payroll pipeline using employee data
stored in a CSV file.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.6
- SBT 2.0.7
- Java 17

## Project Structure

day-14-dataframe-dataset/
├── .gitignore
├── README.md
├── build.sbt
├── data/
│   └── employees.csv
├── project/
│   └── build.properties
└── src/main/scala/
    └── DataFrameDatasetPractice.scala

## How to Run

Set Java 17:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
