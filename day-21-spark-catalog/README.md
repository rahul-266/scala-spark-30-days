# Day 21 — Spark Catalog

## Overview
A small hotel-booking analytics database using
Scala and Apache Spark SQL Catalog.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.6
- SBT 2.0.7

## Features
- List databases and tables
- Create a Spark SQL database
- Register persistent Parquet tables
- Create temporary views
- Query tables and views using SQL
- Inspect schema and catalog metadata
- Hotel-wise booking and revenue analytics

## Scenario
Build an analytics database for hotel bookings.

## Project Structure
- data/hotels.csv
- data/hotel_bookings.csv
- src/main/scala/SparkCatalogPractice.scala
- build.sbt
- README.md

## Compile
sbt -batch compile

## Run
sbt -batch "runMain SparkCatalogPractice"

## Database
hotel_analytics

## Tables
- hotels
- bookings

## Temporary Views
- hotels_temp
- bookings_temp

## Catalog Concepts
The Spark Catalog provides methods to list databases,
list tables, inspect columns and check table existence.

Temporary views are available in the Spark session.
Persistent tables are registered in the catalog
and store their data in Parquet format.
