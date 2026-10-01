# Day 16 — Bus/Train Ticket Streaming

## Overview
A real-time ticket-booking pipeline using
Kafka, Spark Structured Streaming, Scala, and Hive.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.6
- Apache Kafka 4.3.1
- Apache Hive
- SBT 2.0.7

## Features
- Kafka ticket event producer
- Spark Structured Streaming consumer
- Route-wise booking and revenue reports
- Window-based booking aggregation
- Peak booking period analysis
- Available seats and cancellation rate
- Historical booking storage in Hive

## Project Structure
- data/route_capacity.csv
- data/ticket_events.json
- src/main/scala/TicketProducer.scala
- src/main/scala/TicketStreaming.scala
- src/main/scala/HiveVerify.scala
- build.sbt

## Kafka Topic
ticket-bookings

Partitions: 3
Replication factor: 1

## Run
Compile:
```bash
sbt compile
