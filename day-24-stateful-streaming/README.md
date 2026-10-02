# Day 24 — Stateless vs Stateful Streaming

## Overview
A bank account transaction-counting application
using Spark Streaming DStreams.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.6
- SBT 2.0.7
- TCP Socket

## Features
- Stateless transformations
- Stateful updateStateByKey
- Account-wise current batch counts
- Accumulated running transaction counts
- Comparison of batch and running state
- Checkpointing

## Scenario
Maintain running transaction counts per bank account.

## Project Structure
- data/sample_transactions.txt
- src/main/scala/AccountStatefulStreaming.scala
- build.sbt
- README.md

## Compile
sbt -batch compile

## Run
sbt -batch "runMain AccountStatefulStreaming"

## Socket
Start:
nc -lk 9999

Send records in this format:
account_id,transaction_id,amount

Example:
ACC101,TX001,500

## Stateless Processing
The current batch count is calculated using
map and reduceByKey. Each batch is independent.

## Stateful Processing
updateStateByKey maintains the accumulated
transaction count for every account.

## Checkpointing
Checkpointing is configured for stateful
processing and recovery.

## Limitation
The socket source is intended for local practice.
It does not provide production-grade durable ingestion.
