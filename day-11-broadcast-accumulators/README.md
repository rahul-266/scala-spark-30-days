# Day 11 — Broadcast and Accumulators

## Objective

Practice Spark Broadcast Variables and Accumulators by validating transactions against a small product master table.

## Concepts Covered

- Broadcast Variables
- Accumulators
- RDD processing
- Transaction validation
- Distributed processing
- Why normal driver variables should not be used for distributed updates

## Scenario

A transaction dataset is validated against a small product master map.

The product master is broadcast to Spark executors.

Invalid transactions are counted using an accumulator.

## Product Master

| Product ID | Product |
|------------|---------|
| P100 | Laptop |
| P101 | Mouse |
| P102 | Keyboard |
| P103 | Monitor |

## Validation Rules

A transaction is considered invalid when:

- The product ID does not exist in the master table
- The transaction amount is less than or equal to zero

## Result

- Valid transactions: 4
- Bad transactions: 3

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.6
- SBT 2.0.7

## Run

```bash
sbt -batch compile
sbt -batch "run"o

