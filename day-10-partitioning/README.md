# Day 10 — Partitioning

## Objective

Practice Apache Spark RDD partitioning and understand how partition counts affect data processing.

## Tasks Covered

- Inspect partition counts
- Use `repartition`
- Use `coalesce`
- Explain when increasing or decreasing partitions helps
- Use `partitionBy` on a Pair RDD
- Inspect data distribution across partitions
- Optimize a dataset suffering from too few partitions

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.6
- SBT
- Java 17

## Results

```text
Initial partitions: 2
After repartition(4): 4
After coalesce(2): 2

Pair RDD before partitionBy: 4
Pair RDD after partitionBy(3): 3

Too few partitions:
Before optimization: 1
After optimization: 4

Too many partitions:
Before coalesce: 8
After coalesce: 4
