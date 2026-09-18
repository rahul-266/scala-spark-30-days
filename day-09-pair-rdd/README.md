# Day 9 — Pair RDD

## Objective

Practice Pair RDD operations in Apache Spark using Scala.

## Tasks Covered

- Create key-value Pair RDDs
- Use `reduceByKey`
- Use `groupByKey`
- Use `mapValues`
- Calculate revenue by product
- Calculate revenue by department
- Compare `reduceByKey` and `groupByKey` performance
- Aggregate bank transactions by account ID

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.6
- SBT
- Java 17

## Implementation

### Revenue by Product
`reduceByKey` is used to aggregate product revenue.

### Quantity by Product
`groupByKey` is used to group product quantities and calculate totals.

### mapValues
`mapValues` is used to transform values while keeping the original keys.

### Revenue by Department
Department revenue is aggregated using `reduceByKey`.

### Performance Comparison

For the local test dataset:

```text
reduceByKey: 588.16 ms
groupByKey:  512.27 ms
