# Day 04 - RDD Creation

This is Day 4 of my 30-Day Scala + Apache Spark practice journey.

The main focus of Day 4 was to understand how Spark RDDs are created and
processed. I worked with RDDs created from Scala collections as well as
RDDs created by reading text files.

I also practiced basic RDD transformations such as `map`, `filter`, and
`flatMap`, calculated total sales from transaction records, and inspected
how Spark distributes data across partitions.

The final part of the exercise demonstrates how a customer file can be
split into multiple partitions for distributed processing.

---

## Objectives

The main objectives of Day 4 were:

1. Create an RDD from a Scala collection.
2. Create an RDD from a text file.
3. Practice `map`, `filter`, and `flatMap` on RDDs.
4. Calculate total sales from transaction records.
5. Inspect RDD partitions.
6. Understand Spark's default parallelism.
7. Process a customer file using multiple partitions.
8. Understand the relationship between data, partitions, and parallel
   processing.

---

## What I Practiced

### 1. Creating an RDD from a Scala Collection

Spark can create an RDD from an in-memory Scala collection using
`parallelize()`.

Example:

```scala
val numbers = Seq(10, 20, 30, 40, 50)
val numberRDD = sc.parallelize(numbers)
