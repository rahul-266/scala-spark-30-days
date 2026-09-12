# Day 5 — Transformations and Actions

Day 5 focuses on the difference between Spark **transformations** and **actions** and introduces Spark's **lazy evaluation** model. I also built a small log analyzer that filters `ERROR` messages and counts them.

## Objectives

- Practice `map`, `filter`, `flatMap`, `distinct`, and `union`.
- Practice `count`, `collect`, `first`, `take`, and `reduce`.
- Understand transformations versus actions.
- Understand which operations are lazy.
- Build a simple log analyzer that counts `ERROR` messages.

## What I Practiced

### Transformations

Transformations create a new RDD from an existing RDD.

- `map` transforms every element.
- `filter` keeps elements that match a condition.
- `flatMap` can create multiple output elements from one input element.
- `distinct` removes duplicate values.
- `union` combines two RDDs.

### Actions

Actions trigger Spark execution and return a result.

- `count` returns the number of elements.
- `collect` returns all elements to the driver.
- `first` returns the first element.
- `take` returns the first `n` elements.
- `reduce` combines elements into a single result.

### Lazy Evaluation

Spark transformations are lazy. Defining `map` or `filter` does not immediately execute the computation. Spark runs the required work when an action such as `collect`, `count`, or `reduce` is called.

## Log Analyzer

The log analyzer reads `data/logs.txt`, filters only lines containing `ERROR`, prints those lines, and counts the total number of error messages.

## Project Structure

```text
Day05-Transformations-Actions/
├── .gitignore
├── README.md
├── build.sbt
├── project/
│   └── build.properties
├── data/
│   └── logs.txt
├── output/
│   └── transformations_actions_output.txt
└── src/
    └── main/
        ├── resources/
        │   └── log4j2.properties
        └── scala/
            └── Day05.scala
```

## Sample Data

The input file contains normal application messages along with `ERROR` and `WARN` entries.

Example:

```text
2026-09-12 10:02:10 ERROR Database connection failed
2026-09-12 10:04:08 ERROR File not found
```

## How to Run

From the project folder:

```bash
cd ~/scala-spark-30-days/Day05-Transformations-Actions
sbt run
```

To save the complete console output:

```bash
sbt run > output/transformations_actions_output.txt 2>&1
```

To view the saved output:

```bash
cat output/transformations_actions_output.txt
```

## Output / Result

Expected important results:

```text
map: 2, 4, 6, 6, 8, 10
filter: 4, 5
flatMap: Spark, RDD, Scala, Spark
distinct: 1, 2, 3, 4, 5
union: Spark, Scala, RDD, Spark

count: 6
first: 1
take(3): 1, 2, 3
reduce: 18

Lazy result: 20, 40
Total ERROR messages: 4
```

## What I Learned

The main concept from Day 5 is that **transformations are lazy**, while **actions trigger execution**. I also practiced common RDD transformations and actions and used them to build a simple log analyzer.

## Status

Day 5 completed: Transformations, actions, lazy evaluation, and ERROR log analysis.
