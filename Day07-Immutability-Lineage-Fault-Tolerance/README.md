# Day 7 — Immutability, Lineage and Fault Tolerance

Day 7 focuses on how Spark RDDs remain reliable when data is transformed and when a partition is lost. The practice builds a multi-step transformation chain, inspects its lineage, and explains how Spark can recompute lost partitions.

## Objective

The main goals are:

- Create a multi-step RDD transformation chain.
- Inspect and draw the lineage of the RDDs.
- Understand why RDDs are immutable.
- Understand how Spark recomputes lost partitions using lineage.
- Explain executor-loss recovery conceptually.

## What I Practiced

### 1. Multi-step RDD transformation chain

The program reads a text file and applies three transformations:

```text
inputRDD
   ↓ flatMap
wordsRDD
   ↓ filter
sparkWordsRDD
   ↓ map
resultRDD
```

The steps are:

- `flatMap` splits lines into individual words.
- `filter` keeps only the word `spark`, ignoring case.
- `map` converts the matching words to uppercase.

These operations are transformations, so they are lazy.

## 2. RDD Immutability

RDDs are immutable. A transformation does not change the existing RDD. Instead, Spark creates a new RDD that refers to the previous RDD.

For example:

```scala
val wordsRDD = inputRDD.flatMap(_.split(" "))
val sparkWordsRDD = wordsRDD.filter(_.equalsIgnoreCase("spark"))
val resultRDD = sparkWordsRDD.map(_.toUpperCase)
```

`inputRDD` remains unchanged while the new RDDs are created.

This makes the processing model easier to reason about and allows Spark to keep the dependency information needed for recovery.

## 3. RDD Lineage

Lineage is the chain of transformations that Spark remembers for an RDD.

For this program:

```text
Input text file
      ↓
   inputRDD
      ↓ flatMap
   wordsRDD
      ↓ filter
 sparkWordsRDD
      ↓ map
   resultRDD
```

The program also prints Spark's lineage information using:

```scala
resultRDD.toDebugString
```

## 4. Fault Tolerance

RDDs use lineage for fault recovery.

Conceptually, if an executor is lost and one of the partitions being processed by that executor is lost, Spark can recompute the missing partition from the RDD's parent data and transformation history.

For this exercise, the chain is:

```text
inputRDD partition
      ↓
flatMap
      ↓
filter
      ↓
map
      ↓
recreated result partition
```

Spark does not need to recompute unaffected partitions.

The actual executor-loss scenario is explained conceptually here rather than intentionally crashing an executor.

## Project Structure

```text
Day07-Immutability-Lineage-Fault-Tolerance/
├── .gitignore
├── README.md
├── build.sbt
├── project/
│   └── build.properties
├── data/
│   └── sample.txt
├── output/
│   └── lineage_output.txt
└── src/
    └── main/
        ├── resources/
        │   └── log4j2.properties
        └── scala/
            └── Day07.scala
```

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.6
- sbt 1.10.11
- Java 17

## How to Run

From the project directory:

```bash
cd ~/scala-spark-30-days/Day07-Immutability-Lineage-Fault-Tolerance
```

Run the program:

```bash
sbt run
```

Save the complete output:

```bash
sbt run > output/lineage_output.txt 2>&1
```

View the saved output:

```bash
cat output/lineage_output.txt
```

## What I Learned

- RDDs are immutable.
- Transformations create new RDDs instead of modifying existing ones.
- Lineage records how an RDD was produced.
- Spark uses lineage to recompute lost partitions.
- In a narrow transformation chain, Spark can recompute the lost partition by reapplying the required parent transformations.

## Status

Day 7 completed successfully when the application runs and prints the transformation chain, final result, lineage information, and fault-tolerance explanation.
