# Day 8 — DAG and Spark Execution

Day 8 focuses on how Spark turns RDD transformations into jobs, stages, and tasks. The practice uses a small sales dataset to build a transformation pipeline and then explains where the shuffle happens.

## Objective

The main goals of Day 8 are:

- Create a job with several transformations and actions.
- Identify stages and the shuffle boundary.
- Understand jobs, stages, tasks, and partitions.
- Compare narrow and wide transformations.
- Predict the number of stages for a `reduceByKey` pipeline.

## Transformation Pipeline

The main pipeline is:

```text
Input RDD
   ↓ map
Split CSV fields
   ↓ filter
Keep valid records
   ↓ map
Create (product, revenue)
   ↓ mapValues
Prepare key-value data
   ↓ reduceByKey
======= SHUFFLE =======
   ↓
Aggregated product revenue
   ↓ collect
Driver receives result
```

## Jobs, Stages, Tasks and Partitions

### Job
A Spark job is triggered when an action such as `collect()` or `count()` is called.

### Stage
A job is divided into stages around shuffle boundaries. Transformations that can run without a shuffle stay in the same stage.

### Task
A task is the unit of work executed for one partition in a stage.

### Partition
An RDD is divided into partitions so Spark can process the data in parallel.

## Narrow and Wide Transformations

### Narrow transformation
A child partition depends on only one parent partition. Data does not need to be shuffled between partitions.

Examples used here:

- `map`
- `filter`
- `mapValues`

### Wide transformation
A child partition may depend on multiple parent partitions, so Spark needs to move data between partitions.

Example used here:

- `reduceByKey`

## Shuffle Boundary

`reduceByKey` is the shuffle boundary in the main pipeline.

Before the shuffle, Spark can process each input partition independently. During `reduceByKey`, records with the same key have to be brought together so their values can be combined.

## Predicting the Number of Stages

For this pipeline:

```text
map → filter → map → mapValues → reduceByKey → collect
```

There is one major shuffle boundary at `reduceByKey`, so the pipeline is expected to use **2 stages**:

```text
Stage 1
map → filter → map → mapValues
          ↓
       SHUFFLE
          ↓
Stage 2
reduceByKey → collect
```

The exact task count depends on the number of partitions.

## Project Structure

```text
Day08-DAG-Spark-Execution/
├── .gitignore
├── README.md
├── build.sbt
├── project/
│   └── build.properties
├── data/
│   └── sales.txt
├── output/
│   └── dag_output.txt
└── src/main/
    ├── resources/
    │   └── log4j2.properties
    └── scala/
        └── Day08.scala
```

## How to Run

From the project directory:

```bash
cd ~/scala-spark-30-days/Day08-DAG-Spark-Execution
```

Run the application:

```bash
sbt run
```

Save the output:

```bash
sbt run > output/dag_output.txt 2>&1
```

View the saved output:

```bash
cat output/dag_output.txt
```

## What I Practiced

This exercise helped me connect Spark transformations with the execution model. I practiced identifying which transformations are narrow, where a wide transformation causes a shuffle, how jobs are divided into stages, and how partitions lead to tasks.

## Status

Day 8 completed.
