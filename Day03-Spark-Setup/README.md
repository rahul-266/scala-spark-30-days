# Day 03 - Spark Setup and First Application

This is Day 3 of my 30-Day Scala + Apache Spark practice journey.

The main goal of Day 3 was to create my first Spark application using Scala
and sbt. I created a SparkSession and SparkContext, read a text file using
Spark, and tested the same application with 2 and 4 local cores.

## What I Practiced

### SparkSession

`SparkSession` is the main entry point for working with Spark applications.
I used it to create and configure my Spark application.

### SparkContext

`SparkContext` provides the connection to the Spark execution environment.
I used it to create an RDD from the input text file.

### Reading a Text File

The application reads `data/sample.txt` using:

```scala
sc.textFile("data/sample.txt")

## Spark Components

### Driver

The Driver is the main process of the Spark application. It starts the
application and coordinates the execution of jobs and tasks.

### Executor

The Executor performs the tasks assigned by the Driver and processes the
data.

In local mode, the Driver and Executor run on the same machine.

### Cluster Manager

A Cluster Manager manages resources for Spark applications in a cluster.

Common cluster managers include:

- Standalone
- YARN
- Kubernetes

For this practice, I used local mode, so no separate cluster manager was
required.
