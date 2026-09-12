# Day 6 — Word Count

Day 6 focuses on the classic Spark Word Count problem and the RDD pipeline used to solve it.

The main flow practiced here is:

```text
flatMap -> map -> reduceByKey
```

The program also makes the word count case-insensitive, ignores punctuation and empty words, and finds the top 10 most frequent words in application logs.

## Objective

The goal of Day 6 is to understand how Spark converts text into words, maps each word to a count, and combines counts for the same key.

The exercise covers:

- Classic Word Count
- `flatMap`
- `map`
- `reduceByKey`
- Case-insensitive word counting
- Ignoring punctuation
- Ignoring empty words
- Finding the top 10 most frequent words in application logs

## What I Practiced

### 1. `flatMap`

The log file is read as an RDD of lines. Each line is converted into individual words.

The program first converts text to lowercase, replaces punctuation with spaces, splits the line into words, and removes empty values.

### 2. `map`

Each word is converted into a key-value pair:

```scala
(word, 1)
```

For example:

```text
spark -> (spark, 1)
job   -> (job, 1)
```

### 3. `reduceByKey`

`reduceByKey` combines values that have the same key.

For example:

```text
(spark, 1)
(spark, 1)
(spark, 1)
```

becomes:

```text
(spark, 3)
```

This gives the final frequency of each word.

## Case-Insensitive Counting

The program uses:

```scala
.toLowerCase
```

So words such as `Spark`, `spark`, and `SPARK` are counted as the same word.

## Ignoring Punctuation and Empty Words

Punctuation is replaced with spaces before splitting the text:

```scala
.replaceAll("[^a-z0-9\\s]", " ")
```

Empty values are removed using:

```scala
.filter(_.nonEmpty)
```

## Top 10 Words

After calculating the word frequencies, the program sorts the results by frequency in descending order and returns the top 10 words.

## Sample Input

The application log file is stored in:

```text
data/logs.txt
```

It contains INFO, WARN, and ERROR messages related to Spark jobs, orders, and database processing.

## Project Structure

```text
Day06-Word-Count/
├── .gitignore
├── README.md
├── build.sbt
├── project/
│   └── build.properties
├── data/
│   └── logs.txt
├── output/
│   └── word_count_output.txt
└── src/
    └── main/
        ├── resources/
        │   └── log4j2.properties
        └── scala/
            └── Day06.scala
```

## Technologies Used

- Scala 2.12.18
- Apache Spark 3.5.6
- sbt 1.10.11
- Java 17

## How to Run

From the repository root:

```bash
cd ~/scala-spark-30-days/Day06-Word-Count
```

Run the program:

```bash
sbt run
```

Save the output:

```bash
sbt run > output/word_count_output.txt 2>&1
```

View the saved output:

```bash
cat output/word_count_output.txt
```

## Output / Result

The program produces a classic Word Count result and then prints the top 10 most frequent words from the application logs.

A successful run also shows the complete processing pipeline:

```text
flatMap -> map -> reduceByKey
```

The exact word order is made deterministic by sorting first by count in descending order and then alphabetically for ties.

## What I Learned

Day 6 helped me understand the standard Spark Word Count pattern:

```text
Input text
   ↓
flatMap
   ↓
(word, 1)
   ↓
map
   ↓
reduceByKey
   ↓
word frequencies
```

I also learned how to normalize text before counting and how `reduceByKey` combines values for the same key.

## Status

Day 6 completed — Word Count and log frequency analysis implemented.
