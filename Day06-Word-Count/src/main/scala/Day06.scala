import org.apache.spark.{SparkConf, SparkContext}

object Day06 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day06-Word-Count")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("=== Day 06 - Word Count ===")
    println()

    val logs = sc.textFile("data/logs.txt")

    // Step 1: flatMap -> convert each log line into words.
    // Lowercase makes the count case-insensitive.
    // Punctuation is replaced with spaces and empty words are removed.
    val words = logs.flatMap { line =>
      line
        .toLowerCase
        .replaceAll("[^a-z0-9\\s]", " ")
        .split("\\s+")
        .filter(_.nonEmpty)
    }

    // Step 2: map -> convert each word into (word, 1).
    val wordOnes = words.map(word => (word, 1))

    // Step 3: reduceByKey -> add the counts for the same word.
    val wordCounts = wordOnes.reduceByKey(_ + _)

    val totalWords = words.count()
    val distinctWords = wordCounts.count()

    println("--- Classic Word Count ---")
    println(s"Total words: $totalWords")
    println(s"Distinct words: $distinctWords")

    println("\\n--- Top 10 Most Frequent Words ---")

    val top10 = wordCounts
      .sortBy { case (word, count) => (-count, word) }
      .take(10)

    top10.foreach {
      case (word, count) =>
        println(f"$word%-12s $count")
    }

    println("\\n--- Pipeline ---")
    println("flatMap -> map -> reduceByKey")
    println("Case-insensitive counting enabled")
    println("Punctuation and empty words ignored")

    println("\\n=== Day 06 Completed ===")

    sc.stop()
  }
}
