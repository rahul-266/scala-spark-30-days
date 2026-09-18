import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitioningPractice {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day10-Partitioning")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // --------------------------------------------------
    // 1. Inspect partition count
    // --------------------------------------------------

    val data = sc.parallelize(1 to 100, 2)

    println("\n=== Initial Partition Count ===")
    println(data.getNumPartitions)

    // --------------------------------------------------
    // 2. repartition
    // --------------------------------------------------

    val repartitioned = data.repartition(4)

    println("\n=== After repartition(4) ===")
    println(repartitioned.getNumPartitions)

    // --------------------------------------------------
    // 3. coalesce
    // --------------------------------------------------

    val coalesced = repartitioned.coalesce(2)

    println("\n=== After coalesce(2) ===")
    println(coalesced.getNumPartitions)

    // --------------------------------------------------
    // 4. Pair RDD + partitionBy
    // --------------------------------------------------

    val pairRDD = sc.parallelize(
      Seq(
        ("A", 100),
        ("B", 200),
        ("C", 300),
        ("A", 150),
        ("B", 250),
        ("C", 350)
      ),
      4
    )

    println("\n=== Pair RDD Before partitionBy ===")
    println(pairRDD.getNumPartitions)

    val partitionedRDD =
      pairRDD.partitionBy(new HashPartitioner(3))

    println("\n=== Pair RDD After partitionBy(3) ===")
    println(partitionedRDD.getNumPartitions)

    // --------------------------------------------------
    // 5. Inspect data in each partition
    // --------------------------------------------------

    val partitionInfo = partitionedRDD.mapPartitionsWithIndex {
      (index, iterator) =>
        Iterator(
          s"Partition $index -> ${iterator.toList}"
        )
    }

    println("\n=== Data Distribution ===")
    partitionInfo.collect().foreach(println)

    // --------------------------------------------------
    // 6. Scenario: too few partitions
    // --------------------------------------------------

    val lowPartitionData =
      sc.parallelize(1 to 100000, 1)

    println("\n=== Too Few Partitions Scenario ===")
    println(
      s"Before optimization: ${lowPartitionData.getNumPartitions}"
    )

    val optimizedData =
      lowPartitionData.repartition(4)

    println(
      s"After optimization: ${optimizedData.getNumPartitions}"
    )

    // --------------------------------------------------
    // 7. Scenario: reduce partitions
    // --------------------------------------------------

    val manyPartitions =
      sc.parallelize(1 to 100, 8)

    println("\n=== Too Many Partitions Scenario ===")
    println(
      s"Before coalesce: ${manyPartitions.getNumPartitions}"
    )

    val reducedPartitions =
      manyPartitions.coalesce(4)

    println(
      s"After coalesce: ${reducedPartitions.getNumPartitions}"
    )

    sc.stop()
  }
}
