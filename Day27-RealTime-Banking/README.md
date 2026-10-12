# Day 27 — Real-Time Banking Transaction Monitoring

## Objective
Process transaction JSON events with Spark Structured Streaming, aggregate by account and event-time windows, enrich with small branch/risk lookups, and write suspicious-burst alerts.

## Architecture
JSON transaction files -> schema validation -> broadcast branch/risk joins -> 1-minute event-time window aggregation -> suspicious-burst detection -> CSV metrics and alerts.

## Technology
- Scala 2.12.18
- Apache Spark SQL / Structured Streaming 3.5.6
- JSON, CSV, event-time windows, watermarking
- Broadcast joins, repartitioning, persist/unpersist

## Detection rule
An account is flagged when it has at least 3 transactions in a 1-minute event-time window, or when a HIGH-risk account's total amount meets or exceeds its configured high_value_limit. This is a demonstration rule, not a production fraud model.

## Run locally
From this project directory:
SPARK_MASTER='local[*]' sbt 'runMain com.banking.BankingStreamingApp'

The stream processes files from data/stream_input. Keep the app running and add a new uniquely named JSON file to that directory to simulate new events.

## Outputs
- output/account_metrics/
- output/suspicious_alerts/
- checkpoints/banking/

Spark writes directories containing CSV part-files rather than a single flat CSV file.

## Partitioning, shuffles and stages
- File input and validation read JSON data.
- Broadcast joins distribute the small lookup tables to executors; this avoids shuffling the large streaming side for those joins.
- Window aggregation groups by event-time window and account and requires distributed state/aggregation work. The shuffle/state exchange is an expected cost.
- repartition(4, account_id) explicitly redistributes aggregated data by account for downstream parallel work. It introduces an exchange/shuffle, so production use should be justified by downstream workload and partition sizes.
- foreachBatch processes each output micro-batch. The batch is persisted while metrics and alerts are written, then unpersisted to release executor memory.
- Watermarking allows late events within the configured delay while limiting retained streaming state.

## YARN deployment notes
The YARN cluster must have a compatible Spark 3.5.x distribution using Scala 2.12. Copy the input and reference directories to a shared HDFS location and ensure all executors can access them. Build the application with sbt package, then submit the jar through spark-submit with --master yarn. Set SPARK_MASTER=yarn so the application's SparkSession uses the same master. Pass the HDFS project root as the application argument. Configure executor memory, cores, number of executors and checkpoint storage based on the cluster. Do not use a local workstation path for distributed YARN input or checkpoint storage.
