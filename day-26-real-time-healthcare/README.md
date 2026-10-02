# Day 26 — Real-Time Healthcare Project

## Overview
A real-time patient vital monitoring demonstration
using Kafka, Spark DStreams and Scala.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.6
- Apache Kafka
- SBT 2.0.7

## Features
- Patient vital JSON event schema
- Kafka producer and streaming consumer
- Broadcast vital thresholds
- Abnormal vital alerts
- LongAccumulator metrics
- Five-minute rolling abnormal reading counts
- Repeated abnormal reading alerts

## Sample Event Schema
- event_id
- patient_id
- heart_rate
- temperature
- oxygen
- timestamp

## Demo Thresholds
- Heart rate greater than 120
- Temperature greater than 39.0 C
- Oxygen less than 90 percent

These are educational sample values only,
not clinical recommendations.

## Kafka Topic
healthcare-vitals

## Compile
sbt -batch compile

## Run Producer
sbt -batch "runMain HealthcareProducer"

## Run Streaming
sbt -batch "runMain HealthcareStreaming"

## Processing
Spark uses a 10-second batch interval.
Broadcast thresholds classify each reading.
Accumulators track total and abnormal events.
A five-minute rolling window with a 30-second
slide detects repeated abnormal readings.
The repeated alert threshold is three readings
per patient within the window.

## Limitations
This is a learning demonstration using synthetic data.
It is not validated for clinical decisions.
