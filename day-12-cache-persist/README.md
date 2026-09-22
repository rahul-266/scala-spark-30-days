# Day 12 — Cache and Persist

## Objective

This project demonstrates Spark `cache()` and `persist()` by reusing a cleaned transaction dataset across multiple reports.

## Scenario

A transaction dataset is cleaned once and reused to generate three different reports:

1. Revenue by transaction type
2. Successful transactions by type
3. Top accounts by transaction count

Caching the cleaned dataset avoids recomputing the same transformation for each report.

## Concepts Covered

- RDD `cache()`
- RDD `persist()`
- Storage levels
- RDD reuse across multiple actions
- `MEMORY_ONLY`
- `MEMORY_AND_DISK`
- `DISK_ONLY`
- When caching can hurt performance

## Processing Flow

```text
Raw Transactions
       |
       v
Clean Transaction Dataset
       |
       |--- cache()
       |
       +----> Revenue by Transaction Type
       |
       +----> Successful Transactions by Type
       |
       +----> Top Accounts
