
---

# Day 02 — `Day02-Scala-Collections/README.md`

```markdown
# Day 02 - Scala Collections Practice

Day 2 of my 30-Day Scala + Apache Spark practice journey.

Today I focused on using Scala collections to process a small sales dataset
without using Apache Spark. The idea was to understand how common collection
operations work before applying similar concepts to distributed data processing.

## What I Practiced

### List

I used a `List` of sales records and practiced:

- `map`
- `filter`
- `flatMap`
- `reduce`

For example, `map` was used to calculate the total amount for a sale,
`filter` was used to find high-quantity sales, `flatMap` was used to expand
products based on their quantity, and `reduce` was used to calculate total sales.

### Vector

I used a `Vector` to store customer records and accessed a customer using
its index.

This helped me understand why `Vector` is useful when indexed access is needed
while keeping the collection immutable.

### Map

I used `Map` to store:

- Product quantities
- Product prices

The two Maps were then used to calculate product-wise revenue.

### For-Comprehension

I combined customer records and orders using a `for`-comprehension.

The customer ID was matched with the customer ID in the order so that I could
find which customer purchased which product.

## Daily Sales Summary

Finally, I calculated a simple daily sales summary without Spark.

The summary includes:

- Total products sold
- Total revenue
- Average sale value

For this exercise, the total revenue calculated by the program is:

**215000**

## Project Structure

```text
Day02-Scala-Collections/
├── .gitignore
├── README.md
├── build.sbt
├── project/
│   └── build.properties
├── data/
│   ├── customers.txt
│   └── orders.txt
├── output/
│   └── daily_sales_summary.txt
└── src/
    └── main/
        └── scala/
            └── Day02.scala
