cat > README.md <<'EOF'
# Day 01 - Scala Essentials

This is the first day of my 30-Day Scala + Apache Spark practice journey.

The main goal of Day 1 was to build a basic understanding of Scala before
moving into Apache Spark. I practiced variables, immutable collections,
for-comprehensions, traits, and a small Student Grade Processor.

## What I Practiced

### Variables

I practiced the difference between:

- `val` - immutable variable
- `var` - mutable variable
- `lazy val` - initialized only when it is accessed

### Scala Collections

I worked with basic immutable collections:

- `List`
- `Vector`
- `Set`
- `Map`

### For-Comprehension

I used `for` with `yield` to generate results from collections.

### Traits

I created a `Logger` trait and used it in:

- `StudentProcessor`
- `GradeProcessor`

## Mini Project - Student Grade Processor

The final part of Day 1 combines the concepts into a simple Student Grade
Processor.

It calculates:

- Student grade
- PASS / FAIL status
- Student result

### Grade Rules

| Marks | Grade |
|------:|:-----:|
| 90 and above | A |
| 75 - 89 | B |
| 60 - 74 | C |
| 40 - 59 | D |
| Below 40 | F |

## Project Structure

```text
Day01-Scala-Essentials/
├── .gitignore
├── README.md
├── build.sbt
├── project/
│   └── build.properties
├── data/
│   └── students.txt
├── output/
│   └── student_results.txt
└── src/
    └── main/
        └── scala/
            └── Day01.scala
