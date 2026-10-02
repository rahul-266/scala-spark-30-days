
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctionsPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day17-Window-Functions")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // 1. Read student data
    val students = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/students.csv")

    // 2. Apply ranking functions per course
    val studentWindow = Window
      .partitionBy("course_id")
      .orderBy(col("score").desc)

    val rankedStudents = students
      .withColumn("row_number", row_number().over(studentWindow))
      .withColumn("rank", rank().over(studentWindow))
      .withColumn("dense_rank", dense_rank().over(studentWindow))

    println("\n=== Student Ranking ===")
    rankedStudents
      .orderBy(col("course_id"), col("score").desc, col("student_id"))
      .show(false)

    // 3. Find top 3 students per course
    println("\n=== Top 3 Students Per Course ===")
    rankedStudents
      .filter(col("row_number") <= 3)
      .orderBy(col("course_id"), col("row_number"))
      .show(false)

    // 4. Employee ranking by department
    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    val departmentWindow = Window
      .partitionBy("department")
      .orderBy(col("salary").desc)

    val employeeRanking = employees
      .withColumn("row_number", row_number().over(departmentWindow))
      .withColumn("rank", rank().over(departmentWindow))
      .withColumn("dense_rank", dense_rank().over(departmentWindow))

    println("\n=== Employee Ranking By Department ===")
    employeeRanking
      .orderBy(col("department"), col("salary").desc, col("employee_id"))
      .show(false)

    // 5. Read customer policy history
    val policies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customer_policies.csv")
      .withColumn(
        "updated_at_ts",
        to_timestamp(col("updated_at"), "yyyy-MM-dd")
      )

    val policyWindow = Window
      .partitionBy("customer_id")
      .orderBy(col("updated_at_ts"))

    // 6. Use lag and lead for policy premiums
    val policyHistory = policies
      .withColumn("previous_premium", lag("premium", 1).over(policyWindow))
      .withColumn("next_premium", lead("premium", 1).over(policyWindow))

    println("\n=== Customer Policy History With Lag And Lead ===")
    policyHistory
      .orderBy("customer_id", "updated_at_ts")
      .show(false)

    // 7. Find latest policy per customer
    val latestPolicyWindow = Window
      .partitionBy("customer_id")
      .orderBy(col("updated_at_ts").desc, col("policy_id").desc)

    val latestPolicies = policies
      .withColumn("row_number", row_number().over(latestPolicyWindow))
      .filter(col("row_number") === 1)
      .drop("row_number")

    println("\n=== Latest Policy Per Customer ===")
    latestPolicies
      .orderBy("customer_id")
      .show(false)

    // 8. Read route booking history
    val bookings = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/route_bookings.csv")
      .withColumn(
        "booking_ts",
        to_timestamp(col("booking_time"), "yyyy-MM-dd'T'HH:mm:ss")
      )

    val routeWindow = Window
      .partitionBy("route_id")
      .orderBy(col("booking_ts"))

    // 9. Use lag and lead for route fares
    val routeHistory = bookings
      .withColumn("previous_fare", lag("fare", 1).over(routeWindow))
      .withColumn("next_fare", lead("fare", 1).over(routeWindow))

    println("\n=== Route Booking History With Lag And Lead ===")
    routeHistory
      .orderBy("route_id", "booking_ts")
      .show(false)

    // 10. Route-wise fare ranking
    val routeRankWindow = Window
      .partitionBy("route_id")
      .orderBy(col("fare").desc)

    val routeRanking = bookings
      .withColumn("row_number", row_number().over(routeRankWindow))
      .withColumn("rank", rank().over(routeRankWindow))
      .withColumn("dense_rank", dense_rank().over(routeRankWindow))

    println("\n=== Route-wise Fare Ranking ===")
    routeRanking
      .orderBy(col("route_id"), col("fare").desc, col("booking_id"))
      .show(false)

    println("\n=== Day 17 Window Functions Completed ===")

    spark.stop()
  }
}
