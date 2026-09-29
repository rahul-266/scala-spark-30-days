import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Employee(
    employeeId: String,
    name: String,
    department: String,
    salary: Double
)

object DataFrameDatasetPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day14-DataFrame-Dataset")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // 1. Read employee data from CSV
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n=== Original DataFrame ===")
    rawDF.show(false)

    println("\n=== DataFrame Schema ===")
    rawDF.printSchema()

    // 2. Select and standardize columns
    val employeeDF = rawDF.select(
      col("employee_id").cast("string").as("employeeId"),
      col("name").cast("string").as("name"),
      col("department").cast("string").as("department"),
      col("salary").cast("double").as("salary")
    )

    // 3. Convert DataFrame to Dataset
    val employeeDS = employeeDF.as[Employee]

    println("\n=== DataFrame to Dataset ===")
    employeeDS.toDF().show(false)

    // 4. Convert Dataset back to DataFrame
    val convertedDF = employeeDS.toDF()

    println("\n=== Dataset to DataFrame ===")
    convertedDF.show(false)

    // 5. DataFrame filtering
    val highSalaryDF = employeeDF
      .filter(col("salary") >= 60000)
      .select("employeeId", "name", "salary")

    println("\n=== High Salary Employees: DataFrame ===")
    highSalaryDF.show(false)

    // 6. Typed Dataset filtering
    val highSalaryDS = employeeDS
      .filter(employee => employee.salary >= 60000)

    println("\n=== High Salary Employees: Dataset ===")
    highSalaryDS.toDF().show(false)

    // 7. RDD processing
    val departmentPayrollRDD = employeeDS.rdd
      .map(employee =>
        (employee.department, employee.salary)
      )
      .reduceByKey(_ + _)

    println("\n=== Department Payroll: RDD ===")
    departmentPayrollRDD
      .sortByKey()
      .collect()
      .foreach(println)

    // 8. DataFrame aggregation
    val payrollReport = employeeDF
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        round(avg("salary"), 2).alias("average_salary"),
        sum("salary").alias("total_salary"),
        max("salary").alias("maximum_salary"),
        min("salary").alias("minimum_salary")
      )
      .orderBy("department")

    println("\n=== Employee Payroll Report ===")
    payrollReport.show(false)

    // 9. Catalyst optimization
    println("\n=== Catalyst Query Plan ===")

    employeeDF
      .filter(col("salary") >= 60000)
      .select("employeeId", "name", "salary")
      .explain(true)

    // 10. Total employees
    println("\n=== Summary ===")
    println(s"Total employees: ${employeeDS.count()}")

    println("\n=== Day 14 Completed Successfully ===")

    spark.stop()
  }
}
