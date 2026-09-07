// ============================================================
// Scala + Spark 30-Day Practice
// Day 01 - Scala Essentials
// ============================================================

// ------------------------------------------------------------
// 1. Logger Trait
// ------------------------------------------------------------

trait Logger {

  def log(message: String): Unit = {
    println(s"[LOG] $message")
  }

  def error(message: String): Unit = {
    println(s"[ERROR] $message")
  }
}


// ------------------------------------------------------------
// 2. Student Processor
// ------------------------------------------------------------

class StudentProcessor extends Logger {

  def processStudent(name: String, marks: Int): Unit = {

    log(s"Processing student: $name")

    if (marks >= 40) {
      log(s"$name has passed with $marks marks")
    } else {
      error(s"$name has failed with $marks marks")
    }
  }
}


// ------------------------------------------------------------
// 3. Grade Processor
// ------------------------------------------------------------

class GradeProcessor extends Logger {

  def calculateGrade(marks: Int): String = {

    log(s"Calculating grade for marks: $marks")

    if (marks >= 90) {
      "A"
    } else if (marks >= 75) {
      "B"
    } else if (marks >= 60) {
      "C"
    } else if (marks >= 40) {
      "D"
    } else {
      "F"
    }
  }
}


// ------------------------------------------------------------
// 4. Main Program
// ------------------------------------------------------------

object Day01 {

  def main(args: Array[String]): Unit = {

    // ========================================================
    // PART 1: val, var and lazy val
    // ========================================================

    println("========================================")
    println("PART 1: val, var and lazy val")
    println("========================================")

    // val - immutable
    val studentName = "Donesh"

    // var - mutable
    var studentAge = 21
    studentAge = 22

    println(s"Student Name: $studentName")
    println(s"Student Age: $studentAge")

    // lazy val - initialized only when accessed
    lazy val studentMessage = {
      println("lazy val is being initialized...")
      "Welcome to Scala!"
    }

    println("Before accessing lazy val")
    println(s"Message: $studentMessage")
    println("After accessing lazy val")


    // ========================================================
    // PART 2: Immutable Collections
    // ========================================================

    println()
    println("========================================")
    println("PART 2: Immutable Collections")
    println("========================================")

    // List - ordered collection
    val students = List(
      "Rahul",
      "Aman",
      "Priya",
      "Neha"
    )

    // Vector - indexed collection
    val marks = Vector(
      85,
      72,
      91,
      68
    )

    // Set - unique values
    val subjects = Set(
      "Scala",
      "Spark",
      "SQL",
      "Scala"
    )

    // Map - key-value pairs
    val studentMarks = Map(
      "Rahul" -> 85,
      "Aman" -> 72,
      "Priya" -> 91,
      "Neha" -> 68
    )

    println(s"Students: $students")
    println(s"Marks: $marks")
    println(s"Subjects: $subjects")
    println(s"Student Marks: $studentMarks")


    // ========================================================
    // PART 3: Collection Operations
    // ========================================================

    println()
    println("========================================")
    println("PART 3: Collection Operations")
    println("========================================")

    // List
    val passedStudents = studentMarks.filter {
      case (_, mark) => mark >= 40
    }

    println(s"Passed Students: $passedStudents")

    // Map
    val bonusMarks = studentMarks.map {
      case (name, mark) => (name, mark + 5)
    }

    println(s"Marks After Bonus: $bonusMarks")

    // Vector
    println(s"First Mark: ${marks.head}")

    // Set
    println(s"Number of Subjects: ${subjects.size}")


    // ========================================================
    // PART 4: For-Comprehension with yield
    // ========================================================

    println()
    println("========================================")
    println("PART 4: For-Comprehension with yield")
    println("========================================")

    val studentNames = List(
      "Rahul",
      "Aman",
      "Priya"
    )

    val studentMarksList = List(
      85,
      72,
      91
    )

    val studentResults = for {
      name <- studentNames
      mark <- studentMarksList
    } yield s"$name scored $mark"

    studentResults.foreach(println)


    // ========================================================
    // PART 5: Logger Trait
    // ========================================================

    println()
    println("========================================")
    println("PART 5: Logger Trait")
    println("========================================")

    val studentProcessor = new StudentProcessor

    studentProcessor.processStudent(
      "Rahul",
      85
    )

    studentProcessor.processStudent(
      "Aman",
      35
    )

    val gradeProcessor = new GradeProcessor

    val priyaGrade = gradeProcessor.calculateGrade(91)

    println(s"Priya's Grade: $priyaGrade")


    // ========================================================
    // PART 6: Student Grade Processor
    // ========================================================

    println()
    println("========================================")
    println("PART 6: Student Grade Processor")
    println("========================================")

    val gradeStudents = Map(
      "Rahul" -> 85,
      "Aman" -> 35,
      "Priya" -> 91,
      "Neha" -> 68
    )

    println("Student Results:")

    gradeStudents.foreach {
      case (name, mark) =>

        val grade = gradeProcessor.calculateGrade(mark)

        val status =
          if (mark >= 40) "PASS"
          else "FAIL"

        println(
          f"$name%-10s Marks: $mark%3d  Grade: $grade  Status: $status"
        )
    }


    // ========================================================
    // PART 7: Summary
    // ========================================================

    println()
    println("========================================")
    println("DAY 01 COMPLETED")
    println("========================================")

    println("Topics Covered:")
    println("1. val")
    println("2. var")
    println("3. lazy val")
    println("4. List")
    println("5. Vector")
    println("6. Set")
    println("7. Map")
    println("8. Collection operations")
    println("9. For-comprehension")
    println("10. yield")
    println("11. Trait")
    println("12. Student Grade Processor")
  }
}