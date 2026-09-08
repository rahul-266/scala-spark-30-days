object Day2 {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // 1. Sales List using map, filter, flatMap and reduce
    // ============================================================

    val sales = List(
      ("Laptop", 2, 50000),
      ("Mobile", 5, 20000),
      ("Headphones", 3, 3000),
      ("Keyboard", 4, 1500)
    )

    val salesWithAmount = sales.map {
      case (product, quantity, price) =>
        (product, quantity, price, quantity * price)
    }

    println("Sales with Total Amount:")
    salesWithAmount.foreach(println)

    val filteredSales = sales.filter {
      case (_, quantity, _) => quantity > 2
    }

    println("\nSales with Quantity > 2:")
    filteredSales.foreach(println)

    val individualItems = sales.flatMap {
      case (product, quantity, _) =>
        List.fill(quantity)(product)
    }

    println("\nIndividual Items using flatMap:")
    println(individualItems)

    val totalSales = sales.map {
      case (_, quantity, price) => quantity * price
    }.reduce(_ + _)

    println("\nTotal Sales Amount: " + totalSales)


    // ============================================================
    // 2. Vector for indexed customer records
    // ============================================================

    val customers = Vector(
      (101, "Jaya"),
      (102, "Rahul"),
      (103, "Anita"),
      (104, "Kiran")
    )

    println("\nCustomer Vector:")
    println(customers)

    println("\nCustomer at index 2:")
    println(customers(2))


    // ============================================================
    // 3. Map to calculate product quantities and prices
    // ============================================================

    val productQuantities = Map(
      "Laptop" -> 2,
      "Mobile" -> 5,
      "Headphones" -> 3,
      "Keyboard" -> 4
    )

    val productPrices = Map(
      "Laptop" -> 50000,
      "Mobile" -> 20000,
      "Headphones" -> 3000,
      "Keyboard" -> 1500
    )

    println("\nProduct Quantities:")
    productQuantities.foreach(println)

    println("\nProduct Prices:")
    productPrices.foreach(println)

    val productRevenue = productQuantities.map {
      case (product, quantity) =>
        val price = productPrices.getOrElse(product, 0)
        product -> (quantity * price)
    }

    println("\nProduct Revenue:")
    productRevenue.foreach(println)


    // ============================================================
    // 4. For-comprehension combining customers and orders
    // ============================================================

    val orders = List(
      (101, "Laptop"),
      (102, "Mobile"),
      (101, "Headphones"),
      (103, "Keyboard"),
      (104, "Mobile")
    )

    val customerOrders = for {
      (customerId, customerName) <- customers
      (orderCustomerId, product) <- orders
      if customerId == orderCustomerId
    } yield (customerName, product)

    println("\nCustomer Orders:")
    customerOrders.foreach(println)


    // ============================================================
    // 5. Daily Sales Summary without Spark
    // ============================================================

    val dailySales = List(
      ("2026-09-08", "Laptop", 2, 50000),
      ("2026-09-08", "Mobile", 5, 20000),
      ("2026-09-08", "Headphones", 3, 3000),
      ("2026-09-08", "Keyboard", 4, 1500)
    )

    val totalQuantity = dailySales.map {
      case (_, _, quantity, _) => quantity
    }.sum

    val totalRevenue = dailySales.map {
      case (_, _, quantity, price) => quantity * price
    }.sum

    val averageSale = totalRevenue.toDouble / dailySales.size

    println("\n==============================")
    println("DAILY SALES SUMMARY")
    println("==============================")
    println("Date: 2026-09-08")
    println("Total Products Sold: " + totalQuantity)
    println("Total Revenue: " + totalRevenue)
    println(f"Average Sale Value: $averageSale%.2f")
    println("==============================")
  }
}