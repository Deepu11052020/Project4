import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, format_number, initcap, to_date, when}

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    // Define Spark configuration
    val sparkConf = new SparkConf()
      .setAppName("superMarket Project")
      .setMaster("local[1]")
    // Create SparkSession
    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    // Define schema for the supermarket sales data
    val superSchema = "InvoiceId String, BranchId Int, customer_type String, gender String, productId Int, unit_price Float, quantity Int, tax_5_percent Float, total Float, date1 String, time1 String, payment String, cogs Float, gross_margin_per Float, gross_income Float, rating Float"
    val superdf = spark.read
      .option("header", true)
      .schema(superSchema)
      .csv(args(0))
    // .csv("C:\\Users\\deepu\\Documents\\Project_SuperMarket\\supermarket_sales.csv")
    val sortedSuperdf = superdf.orderBy("InvoiceId")
    val superdf_D = sortedSuperdf.dropDuplicates()
    val superdfFilled = superdf_D.na.fill("Unknown")
    val convertedDF = superdfFilled.withColumn("Gender", when(col("Gender") === "F", "Female").otherwise("Male"))
    val decimalDF = convertedDF.withColumn("Tax_5_percent", format_number(col("Tax_5_percent").cast("Decimal(10,2)"), 2))
      .withColumn("Total", format_number(col("Total").cast("Decimal(10,2)"), 2))
      .withColumn("unit_price", format_number(col("unit_price").cast("Decimal(10,2)"), 2))
      .withColumn("cogs", format_number(col("cogs").cast("Decimal(10,2)"), 2))
      .withColumn("gross_margin_per", format_number(col("gross_margin_per").cast("Decimal(10,2)"), 2))
      .withColumn("gross_income", format_number(col("gross_income").cast("Decimal(10,2)"), 2))
      .withColumn("rating", format_number(col("rating").cast("Decimal(10,2)"), 2))
    val superMarketdf_cleaned = decimalDF.withColumn("Created_date",current_timestamp())
    //define schema for Branch
    val branchSchema = "BranchId Int, Branch_Name String, City_Name String, Start_Date String, End_Date String"
    val branchdf= spark.read
      .option("header", true)
      .schema(branchSchema)
      .csv(args(2))
    // .csv("C:\\Users\\deepu\\Documents\\Project_SuperMarket\\BranchCity.csv")
    val branchdf_cleaned1 = branchdf.withColumn("City_Name", initcap(col("City_Name")))
    val branchdf_cleaned = branchdf_cleaned1.withColumn("Created_date",current_timestamp())
    //define schema for product Table
    val productLine = "ProductLine_Id Int,ProductLine_Desc String,Start_Date String,End_Date String"
    var producthdf= spark.read
      .option("header", true)
      .schema(productLine)
      .csv(args(4))
    //.csv("C:\\Users\\deepu\\Documents\\Project_SuperMarket\\ProductLine.csv")
    val formattedproducthdf= producthdf.withColumn("Start_Date", to_date(col("Start_Date"), "dd/MM/yyyy"))
      .withColumn("End_Date", to_date(col("End_Date"), "dd/MM/yyyy"))
    val productdf_cleaned1 = formattedproducthdf.withColumn("ProductLine_Desc", initcap(col("ProductLine_Desc")))
    val productdf_cleaned = productdf_cleaned1.withColumn("Created_date",current_timestamp())
    superMarketdf_cleaned.show(100, false)
    branchdf_cleaned.show()
    productdf_cleaned.show()

    //HDFS
    superMarketdf_cleaned.coalesce(1).write.option("header", true).mode("overwrite").csv(args(1))
    branchdf_cleaned.coalesce(1).write.option("header", true).mode("overwrite").csv(args(3))
    productdf_cleaned.coalesce(1).write.option("header", true).mode("overwrite").csv(args(5))
    println("tables loaded into HDFS")

    //Hive
    superMarketdf_cleaned.write.mode("overwrite").saveAsTable("ukusmar.superMarket1")
    branchdf_cleaned.write.mode("overwrite").saveAsTable("ukusmar.Branch1")
    productdf_cleaned.write.mode("overwrite").saveAsTable("ukusmar.selectProductLine1")
    println("tables loaded into Hive")

    //Postgres SQL
    superMarketdf_cleaned.write.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable", "superMarket").option("driver", "org.postgresql.Driver").option("user", "consultants")
      .option("password", "WelcomeItc@2022").mode("overwrite").save()
    branchdf_cleaned.write.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable", "branch").option("driver", "org.postgresql.Driver").option("user", "consultants")
      .option("password", "WelcomeItc@2022").mode("overwrite").save()
    productdf_cleaned.write.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable", "productLine").option("driver", "org.postgresql.Driver").option("user", "consultants")
      .option("password", "WelcomeItc@2022").mode("overwrite").save()
    println("tables loaded into DB")

    /* // .mode("overwrite")
    //Mysql
    val url = "jdbc:mysql://localhost:3306/testdb"
        val username = "root"
        val password = "Kittians@01"
        val connectionProperties = new Properties()
        connectionProperties.put("user", username)
        connectionProperties.put("password", password)
        val superMarket = "superMarket"
        val branch ="branch"
        val ProductLine ="ProductLine"
        superMarketdf_cleaned.write.jdbc(url, superMarket, connectionProperties)
        branchdf_cleaned.write.jdbc(url,branch,connectionProperties)
        productdf_cleaned.write.jdbc(url, ProductLine, connectionProperties)
        */

  }
}