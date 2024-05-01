import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, format_number, initcap, to_date, when}


object Usecaes {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("DB updates")
      .master("local[1]")
       .enableHiveSupport()
      .getOrCreate()


    // Define MySQL connection properties
    val url = "jdbc:mysql://localhost:3306/testdb"
    val username = "root"
    val password = "Kittians@01"
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", username)
    connectionProperties.put("password", password)

    // Read data from MySQL table into DataFrame
    //val tableName = "Branch" // Update with your table name
    val Branchdf = spark.read.jdbc(url, "Branch", connectionProperties)
    val Superhdf = spark.read.jdbc(url, "superMarket", connectionProperties)
    Superhdf.show(100, false)
    Branchdf.show()
    Superhdf.createOrReplaceTempView("superMarket")
    spark.sql("select * from superMarket").show()
    Branchdf.createOrReplaceTempView("Branch")
    spark.sql("select * from Branch").show()

    // Perform an inner join on the common column 'BranchId'
    val UseCasedDF1 = spark.sql("""
  SELECT sm.InvoiceID,b.Branch_Name,sm.Total
  FROM superMarket sm
  JOIN Branch b
  ON sm.BranchId = b.BranchId
""")
    // Show the joined DataFrame
    UseCasedDF1.show()
    UseCasedDF1.coalesce(1).write.option("header", true).mode("overwrite").csv(args(0))


    //hive
       UseCasedDF1.write.saveAsTable("ukusmar.UseCasedDF1")
    // DB
     /* UseCasedDF1.write.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable", "UseCasedDF1").option("driver", "org.postgresql.Driver").option("user", "consultants")
      .option("password", "WelcomeItc@2022").mode("overwrite").save()
      */

  }
}
