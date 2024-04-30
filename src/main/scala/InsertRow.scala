import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object InsertRow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hive updates")
      .enableHiveSupport()
      .getOrCreate()

    // Read data from Hive table into DataFrame
    val tableName = "ukusmar.branch1" // Update with your Hive table name
    val df = spark.table(tableName)
    df.show(100, false)
    // Create a DataFrame with the new rows
    val data = Seq(
      Row(1, "D", "Dallas", "2020-01-01", ""),
      Row(2, "E", "Chicago", "2020-01-01", "")
    )
    val schema = new StructType()
      .add("BranchId", IntegerType)
      .add("Branch_Name", StringType)
      .add("City_Name", StringType)
      .add("Start_Date", StringType)
      .add("End_Date", StringType)

    val newRowDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    newRowDF.show()

    // Append the new DataFrame to the existing DataFrame
    val updatedDF = df.union(newRowDF)

    // Write the updated DataFrame back to the Hive table
    newRowDF.write.mode("append").saveAsTable(tableName)

    // Stop Spark session
    spark.stop()
  }
}