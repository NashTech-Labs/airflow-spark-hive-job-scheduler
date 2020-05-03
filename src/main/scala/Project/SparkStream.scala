package Project

import org.apache.spark.sql.functions.{col, lit, split, to_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkStream {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Kafka_Spark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "kfktpksix").load()

    val mysql_host_name = "localhost"
    val mysql_port_no = "3306"
    val mysql_user_name = "root"
    val mysql_password = "password"
    val mysql_database_name = "test1"
    val mysql_driver_class = "com.mysql.cj.jdbc.Driver"
    val mysql_table_name = "weblogdetails5"
    val mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

    val properties = new java.util.Properties()
    properties.setProperty("driver", mysql_driver_class)
    properties.setProperty("user", mysql_user_name)
    properties.setProperty("password", mysql_password)

    val df1 = df.selectExpr("CAST(value as String)")

    val df2 = df1.withColumn("valueSplit", split(col("value"), ","))
      .withColumn("datevalue", to_timestamp(col("valueSplit").getItem(0), "yyyy/MM/dd HH:mm:ss"))
      .withColumn("ipaddress", col("valueSplit").getItem(1))
      .withColumn("host", col("valueSplit").getItem(2))
      .withColumn("url", col("valueSplit").getItem(3))
      .withColumn("responsecode", col("valueSplit").getItem(4).cast("Integer"))
      .drop("valueSplit", "value")

    df2.printSchema()

        df2.write.
          mode("append").jdbc(mysql_jdbc_url, mysql_table_name, properties)

  }
}
