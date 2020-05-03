package Project

import org.apache.spark.sql.SparkSession

object spark_hive_aggregation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("spark_hive")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://localhost/metastore?createDatabaseIfNotExist=true")
      .config("javax.jdo.option.ConnectionUserName", "hiveuser")
      .config("javax.jdo.option.ConnectionPassword", "hivepassword")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val mysql_host_name = "localhost"
    val mysql_port_no = "3306"
    val mysql_user_name = "root"
    val mysql_password = "password"
    val mysql_database_name = "test1"
    val mysql_driver_class = "com.mysql.cj.jdbc.Driver"
    val mysql_table_name = "hive_agg"
    val mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

    val properties = new java.util.Properties()
    properties.setProperty("driver", mysql_driver_class)
    properties.setProperty("user", mysql_user_name)
    properties.setProperty("password", mysql_password)

    spark.sql("show databases").show()

    spark.sql("show tables").show()

    spark.sql("select * from weblog_partiton_table").show()

    val df = spark.sql("select host, ipaddress, count(url) as Total_count, sum(responsecode) as Total_Response from weblog_partiton_table group by host, ipaddress order by host, ipaddress")
    df.write.mode("append").jdbc(mysql_jdbc_url, mysql_table_name, properties)

  }
}
