import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkToImpalaAndSQL {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SparkToImpalaAndSQL")
      .getOrCreate()

    // Connect to Impala and fetch 10000 rows into DataFrame
    val impalaDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:impala://impala_host:21050/default")
      .option("query", "SELECT ClientRequestId, IngestionDate FROM your_table")
      .option("user", "username")
      .option("password", "password")
      .option("fetchsize", "10000") // Fetch 10000 rows at a time
      .load()

    // Show the schema of Impala DataFrame
    impalaDF.printSchema()

    // Connect to SQL server and save the DataFrame
    val sqlUrl = "jdbc:mysql://sql_server_host:port/database"
    val sqlTable = "sql_table"
    val sqlUser = "username"
    val sqlPassword = "password"

    // Write Impala DataFrame to SQL
    impalaDF.write
      .format("jdbc")
      .option("url", sqlUrl)
      .option("dbtable", sqlTable)
      .option("user", sqlUser)
      .option("password", sqlPassword)
      .mode("append") // Choose the appropriate mode
      .save()

    // Stop SparkSession
    spark.stop()
  }
}
