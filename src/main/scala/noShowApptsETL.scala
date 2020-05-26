import org.slf4j.LoggerFactory
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, lit, rand, round, typedLit, to_timestamp}


object noShowApptsETL {

  def main(args: Array[String]): Unit = {

    // Setting up logging info
    val logger = LoggerFactory.getLogger(getClass.getSimpleName)

    try {
      // Setting up Spark session
      val spark: SparkSession = SparkSession.builder()
        .master("local[*]")
        .appName("simple-app")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      import spark.implicits._

      logger.info("Importing data into Spark DataFrame that will be used for Cassandra tables")
      val path = "data/no_show_appointments.csv"
      val rawDF: DataFrame = spark.read.option("header", "true").csv(path).toDF()

      logger.info("Transforming the data")
      // dropping unnecessary columns
      val droppedColumnsDF: DataFrame = rawDF.drop("Neighborhood").drop("Scholarship")

      // adding a column with states randomly selected from a predefined list
      val stateIntDF: DataFrame = {
        droppedColumnsDF.withColumn("StateId", round(rand() * (9 - 1) + 1, 0).cast("int"))
      }

      val stateDF: DataFrame = Seq(
        (1, "AL"),
        (2, "CA"),
        (3, "CO"),
        (4, "DC"),
        (5, "FL"),
        (6, "GA"),
        (7, "MD"),
        (8, "TX"),
        (9, "VA")
      ).toDF("StateId", "StateName")

      val joinedDF = stateIntDF.join(
        stateDF
        , "StateId").drop("StateId")

      // replaces 0s and 1s with False and True to be more readable
      val booleanMap: Column = typedLit(Map(
        0 -> "False",
        1 -> "True"
      ))

      val cleanedUpDF: DataFrame = Seq(
        "Hypertension",
        "Diabetes",
        "Alcoholism",
        "Handcap",
        "SMS_Received"
      ).foldLeft(joinedDF) {
        (tempDf, colName) => tempDf.withColumn(colName,coalesce(booleanMap(col(colName)),lit("Null")))
      }

      // Renaming the columns to have correct spelling and consistent naming convention
      val renamedDF: DataFrame = {
        cleanedUpDF.withColumnRenamed("Handcap","Handicap")
          .withColumnRenamed("SMS_Received","SMSReceived")
          .withColumnRenamed("No_Show","NoShow")
      }

      // Converting data columns from object to datetime
      val finalDF: DataFrame = Seq(
        "ScheduledDay",
        "AppointmentDay",
      ).foldLeft(renamedDF) {
        (tempDf, colName) => tempDf.withColumn(colName,to_timestamp(col(colName),"yyyy-MM-dd'T'HH:mm:ss'Z'"))
      }

      logger.info("Loading data into the tables")

    } catch {

      case e: Throwable => logger.info("I am a log message with exception", e)

    }
  }
}
