package Spark_sql_tasks.exercise01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object RDDtoDataFrame {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[*]").getOrCreate()
    import session.implicits._

    val lines = sc.textFile("in/2016-stack-overflow-survey-responses.csv")

    val responseRDD = lines
      .filter(line => !line.split(Utils.COMMA_DELIMITER, -1)(2).equals("country")) // omit the header row
      .map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER, -1)
        Response(splits(2), toInt(splits(6)), splits(9), toInt(splits(14)))})

    responseRDD.take(10)

    // Method 1: Using a Schema/ StructType
    // create the schema
    val schema = new StructType()
      .add(StructField("country", StringType, true))
      .add(StructField("Age Midpoint", IntegerType, true))
      .add(StructField("Occupation", StringType, true))
      .add(StructField("Salary Midpoint", IntegerType, true))

    val df1 = session.createDataFrame(responseRDD,schema)

    // Method 2:

    val df2 = responseRDD.toDF("country",
                                "Age Midpoint,",
                                "Occupation",
                                "Salary Midpoint")

    // print Schema
    System.out.println("=== Print out schema ===")
    df2.printSchema()

    System.out.println("=== Print 20 records of responses table ===")
    df2.show(20)

    for (response <- df2.rdd.collect()) println(response)

  }

  def toInt(split: String): Option[Double] = {
    if (split.isEmpty) None else Some(split.toDouble)
  }

}

object Utils {
  // a regular expression which matches commas but not commas within double quotations
  val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
}

