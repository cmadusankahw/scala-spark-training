package Spark_Scala_tasks.exercise03

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RealEstateDataFrames {

  val PRICE_SQ_FT = "Price SQ Ft"

  def main(args: Array[String]) {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder().appName("RealStateDataFrames").master("local[2]").getOrCreate()

  val realEstate = session.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("in/RealEstate.csv")

  realEstate.groupBy("Location")
    .avg(PRICE_SQ_FT)
    .orderBy("avg(Price SQ Ft)")
    .show()
}

}
