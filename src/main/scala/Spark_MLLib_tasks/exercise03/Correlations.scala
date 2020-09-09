package Spark_MLLib_tasks.exercise03

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

object Correlations {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val appName = "MLLibCorrelations"
    val master = "local[*]"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)

    // a series generated with normal distribution
    val rddX = RandomRDDs.normalRDD(sc, 100000, 2)
    val rddY = RandomRDDs.normalRDD(sc, 100000, 2)

    // Apply a transform to get a random double RDD following `N(1, 4)`.
    val seriesX: RDD[Double] = rddX.map(x => 1.0 + 2.0 * x)

    // must have the same number of partitions and cardinality as seriesX
    val seriesY: RDD[Double] = rddY.map(x => 0.5 + 3.0 * x)

    // have a look at generated normal distributions
    seriesX.take(20)
    seriesY.take(20)

    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
    // method is not specified, Pearson's method will be used by default.
    // Note: same method can be applied to a Vector RDD as well ( RDD[Vector] )
    val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")

    // Vector RDD : Output will be a Matrix

    val data  = RandomRDDs.uniformVectorRDD(sc, 10,10)
    .map(a => DenseVector)
      .map(a => (a,))
    .toDF()

    val correlMatrix: Matrix = Statistics.corr(data, "pearson")

    println("Correlation is: " + correlation)

  }

}
