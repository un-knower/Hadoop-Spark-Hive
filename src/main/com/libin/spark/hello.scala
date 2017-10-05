package libin.spark

import org.apache.spark.{SparkConf, SparkContext}

object hello {
  def main(args: Array[String]): Unit = {

    val sc=new SparkContext(new SparkConf().setAppName("hello").setMaster("local"))
    val trainData = sc.parallelize(Array(3,5,7))

    trainData.collect().foreach(println)
  }
}
