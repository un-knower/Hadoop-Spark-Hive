package libin.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import scala.collection.JavaConversions
import scala.collection.immutable.HashSet

/**
  * Created by baolibin on 2017/10/5.
  * Spark-Version 1.6.0
  * Hadoop-Version 2.6.0
  * RDD一些算子的学习
  */
object RddLearn {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("RddLearn").setMaster("local"))

    //1、创建RDD，value型RDD
    val trainRddK: RDD[String] = sc.parallelize(List("hadoop", "hive", "spark", "oozie", "redis", "solr", "hdfs", "mapreduce", "hive", "hadoop", "hive"), 4)
    //2、生成(key,value)型RDD ,这里用keyBy算子生成。
    val trainRddKv: RDD[(Int, String)] = trainRddK.keyBy(_.length)
    //3、遍历RDD元素
    trainRddK.collect().foreach(println)
    trainRddKv.collect().foreach(println)
    //4、查看RDD分区个数
    println(trainRddK.partitions.length)
    println(trainRddKv.partitions.length)
    //5、分区
    val trainRddKP = trainRddK.repartition(2) //coalesce
    val trainRddKvp = trainRddKv.partitionBy(new HashPartitioner(2))
    println(trainRddKP.partitions.length)
    println(trainRddKvp.partitions.length)
    //6、aggregateByKey     此例应用：wordCount|过滤掉出现多次数据
    def seqOpA(init: Int, num: Int): Int = init + num
    def combOpA(p1: Int, p2: Int): Int = p1 + p2
    val aggregateRdd = trainRddK.map((_, 1)).aggregateByKey(0)(seqOpA, combOpA).collect()
    aggregateRdd.sortBy(_._2).reverse.foreach(println)
    //7、reduceByKey
    val reduceRdd = trainRddK.map((_, 1)).reduceByKey(_ + _).collect()
    reduceRdd.sortBy(_._2).reverse.foreach(println)
    //8、combineByKey
    val combineRdd = trainRddK.map((_, 1)).combineByKey(
      C => 1,
      (C1: Int, C2: Int) => C1 + C2,
      (p1: Int, p2: Int) => p1 + p2
    ).collect()
    combineRdd.sortBy(_._2).reverse.foreach(println)
    //9、take
    sc.makeRDD(trainRddK.take(3)).foreach(println)
    //10、sample   withReplacement表示是否为放回抽样(true表示有放回);fraction表示抽样比例;seed为随机种子
    trainRddK.sample(withReplacement=false,0.5,10).foreach(println)
    //11、takeSample   withReplacement是否放回抽样;num为取样个数;seed为随机种子
    sc.makeRDD(trainRddK.takeSample(withReplacement=false,3,10)).foreach(println)
  }
}
