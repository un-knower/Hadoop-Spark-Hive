package libin.spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by baolibin on 2017/10/6.
  * KMeans算法
  */
object KMeans {
  def main(args: Array[String]) {
    val dataPath = "src/main/com/libin/spark/mllib/"
    //1 构建Spark对象
    val conf = new SparkConf()
      .setAppName("KMeans")
      .setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据1，格式为LIBSVM format
    val data = sc.textFile(dataPath + "data/kmean.txt")
    val parsedData = data.map(s => Vectors.dense(s.split('\t').map(_.toDouble))).cache()

    // 新建KMeans聚类模型，并训练
    //val initMode = "k-means"
    val initMode = "k-means++"
    //val initMode = "k-means||"
    val numClusters = 5
    val numIterations = 100

    val model = new KMeans()
      .setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .run(parsedData)
    val centers = model.clusterCenters
    println("centers")
    for (i <- centers.indices) {
      println(centers(i)(0) + "\t" + centers(i)(1))
    }
    // 误差计算
    val WSSSE = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    //保存模型
    /**val ModelPath = dataPath + "model/kmean"
    model.save(sc, ModelPath)
    val sameModel = KMeansModel.load(sc, ModelPath)*/
  }
}
