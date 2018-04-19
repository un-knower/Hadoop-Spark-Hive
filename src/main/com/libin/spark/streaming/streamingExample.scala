package libin.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by baolibin on 2017/10/22.
  */
object streamingExample {
  def main(args: Array[String]): Unit = {
    val dataPath = "src/main/com/libin/spark/streaming/data"
    val conf: SparkConf = new SparkConf().setAppName("streamingExample").setMaster("local[6]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val lines = ssc.textFileStream(dataPath)
    val words: DStream[(String, Int)] = lines.flatMap(_.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _)

    words.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
