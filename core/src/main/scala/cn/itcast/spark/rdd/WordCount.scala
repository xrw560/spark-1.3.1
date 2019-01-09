package cn.itcast.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2016/5/27.
  */
object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount")
    //SparkContext是通向Spark集群的入口
    //SparkContext实例在SparkSubmit(Driver) -> 跟Master建立连接
    //创建DAGScheduler -> TaskScheduler
    val sc = new SparkContext()
    //构建RDD并调用了他的Transformation
    val wordAndCount: RDD[(String, Int)] = sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    //TODO 调用RDD的Action开始真正提交任务
    wordAndCount.saveAsTextFile(args(1))
    //关闭SparkContext释放资源
    sc.stop()
  }
}
