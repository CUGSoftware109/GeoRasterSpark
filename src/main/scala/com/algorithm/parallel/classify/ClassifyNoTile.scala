package com.algorithm.parallel.classify

import java.nio.ByteBuffer

import org.apache.hadoop
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.{SparkConf, SparkContext}

import com.hadoop.utils.TileOutputFormat

/**
  * Created by lyc on 2017/7/21.
  */
object ClassifyNoTile {
  val InvalidValue = 1.70141E38f

  val getFloatByte = (value: Float) => {
    val byteBuffer = ByteBuffer.allocate(4)
    byteBuffer.asFloatBuffer().put(value)
    byteBuffer.array.reverse.array
  }

  def classify(keyValue: (Array[Byte], Long)) = {

    if (keyValue._2 > 14){
      val elevation = ByteBuffer.wrap(keyValue._1.reverse).getFloat()
      var clazz = new Array[Byte](4)
      if (elevation == InvalidValue){
        clazz = getFloatByte(InvalidValue)
      }else if (elevation <= 1000f){
        clazz = getFloatByte(1f)
      }else if (elevation > 1000f && elevation <= 3500f){
        clazz = getFloatByte(2f)
      }else if (elevation > 3500f && elevation <= 5000f){
        clazz = getFloatByte(3f)
      }else{
        clazz = getFloatByte(4f)
      }
      (keyValue._2, clazz)
    }else {
      (keyValue._2, keyValue._1)
    }
  }

  def toOutputFormat(keyValue: (Long, Array[Byte])) = {
    (new LongWritable(keyValue._1), new BytesWritable(keyValue._2))
  }

  def main(args: Array[String]): Unit = {
    //hdfs路径为：/user/lyc, 输入参数为文件名
    val fileName = args(0)
    //val fileName = "Grid47.Ras"
    val inputPath = "hdfs://masters/user/lyc/" + fileName
    val outputPath = "hdfs://masters/user/lyc/" + fileName + "_outputRaster"

    //初始化hdfs环境
    val fdConfig = new hadoop.conf.Configuration()
    val hdfs = hadoop.fs.FileSystem.get(fdConfig)

    //输出文件存在则删除
    if (hdfs.exists(new hadoop.fs.Path(outputPath))) {
      hdfs.delete(new hadoop.fs.Path(outputPath), true)
      println("outputPath exist, delete hdfs file!")
    }
    val conf = new SparkConf().setAppName("classify")
    val sc = new SparkContext(conf)
    val gridFileRdd = sc.binaryRecords(inputPath, 4)

    gridFileRdd.zipWithIndex()
      .map(classify)
      .map(toOutputFormat)
      .saveAsNewAPIHadoopFile(outputPath,
        classOf[LongWritable],
        classOf[BytesWritable],
        classOf[TileOutputFormat])
    println("compute success!")
  }
}
