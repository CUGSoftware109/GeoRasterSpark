package com.algorithm.parallel.classify

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.zip.ZipInputStream

import com.hadoop.utils.{TileInputFormat, TileOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.log4j.xml.DOMConfigurator
import org.apache.spark.input.PortableDataStream
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.Scallop
import org.rogach.scallop.exceptions.ScallopException

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.io.{BytesWritable, LongWritable, NullWritable}

/**
  * Created by ZJA on 2016/5/12.
  */
object MountainReclassify {

  val getInt = (buff: Array[Byte], off: Int) => ByteBuffer.wrap(buff.view(off, off + 4).toArray.reverse).getInt

  val getFloatByte = (value: Float) => {
    val byteBuffer = ByteBuffer.allocate(4)
    byteBuffer.asFloatBuffer().put(value)
    byteBuffer.array.reverse.array
  }

  val getFloat = (buff: Array[Byte], off: Int) => ByteBuffer.wrap(buff.view(off, off + 4).toArray.reverse).getFloat

  val getDouble = (buff: Array[Byte], off: Int) => ByteBuffer.wrap(buff.view(off, off + 8).toArray.reverse).getFloat

  val InvalidValue = 1.70141E38f

  val logger = Logger.getLogger("\033[1;31;40m" + "Classify" + "\033[0m")

  var outputPath = ""

  val getClassify = (elevation: Float, slope: Float) => {
    var clazz = 0f
    if (elevation == InvalidValue || slope == InvalidValue) {
      clazz = InvalidValue
    } else if (elevation >= 4500f) {
      clazz = 1f
    } else if (elevation >= 3500f && elevation < 4500f) {
      clazz = 2f
    } else if (elevation >= 2500f && elevation < 3500f) {
      clazz = 3f
    } else if (elevation >= 1500f && elevation < 2500f && slope >= 2f) {
      clazz = 4f
    } else if (elevation >= 1000f && elevation < 1500f && slope >= 5f) {
      clazz = 5f
    } else if (elevation >= 300f && elevation < 1000f) {
      clazz = 6f
    } else {
      clazz = 0f
    }
    clazz
  }

  def getZippedFiles(xZippedFile: (String, PortableDataStream)) = {
    val zippedFileList = new ArrayBuffer[(Int, Array[Byte])]()
    val arrayByte = xZippedFile._2.toArray()
    var zipPartStart = 0
    var fileLength = 0
    var tileNum = 0
    while (zipPartStart < arrayByte.length) {
      tileNum = getInt(arrayByte, zipPartStart)
      fileLength = getInt(arrayByte, zipPartStart + 4)
      val data = arrayByte.view(zipPartStart + 8, zipPartStart + 8 + fileLength)
      zippedFileList.append((tileNum, data.toArray))
      zipPartStart += 8 + fileLength
    }
    zippedFileList
  }

  def unZipFile(indexedZip: (LongWritable, BytesWritable)) = {
    //    logger.info(indexedZip._1.get)
    val bais = new ByteArrayInputStream(indexedZip._2.getBytes)
    //    logger.info(getInt(indexedZip._2.getBytes, 400))
    val zip = new ZipInputStream(bais)
    val buf = new Array[Byte](1024)
    val baos = new ByteArrayOutputStream()
    while (zip.getNextEntry != null) {
      // 每个Tile一个zip，此循环只执行一次
      var num: Int = -1
      num = zip.read(buf, 0, buf.length)
      while (num != -1) {
        baos.write(buf, 0, num)
        num = zip.read(buf, 0, buf.length)
      }
      baos.flush()
      baos.close()
    }
    zip.close()
    bais.close()
    (indexedZip._1, baos.toByteArray)
  }

  def toFloatTile(unzippedTile: (LongWritable, Array[Byte])) = {
    val bytesArray = unzippedTile._2
    val floatArray = new Array[Float](bytesArray.length / 4 - 1)
    for (index <- floatArray.indices) {
      floatArray(index) = getFloat(bytesArray, (index + 1) * 4)
    }
    val tileType = getInt(bytesArray, 0) // 类型
    (unzippedTile._1.get, tileType, floatArray)
  }

  def classify(layers: (Long, Iterable[(Long, Int, Array[Float])])) = {
    var elevation = new Array[Float](0)
    var slope = new Array[Float](0)
    layers._2.foreach(a => {
      a._2 match {
        case 0 => elevation = a._3
        case 1 => slope = a._3
      }
    })
    var classifiedTile = new Array[Float](0)
    if (elevation.length != 0 && slope.length != 0 && elevation.length == slope.length) {
      classifiedTile = new Array[Float](elevation.length)
      for (index <- classifiedTile.indices) {
        classifiedTile(index) = getClassify(elevation(index), slope(index))
      }
      Some((layers._1, classifiedTile))
    }
    else {
      None
    }
  }

  def toOutputFormat(tile: (Long, Array[Float])) = {
    val arrayFloat = tile._2
    val bytesArray = new Array[Byte](arrayFloat.length * 4)
    for (index <- arrayFloat.indices) {
      getFloatByte(arrayFloat(index)).copyToArray(bytesArray, index * 4)
    }
    (new LongWritable(tile._1), new BytesWritable(bytesArray))
  }

  def main(args: Array[String]) {
    // 参数列表
    val opts = Scallop(args)
      .version("version 0.0.1") // --version option is provided for you
      // in "verify" stage it would print this message and exit
      .banner(
      """Usage: Parser -i <input file>
        |Options:
        | """.stripMargin) // --help is provided, will also exit after printing version,
      // banner, options usage, and footer
      .footer("\nFor all other tricks, consult the documentation!")
      .opt[String]("elevation", short = 'e', descr = "input elevation file", required = true)
      .opt[String]("slope", short = 's', descr = "input slope file", required = true)
      .opt[String]("output", short = 'o', descr = "output path", required = true)
      .opt[Boolean]("gzip", short = 'g', descr = "output with Gzip codec", required = true, default = () => Some(false))
      .opt[String]("logger", short = 'l', descr = "log config file(xml)")
      .opt[Boolean]("debug", short = 'd', descr = "debug the program", default = () => Some(false), hidden = true)
    // 校验参数
    try {
      opts.verify
    }
    catch {
      case e: ScallopException =>
        println(e.message)
        opts.printHelp()
        sys.exit(-1)
    }
    // 解析参数
    println(opts.summary)
    val elevationFile = opts.get[String]("elevation").get
    val slopeFile = opts.get[String]("slope").get
    outputPath = opts.get[String]("output").get
    val gzip = !opts.get[Boolean]("gzip").get // gzip取非
    val logConf = opts.get[String]("logger")
    // 初始化logger配置
    if (logConf.nonEmpty) {
      DOMConfigurator.configure(logConf.get)
    }
    // 初始化spark环境
    val conf = new SparkConf().setAppName("Parser XGrd")/*.set("spark.executor.memory", "4600mb")*/
    val sc = new SparkContext(conf)
    // 初始化hdfs环境
    val fdConfig = new Configuration()
    val hdfs = FileSystem.get(fdConfig)
    // 检查输入/输出文件是否存在，输出文件若存在则警告并删除
    if (!hdfs.exists(new Path(elevationFile))) {
      logger.error("input file \"" + elevationFile + "\" does not exist")
      sc.stop()
      sys.exit(-1)
    }
    if (!hdfs.exists(new Path(slopeFile))) {
      logger.error("input file \"" + slopeFile + "\" does not exist")
      sc.stop()
      sys.exit(-1)
    }
    if (hdfs.exists(new Path(outputPath))) {
      logger.warn("output path/file \"" + outputPath + "\" exists, removing it!")
      hdfs.delete(new Path(outputPath), true)
    }
    // 打印输入输出文件目录
    logger.info("Elevation file:\t" + elevationFile)
    logger.info("Slope file:    \t" + slopeFile)
    logger.info("Output path:   \t" + outputPath)
    // 读入高层数据并缓存
    val zipElevationRdd = sc.newAPIHadoopFile(elevationFile
      , classOf[TileInputFormat]
      , classOf[LongWritable]
      , classOf[BytesWritable]) /*.cache()*/
    // TODO:不可以cache，cache会导致第二次读取时读到最后一个瓦片
    // 读入坡度数据并缓存
    val zipSlopeRdd = sc.newAPIHadoopFile(slopeFile
        , classOf[TileInputFormat]
        , classOf[LongWritable]
        , classOf[BytesWritable]) /*.cache()*/
    // 通过简单判断数量避免错误输入
    val elevationCount = zipElevationRdd.count()
    val slopeCount = zipSlopeRdd.count()
    if (elevationCount != slopeCount) {
      logger.error("Elevation tile count: " + elevationCount + "\nSlope tile count: " + slopeCount + "\nCheck your inputs")
      sc.stop()
      sys.exit(-1)
    }
    // 解压缩高层数据
    val unZippedElevationTile = zipElevationRdd.map(unZipFile) /*.cache()*/
    // 解压坡度数据
    val unZippedSlopeTile = zipSlopeRdd.map(unZipFile) /*.cache()*/
    // 转换为Float
    val elevationFloats = unZippedElevationTile.map(toFloatTile)
    val slopeFloats = unZippedSlopeTile.map(toFloatTile)
    // 合并数据
    val mixedTile = elevationFloats.union(slopeFloats)/*.cache()*/
    logger.info("mixed tile count: " + mixedTile.count)
    val groupedTile = mixedTile.groupBy(tile => tile._1)
    val totalTileCount = groupedTile.count
    logger.info("grouped count: " + groupedTile.count)
    val classifiedTile = groupedTile.filter(t => t._2.size == 2).flatMap(classify)
    val validateTileCount = classifiedTile.count
    if (validateTileCount != totalTileCount) {
      logger.error("Tiles should have elevation and slope data, " + (totalTileCount - validateTileCount) + " is invalidate.")
      sc.stop()
      sys.exit(-1)
    }
    // 存回HDFS
    if (gzip) {
      val hdfsConf = new Configuration()
      hdfsConf.setBoolean("mapreduce.output.fileoutputformat.compress", true)
      hdfsConf.setClass("mapreduce.output.fileoutputformat.compress.codec", classOf[GzipCodec], classOf[CompressionCodec])
      classifiedTile.sortBy(a => a._1).map(toOutputFormat).saveAsNewAPIHadoopFile(outputPath
        , classOf[LongWritable]
        , classOf[BytesWritable]
        , classOf[TileOutputFormat]
        , hdfsConf
      )
    } else {
      classifiedTile.sortBy(a => a._1).map(toOutputFormat).saveAsNewAPIHadoopFile(outputPath
        , classOf[LongWritable]
        , classOf[BytesWritable]
        , classOf[TileOutputFormat]
      )
    }
    logger.info(classifiedTile.count())
    sc.stop()
    sys.exit(0)
  }
}
