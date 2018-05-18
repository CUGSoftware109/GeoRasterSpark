package com.algorithm.single.los

import java.io._
import java.nio.ByteBuffer


/**
  * Created by xiadc on 2018-02-28.
  */
object SingleLosAlgorithm {

  //grid无效值
  val InvalidValue = 1.70141E38f
  //判断两个double类型的值的大小用
  val eps = 1E-9

  var calGrdZValue = new Array[Float](0)

  val eyePnt = new Point()
  //demo47视点
  /*eyePnt.x = 393820.0d
  eyePnt.y = 3258990.0d
  eyePnt.z = 600.0d*/
  //hubei视点
  /*eyePnt.x = 114.0d
  eyePnt.y = 30.5d
  eyePnt.z = 500.0d*/
  //shanxi视点
 /* eyePnt.x = 108.84d
  eyePnt.y = 34.53d
  eyePnt.z = 2800.0d*/
  //qinghai视点
 /* eyePnt.x = 98.804d
  eyePnt.y = 36.227d
  eyePnt.z = 6300.0d*/
  //xizang视点
  /*eyePnt.x = 88.655d
  eyePnt.y = 30.808d
  eyePnt.z = 8000.0d*/
  //xinjiang视点
  eyePnt.x = 84.962d
  eyePnt.y = 42.233d
  eyePnt.z = 3500.0d


  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    doLos("D:\\sparkTestData\\xizang.Ras")
    println("success")
    val endTime = System.currentTimeMillis()
    val time = (endTime - startTime)/1000
    println("用时"+time+"秒")

  }

  //获取文件元信息
  def doLos(filePath: String): Unit = {
    val file = new File(filePath)
    val fis = new FileInputStream(file)
    val bis = new BufferedInputStream(fis)
    val bos = new BufferedOutputStream(new FileOutputStream("D:\\sparkTestData\\newtif\\test111"))
    val bytes = new Array[Byte](4)
    val bytes8 = new Array[Byte](8)

    //1.如何得到DEM文件的左下角经纬度坐标xmin,ymin
    //2.如何得到DEM文件的网格间距（每两个像素之间的经纬度数）xgap和ygap
    //3.如何得到DEM文件的元素个数的行列数nx，ny
    //4.根据步骤1、2、3设置CalDemInfo的值
    bis.read(bytes) //读出文件头4个字节
    //读出nx
    bis.read(bytes)
    CalDemInfo.nx = ByteBuffer.wrap(bytes.reverse).getInt().toLong
    //读出ny
    bis.read(bytes)
    CalDemInfo.ny = ByteBuffer.wrap(bytes.reverse).getInt().toLong
    //读出xmin
    bis.read(bytes8)
    CalDemInfo.xmin = ByteBuffer.wrap(bytes8.reverse).getDouble()
    //读出xmax
    bis.read(bytes8)
    CalDemInfo.xmax = ByteBuffer.wrap(bytes8.reverse).getDouble()
    //读出ymin
    bis.read(bytes8)
    CalDemInfo.ymin = ByteBuffer.wrap(bytes8.reverse).getDouble()
    //读出ymax
    bis.read(bytes8)
    CalDemInfo.ymax = ByteBuffer.wrap(bytes8.reverse).getDouble()
    //计算xgap，ygap
    CalDemInfo.xgap = (CalDemInfo.xmax - CalDemInfo.xmin) / (CalDemInfo.nx - 1)
    CalDemInfo.ygap = (CalDemInfo.ymax - CalDemInfo.ymin) / (CalDemInfo.ny - 1)
    println(CalDemInfo.nx+" "+CalDemInfo.ny+" "+ CalDemInfo.xmin+" "+CalDemInfo.xmax+" "+CalDemInfo.ymin+" "+CalDemInfo.ymax)
    //读取z值最大最小
    bis.read(bytes8)
    bis.read(bytes8)

    calGrdZValue = new Array[Float]((CalDemInfo.nx * CalDemInfo.ny).toInt)
    var len = 0
    var index = 0
    val bytesbuf = new Array[Byte](2048 * 2)
    val tmpBytes = new Array[Byte](4)
    len = bis.read(bytesbuf)
    while (len != -1) {
      for (i <- 0 to (len / 4) - 1) {
        tmpBytes(0) = bytesbuf(i * 4)
        tmpBytes(1) = bytesbuf(i * 4 + 1)
        tmpBytes(2) = bytesbuf(i * 4 + 2)
        tmpBytes(3) = bytesbuf(i * 4 + 3)

        calGrdZValue(index) = ByteBuffer.wrap(tmpBytes.reverse).getFloat()
        index += 1
      }
      len = bis.read(bytesbuf)
    }

    //开始计算
    var elevation = 0.0f
    var px = 0.0
    var py = 0.0


    for (i <- 0 to calGrdZValue.length  - 1) {
      elevation = calGrdZValue(i)
      var tempByte:Byte = 2
      if (doubleEquals(elevation, InvalidValue)) {
        //高程值无效值，直接视为可视
        //tempByteArray = getFloatByte(InvalidValue)
      } else {
        px = CalDemInfo.xmin + (i % CalDemInfo.nx) * CalDemInfo.xgap
        py = CalDemInfo.ymin + (i / CalDemInfo.nx) * CalDemInfo.ygap
        tempByte = if (isVisible(px, py, elevation)) 0 else 1
      }
     // println(tempByte)
      bos.write(tempByte)

    }
      //关闭输入流
      bis.close()
      fis.close()
      bos.flush()
      bos.close()
      //fis.close()

  }
    //LOS可视性算法实现，输入参数是目标点的坐标与高程值
    def isVisible(px: Double, py: Double, pz: Double): Boolean = {

      val xmin: Array[Double] = new Array[Double](2)
      val ymin: Array[Double] = new Array[Double](2)
      val xtap: Array[Double] = new Array[Double](2)
      val ytap: Array[Double] = new Array[Double](2)
      val nx: Array[Long] = new Array[Long](2)
      val ny: Array[Long] = new Array[Long](2)

      //后面交换使用
      xmin(0) = CalDemInfo.xmin
      ymin(1) = CalDemInfo.xmin

      ymin(0) = CalDemInfo.ymin
      xmin(1) = CalDemInfo.ymin

      xtap(0) = CalDemInfo.xgap
      ytap(1) = CalDemInfo.xgap

      ytap(0) = CalDemInfo.ygap
      xtap(1) = CalDemInfo.ygap

      nx(0) = CalDemInfo.nx
      ny(1) = CalDemInfo.nx

      nx(1) = CalDemInfo.ny
      ny(0) = CalDemInfo.ny

      var dx, dy, k, x0, y0, x1, x, y, dz, uz, z, phi, ydelt = 0.0
      var i, si, ei, j, No = 0L
      var swapFlag = 0
      var FoundFlag = false
      var temp = new Point()
      var pnt = new Array[Point](2)
      //深拷贝视点数据，防止视点变化
      pnt(0) = new Point
      pnt(0).x = eyePnt.x
      pnt(0).y = eyePnt.y
      pnt(0).z = eyePnt.z

     // pnt(0) = eyePnt
      pnt(1) = new Point

      pnt(1).x = px
      pnt(1).y = py
      pnt(1).z = pz

      //保证视点比目标点低
      if (pnt(0).z > pnt(1).z) {
        temp = pnt(0);
        pnt(0) = pnt(1);
        pnt(1) = temp
      }
      //统一两点连线斜率到一种情况
      dx = pnt(1).x - pnt(0).x
      dy = pnt(1).y - pnt(0).y
      if (Math.abs(dy) > Math.abs(dx)) {
        //X和Y互换
        x0 = dx;
        dx = dy;
        dy = x0
        x0 = pnt(0).x;
        pnt(0).x = pnt(0).y;
        pnt(0).y = x0
        x0 = pnt(1).x;
        pnt(1).x = pnt(1).y;
        pnt(1).y = x0
        swapFlag = 1
      }

      //起始点坐标
      x0 = Math.min(pnt(0).x, pnt(1).x)
      x1 = Math.max(pnt(0).x, pnt(1).x)
      y0 = if (doubleEquals(x0, pnt(0).x)) pnt(0).y else pnt(1).y
      k = dy / dx

      //加入指定范围的考虑
      si = ((x0 - xmin(swapFlag)) / xtap(swapFlag)).toLong
      if (si < 0) si = 0
      if (si > (if (swapFlag == 0) CalDemInfo.nx - 1 else CalDemInfo.ny - 1)) si = if (swapFlag == 0) CalDemInfo.nx - 1 else CalDemInfo.ny - 1

      ei = ((x1 - xmin(swapFlag)) / xtap(swapFlag)).toLong + 1
      if (ei < 0) ei = 0
      if (ei > (if (swapFlag == 0) CalDemInfo.nx - 1 else CalDemInfo.ny - 1)) ei = if (swapFlag == 0) CalDemInfo.nx - 1 else CalDemInfo.ny - 1

      //视线倾角.
      phi = (pnt(1).z - pnt(0).z) / distance(pnt(0).x, pnt(0).y, pnt(1).x, pnt(1).y)
      x = xmin(swapFlag) + si * xtap(swapFlag)

      //起始Y及步长.
      y = y0 + k * (x - x0)
      ydelt = k * xtap(swapFlag)

      for (i <- si + 1 to ei - 1 if !FoundFlag) {
        x += xtap(swapFlag)
        y += ydelt

        j = ((y - ymin(swapFlag)) / ytap(swapFlag)).toLong

        //越界处理
        if (swapFlag == 0) {
          j = if ((j > (CalDemInfo.ny - 2))) (CalDemInfo.ny - 2) else j
        } else {
          j = if ((j > (CalDemInfo.nx - 2))) (CalDemInfo.nx - 2) else j
        }
        if (swapFlag == 0) {
          No = j * CalDemInfo.nx + i + 1
          dz = calGrdZValue(No.toInt)
          uz = calGrdZValue((No + CalDemInfo.nx).toInt)
        } else {
          No = i * CalDemInfo.nx + j + 1
          dz = calGrdZValue(No.toInt)
          uz = calGrdZValue((No + 1).toInt)
        }

        /*dz = if (swapFlag == 0) CalGrdZValue(No = j * CalDemInfo.nx + i + 1) else CalGrdZValue(No = i * CalDemInfo.nx + j + 1) //CalGrdZValue是DEM高程值数组.
          uz = if (swapFlag == 0) CalGrdZValue(No + CalDemInfo.nx) else CalGrdZValue(No + 1)*/

        //无效点作为可视情况处理,跳过.(暂不考虑Surfer无效值)
        if (doubleEquals(dz, InvalidValue) || doubleEquals(uz, InvalidValue)) {
          //不处理，当做可视
        } else {
          //线性插值.(Down,Upper)
          z = dz + (y - (ymin(swapFlag) + j * ytap(swapFlag))) * (uz - dz) / ytap(swapFlag)
          if (doubleLEquals(z, phi * distance(pnt(0).x, pnt(0).y, x, y) + pnt(0).z)) {
            //计算结果可视
          } else {
            //计算结果不可视
            FoundFlag = true
          }
        }
      }
      FoundFlag
    }


    //坐标点(x1,y1)与(x2,y2)之间的距离()
    private val distance = (x1: Double, y1: Double, x2: Double, y2: Double) => Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2))
      //判断a是否等于b
    private val doubleEquals = (a: Double, b: Double) => Math.abs(a - b) <= eps

        //判断a是否小于或等于b
        private val doubleLEquals = (a: Double, b: Double) => (Math.abs(a - b) <= eps) || (a - b <= 0)
          //float转字节数组
          val getFloatByte = (value: Float) => {
            val byteBuffer = ByteBuffer.allocate(4)
            byteBuffer.asFloatBuffer().put(value)
            byteBuffer.array.reverse.array

          }


}
/*---------------------以下是定义的辅助类-----------------------------------*/

//表示点的十进制经纬度坐标和高程值
class Point extends Serializable{
  var x: Double  = 0.0
  var y: Double = 0.0
  var z: Double = 0.0
}

//栅格数据元数据信息存储
object CalDemInfo {
  //dem左下角
  var xmin: Double = 0.0
  var ymin: Double = 0.0

  var xmax: Double = 0.0
  var ymax: Double = 0.0

  //可视域分析网格间距，默认与DEM一样
  var xgap: Double = 0.0
  var ygap: Double = 0.0
  //dem行列数
  var nx: Long = 0L
  var ny: Long = 0L
}