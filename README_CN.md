# GeoRasterSpark

GeoRasterSpark是基于Spark分布式计算框架的地理栅格数据的分布式计算方法，主要分为局部地图代数计算（山地分类）和全局地图代数计算（可视域分析）两类算法。


# 功能点

1.单机可视域los算法

2.基于spark的并行los算法，采用等面积划分法划分原始栅格数据，达到数据并行的目的

3.基于原始栅格文件的spark并行山地分类算法

4.基于瓦片分割的spark并行山地分类算法


# 项目结构

/src/main/scala/com/algorithm/single/los/： 单机可视域los算法。

/src/main/scala/com/algorithm/parallel/los/： 采用等面积划分的并行可视域los算法。

/src/main/scala/com/algorithm/parallel/classify/： 采用瓦片分割数据的山地分类并行算法和直接基于原始栅格数据的山地分类算法。

/src/main/scala/com/hadoop/utils/：辅助spark数据读取和输出的hadoop工具类。


# 运行环境和开发工具

运行环境：Centos6.5、JDK1.7、scala-2.10.4、Spark-1.5.1

开发工具：IntelliJ IDEA、Maven
