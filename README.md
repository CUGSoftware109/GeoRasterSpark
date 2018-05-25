# SparkGeoCompute


SparkGeoCompute is a distributed computing method for geographical and raster data based on Spark distributed computing framework. It is divided into two types of algorithms: local map algebra calculation (mountain classification) and global map algebra calculation (visual analysis).

[Chinese-Introduction](https://github.com/CUGSoftware109/SparkGeoCompute/blob/master/README_CN.md)



# Function

1.Standalone visual field LOS(line of sight) algorithm.

2.Parallel los algorithm based on Spark uses equal-area method to divide the original raster data to achieve data parallelism.

3.Parallel mountain classification algorithm of original Raster File on Spark enviroment.

4.Parallel mountain classification algorithm of tile segmentation on Spark enviroment.


# 项目结构

/src/main/scala/com/algorithm/single/los/： The implementation of the single machine visible domain LOS algorithm .

/src/main/scala/com/algorithm/parallel/los/： Parallel region los algorithm with equal area division.

/src/main/scala/com/algorithm/parallel/classify/： Mountain classification parallel algorithm using tile segmentation data and mountain classification algorithm directly based on original raster data.

/src/main/scala/com/hadoop/utils/：The hadoop utility class that assists in reading and writing spark data.


# Running environment and development tools

running environment：Centos6.5、JDK1.7、scala-2.10.4、Spark-1.5.1

development tools：IntelliJ IDEA、Maven
