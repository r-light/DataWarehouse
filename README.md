# DataWarehouse Implementing in Java

- [DataWarehouse Implementing in Java](#datawarehouse-implementing-in-java)
  - [Background](#background)
  - [Dataset](#dataset)
  - [OrinToHBase](#orintohbase)
  - [HbaseToMysql](#hbasetomysql)
  - [ExportOutdate](#exportoutdate)
  - [RecommendAlgo](#recommendalgo)

## Background

```
bjtu大数据作业
```

## Dataset

```
本次实验所使用的数据集由某中文 IT 技术社区提供，共包含 157,427 位用户在2015 年期间产生的多种类型的行为数据。数据描述请参照data文件夹。
```

## Structure

```
|-- src
|   |-- main
|   |   |-- java
|   |   |   |-- ExportOutdate
|   |   |   |   |-- ExportMain.java
|   |   |   |   |-- HBaseOperation.java
|   |   |   |-- HbaseToMysql
|   |   |   |   |-- AggregateMain.java
|   |   |   |   |-- JavaSparkPi.java
|   |   |   |   |-- MysqlTest.java
|   |   |   |   |-- SparkOperation.java
|   |   |   |   |-- UserAggre.java
|   |   |   |-- OrinToHBase
|   |   |   |   |-- HbaseBolt.java
|   |   |   |   |-- KafkaProducer.java
|   |   |   |   |-- MessageScheme.java
|   |   |   |   |-- MyProducer.java
|   |   |   |   |-- ProducerMain.java
|   |   |   |   |-- StormTopology.java
|   |   |   |-- RecommendAlgo
|   |   |       |-- RecEngine.java
```

## OrinToHBase

```
将用户基础数据、文章基础数据、用户行为日志数据按照时间顺序发送至 Kafka，并通过实时计算框架 Storm 解析原始日志数据并存储至 HBase 中。
```

## HbaseToMysql

```
利用分布式计算框架 Spark 从 HBase 读取细节数据，并聚合数据到 MySQL 中。
除去一些测试类，UserAggre定义了聚合用户类，c用于具体的spark操作，AggregateMain则是入口类。
本模块将已经存至 HBase 的数据，通过 SPpark RDD 获取，并聚合、存入MySQL。此模块主要模拟对用户画像相关信息的聚合，因此整体流程为：将需要的用户基本信息存入 Spark RDD，并进行合并；接着将用户行为存入 RDD，并进行 reduce 操作，进而记录每个用户浏览、评论、发表的次数，以及这些行为的最后一次执行时间；将这两个 RDD 进行进一步合并，并存至 MySQL。
```

```java
// SparkOperation执行流程
Init()
execute()
  extractTableUserB()
  extractTableUserE()
  extractTableUserI()
```

## ExportOutdate

```
模拟实现过期数据的导出功能。
```

## RecommendAlgo

```
推荐系统框架。
```
