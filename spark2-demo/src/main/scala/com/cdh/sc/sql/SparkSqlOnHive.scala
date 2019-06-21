package com.cdh.sc.sql

import org.apache.spark.sql.SparkSession

object SparkSqlOnHive {
  def main(args: Array[String]): Unit = {

    /**
      * 如果要操作集群上的hive库，需要先将服务器中core-site.xml，hdfs-site.xml，hive-site.xml 三个文件copy至resources中，缺少前两个文件将无法找到hadoop环境，不能正常执行
      * core-site.xml：集群地址，连接方式配置
      * hdfs-site.xml：集群分布式文件系统配置
      * hive-site.xml：集群hive warehouse路径等信息配置，缺少此文件，将会在本地创建hive文件夹，文件位置为spark.sql.warehouse.dir 参数指定位置
      *
      * 需修改上述配置文件中对应链接地址
      * */
    val spark = SparkSession.builder()
      .appName("SparkSqlOnHive")
      .master("local") // 运行模式 ， 一般提交时指定 yarn模式
      .config("spark.sql.warehouse.dir", "hdfs://node1/tmp/spark2/hive_test") // 如果hive未指定warehouse 使用此路径
      .enableHiveSupport() // 使spark-sql 支持hive操作
      .getOrCreate() // 创建 sparkSession对象
    import spark.sql // 导入hivesql支持
    //    sql("DROP TABLE IF EXISTS src")                 // 删除表
    //    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")   // 创建表
    //    val resData = sql("SELECT * FROM bmr_test where channelnumber = '1' and absolutetimestamp > 1560401770238 limit 100")
    val resData = sql("SELECT idhex,data[0] as data0,data[1] as data1,data[2] as data2 from (SELECT idhex,split(datahex,' ') as data FROM bmr_test where channelnumber = '1' and absolutetimestamp > 1560401770238 limit 100) as t")
    resData.show()  // 打印查询结果
    spark.close()
  }
}
