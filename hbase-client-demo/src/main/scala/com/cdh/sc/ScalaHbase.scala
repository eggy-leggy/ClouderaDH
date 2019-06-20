package com.cdh.sc

import java.io.IOException
import java.util.List
import java.util.UUID
import java.util.Map
import java.util.concurrent.ConcurrentHashMap
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object ScalaHbase {
  //private val LOG: Logger = LoggerFactory.getLogger(classOf[ScalaHbase])
  def LOG = LoggerFactory.getLogger(getClass)

  def getHbaseConf: Configuration = {
    val conf: Configuration = HBaseConfiguration.create
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("spark.executor.memory", "3000m")
    conf.set("hbase.zookeeper.quorum", "node1,node2,node3")
    conf.set("hbase.master", "node1:6000")
    conf.set("hadoop.home.dir", ".")
    conf
  }

  @throws(classOf[MasterNotRunningException])
  @throws(classOf[ZooKeeperConnectionException])
  @throws(classOf[IOException])
  def createTable(conf: Configuration, hbaseconn: Connection, tablename: String, columnFamily: Array[String]) {
    val admin: HBaseAdmin = hbaseconn.getAdmin.asInstanceOf[HBaseAdmin]

    if (admin.tableExists(tablename)) {
      LOG.info(tablename + " Table exists!")
      //      val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename))
      //      tableDesc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")
    }
    else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename))
      tableDesc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")
      for (i <- 0 to (columnFamily.length - 1)) {
        val columnDesc: HColumnDescriptor = new HColumnDescriptor(columnFamily(i));
        tableDesc.addFamily(columnDesc);
      }
      admin.createTable(tableDesc)
      LOG.info(tablename + " create table success!")
    }
    admin.close
  }

  @throws(classOf[IOException])
  def addRow(table: Table, rowKey: String, columnFamily: String, key: String, value: String) {
    //    LOG.info("put '" + rowKey + "', '" + columnFamily + ":" + key + "', '" + value + "'")

    val rowPut: Put = new Put(Bytes.toBytes(rowKey))
    if (value == null) {
      rowPut.addColumn(columnFamily.getBytes, key.getBytes, "".getBytes)
    } else {
      rowPut.addColumn(columnFamily.getBytes, key.getBytes, value.getBytes)
    }
    table.put(rowPut)
  }

  @throws(classOf[IOException])
  def putRow(rowPut: Put, table: HTable, rowKey: String, columnFamily: String, key: String, value: String) {
    //    val rowPut: Put = new Put(Bytes.toBytes(rowKey))
    //    println("put '" + rowKey + "', '" + columnFamily + ":" + key + "', '" + value + "'")
    if (value == null) {
      rowPut.add(columnFamily.getBytes, key.getBytes, "".getBytes)
    } else {
      rowPut.add(columnFamily.getBytes, key.getBytes, value.getBytes)
    }
    table.put(rowPut)

  }

  @throws(classOf[IOException])
  def getRow(table: HTable, rowKey: String): Result = {
    val get: Get = new Get(Bytes.toBytes(rowKey))
    val result: Result = table.get(get)
    return result
  }

  /**
    * 鎵归噺娣诲姞鏁版�?
    *
    * @param list
    * @throws IOException
    */
  def addDataBatch(table: HTable, list: List[Put]) {
    try {
      table.put(list)
    }
    catch {
      case e: RetriesExhaustedWithDetailsException => {
        LOG.error(e.getMessage)
      }
      case e: IOException => {
        LOG.error(e.getMessage)
      }
    }
  }

  /**
    * 鏌ヨ鍏ㄩ儴
    */
  def queryAll(table: HTable) {
    val scan: Scan = new Scan
    try {
      val s = new Scan()
      val results = table.getScanner(s)

    }
    catch {
      case e: IOException => {
        LOG.error(e.toString)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    queryAll("hbasecarttest");
  }
}
