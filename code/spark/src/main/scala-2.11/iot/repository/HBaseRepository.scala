package iot.repository

import iot.common.Config
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Created by kiddlee on 17-1-19.
  */
trait HBaseRepository extends Config {
  val conf = HBaseConfiguration create
  val tableName = "iot"
  val zkAddress = getConfig("hbase.zkAddress")
  val zkPort = getConfig("hbase.zkPort")

  def init(): Unit = {
    val connection = getConnection
    val admin = connection.getAdmin
    if (!admin.tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
      val tableDesc = new HTableDescriptor(Bytes.toBytes(tableName))
      val gpsnoColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("gpsno"))
      tableDesc.addFamily(gpsnoColumnFamilyDesc)
      admin.createTable(tableDesc)
    }
    connection.close()
  }

  def put(rowKey: String, familyName:String, data: Map[String, String]): Unit = {
    val connection = getConnection
    val table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))
    val key = data(rowKey)
    val theput = new Put(Bytes.toBytes(key))
    for(c <- data) {
      if(c._1 != rowKey) {
        theput.add(Bytes.toBytes(familyName), Bytes.toBytes(c._1), Bytes.toBytes(c._2))
      }
    }
    table.put(theput)
    table.close()
    connection.close()
  }

  def get(key: String): Map[String, Any] = {
    val connection = getConnection
    val table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))
    var result: Map[String, Any] = Map[String, Any]()
    val theget = new Get(Bytes.toBytes(key))
    val data = table.get(theget)
    val value = data.value()
    println(Bytes.toString(value))
    result += (key -> value)
    table.close()
    connection.close()
    result
  }

  def getConnection: Connection = {
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    conf.set("hbase.zookeeper.quorum", zkAddress)
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }
}
