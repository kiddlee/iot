package iot.repository

import iot.common.Log
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Created by kiddlee on 17-3-20.
  */
object DeviceRepository extends HBaseRepository with Log {
  override val tableName = "iot_device"

  override def init(): Unit = {
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    conf.set("hbase.zookeeper.quorum", zkAddress)
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    println(tableName)
    if (!admin.tableExists(TableName.valueOf(Bytes.toBytes(tableName)))) {
      val tableDesc = new HTableDescriptor(Bytes.toBytes(tableName))
      val infoColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("info"))
      val statusColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("status"))
      val timerColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("timer"))
      val latlngColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("latlng"))
      tableDesc.addFamily(infoColumnFamilyDesc)
      tableDesc.addFamily(statusColumnFamilyDesc)
      tableDesc.addFamily(timerColumnFamilyDesc)
      tableDesc.addFamily(latlngColumnFamilyDesc)
      admin.createTable(tableDesc)
    }
    connection.close()
  }

  def putStatus(deviceno: String, temp: String, humidity: String, pm25: String, mode: String): Unit = {
    var status: Map[String, String] = Map[String, String]()
    status += ("deviceno" -> deviceno.reverse)
    status += ("temp" -> temp)
    status += ("humidity" -> humidity)
    status += ("pm25" -> pm25)
    status += ("mode" -> mode)
    put("deviceno", "status", status)
  }
}
