package iot.common.mysql

import java.sql.{Connection, DriverManager}
import java.util

import iot.common.Config
/**
  * Created by lishulei on 3/7/17.
  */
object MySqlPool extends Config {
  private val max = getConfig("command.mysql.connectmax").toInt              //连接池连接总数
  private val connectionNum = getConfig("command.mysql.connectnum").toInt    //每次产生连接数
  private var conNum = 0                                                     //当前连接池已产生的连接数
  private val pool = new util.LinkedList[Connection]()                       //连接池
  private val driver = getConfig("mysql.driver")
  private val url = "jdbc:mysql://" +
    getConfig("command.mysql.ip") + ":"+
    getConfig("command.mysql.port")+ "/" +
    getConfig("command.mysql.database")
  private val username = getConfig("command.mysql.username")
  private val password = getConfig("command.mysql.password")

  //获取连接
  def getJdbcConn() : Connection = {
    //同步代码块
    AnyRef.synchronized({
      if(pool.isEmpty){
        //加载驱动
        preGetConn()
        for(i <- 1 to connectionNum){
          val conn = DriverManager.getConnection(url,username,password)
          pool.push(conn)
          conNum +=  1
        }
      }
      pool.poll()
    })
  }


  //释放连接
  def releaseConn(conn:Connection): Unit ={
    pool.push(conn)
  }
  //加载驱动
  private def preGetConn() : Unit = {
    //控制加载
    if(conNum < max && !pool.isEmpty){
      println("Jdbc Pool has no connection now, please wait a moments!")
      //Thread.sleep(2000)
      preGetConn()
    }else{
      Class.forName(driver);
    }
  }
}
