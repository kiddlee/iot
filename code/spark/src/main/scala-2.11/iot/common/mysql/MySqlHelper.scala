package iot.common.mysql

/**
  * Created by lishulei on 3/7/17.
  */
import java.sql.{Connection, DriverManager}
import iot.common.{Config, Log}

class MySqlHelper extends Config with Log {
  val driver = getConfig("mysql.driver")
  val url = "jdbc:mysql://" +
    getConfig("command.mysql.ip") + ":"+
    getConfig("command.mysql.port")+ "/" +
    getConfig("command.mysql.database")
  val username = getConfig("command.mysql.username")
  val password = getConfig("command.mysql.password")

  //executeQuery("SELECT gpsno FROM skyeye_device WHERE id = '006E292725C01D3CEC279FB472D46F09'")
  def executeQueryOne(sql: String, columns: List[String]): Map[String, String] = {
    var result: Map[String, String] = Map[String, String]()
    val connection = MySqlPool.getJdbcConn()
    try {
      // create the statement, and run the select query
      val statement = connection.createStatement()
      info(sql, getClass.getName)
      val data = statement.executeQuery(sql)
      if(data.next()) {
        for (column <- columns) {
          result += (column -> data.getString(column))
        }
      }
    } catch {
      case e: Throwable => e.printStackTrace
    } finally {
      MySqlPool.releaseConn(connection)
    }
    result
  }

  def executeQueryList(sql: String, columns: List[String]): List[Map[String, String]] = {
    var result : List[Map[String, String]] = List()
    val connection = MySqlPool.getJdbcConn()
    try {
      // create the statement, and run the select query
      val statement = connection.createStatement()
      info(sql, getClass.getName)
      val data = statement.executeQuery(sql)
      while (data.next()) {
        var item: Map[String, String] = Map[String, String]()
        columns.map(x => {item += x -> data.getString(x)})
        result = result :+ item
      }
    } catch {
      case e: Throwable => e.printStackTrace
    } finally {
      MySqlPool.releaseConn(connection)
    }
    result
  }

  //executeQuery("Update Set skyeye_device gpsno = '' WHERE id = '006E292725C01D3CEC279FB472D46F09'")
  def executeOprator(sql: String): Int = {
    var result = 0
    val connection = MySqlPool.getJdbcConn()
    try {
      // create the statement, and run the select query
      val statement = connection.createStatement()
      info(sql, getClass.getName)
      result = statement.executeUpdate(sql)
    } catch {
      case e: Throwable => e.printStackTrace
    } finally {
      MySqlPool.releaseConn(connection)
    }
    result
  }

  def executeMultiOprator(sqls: List[String]): Unit = {
    var result = 0
    val connection = MySqlPool.getJdbcConn()
    try {
      // create the statement, and run the select query
      val statement = connection.createStatement()
      for(sql <- sqls) {
        info(sql, getClass.getName)
        result = statement.executeUpdate(sql)
      }
    } catch {
      case e: Throwable => e.printStackTrace
    } finally {
      MySqlPool.releaseConn(connection)
    }
    //result
  }
}
