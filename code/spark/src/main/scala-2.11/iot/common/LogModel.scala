package iot.common

import java.util.{Calendar, Date}
import com.google.gson.Gson

/**
  * Created by lishulei on 11/22/16.
  */
class LogModel[T] (val message: String, val className: String, val params: Map[String, Any]) {
  val time: Date = Calendar.getInstance.getTime

  override def toString: String = {
    var paramString = ""
    var result = new Gson().toJson(this)
    if(params != null) {
      params.map(x => paramString += "\"" + x._1 + "\":\"" + x._2 + "\",")
      result = result.substring(0, result.indexOf("params") + 9) +
        paramString.substring(0, paramString.length - 1) +
        result.substring(result.indexOf("params") + 9)
    }
    result
  }
}
