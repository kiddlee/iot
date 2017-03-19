package iot.command

import java.sql.Timestamp
import java.text.SimpleDateFormat

import iot.common.Log

import com.alibaba.fastjson.JSON

/**
  * Created by kiddlee on 16-11-25.
  */
trait CommandBase extends Log {
  def getJsonObj (cmd: String) : Map[String, Any] = {
    JSON.parse(cmd).asInstanceOf[Map[String, Any]]
  }

  def getBody(jsonObj: Map[String, Any]) : Map[String, String] = {
    jsonObj.get("body").get.asInstanceOf[Map[String, String]]
  }
}
