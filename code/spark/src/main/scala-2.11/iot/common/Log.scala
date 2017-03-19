package iot.common

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
  * Created by lishulei on 11/22/16.
  */
trait Log {
 val logger: Logger = LogManager.getRootLogger

 def error [T] (message: String, className: String, params: Map[String, Any]) : Unit = {
  val logModel: LogModel[T] = new LogModel[T](message, className, params)
  logger.error(logModel.toString)
 }

 def error (message: String, className: String) : Unit = {
   val logModel: LogModel[Any] = new LogModel[Any](message, className, null)
   logger.error(logModel.toString)
 }

  def warn [T] (message: String, className: String, params: Map[String, Any]) : Unit = {
    val logModel: LogModel[T] = new LogModel[T](message, className, params)
    logger.warn(logModel.toString)
  }

  def warn (message: String, className: String) : Unit = {
    val logModel: LogModel[Any] = new LogModel[Any](message, className, null)
    logger.warn(logModel.toString)
  }

  def info[T] (message: String, className: String, params: Map[String, Any]) : Unit = {
    val logModel: LogModel[T] = new LogModel[T](message, className, params)
    logger.info(logModel.toString)
  }

  def info (message: String, className: String) : Unit = {
    val logModel: LogModel[Any] = new LogModel[Any](message, className, null)
    logger.info(logModel.toString)
  }

  def debug [T] (message: String, className: String, params: Map[String, Any]) : Unit = {
    val logModel: LogModel[T] = new LogModel[T](message, className, params)
    logger.debug(logModel.toString)
  }

  def debug (message: String, className: String) : Unit = {
    val logModel: LogModel[Any] = new LogModel[Any](message, className, null)
    logger.debug(logModel.toString)
  }
}
