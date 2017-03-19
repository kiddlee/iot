package iot.common

import java.io.FileNotFoundException

/**
  * Created by lishulei on 11/15/16.
  */
trait Config {
  def getConfig(propertyName: String): String={
    val lines = readResourceFile("/config.properties")
    lines.map(x => println(x))
    lines.filter(x => x.contains(propertyName + "=")).head.split("=").apply(1)
  }

  def readResourceFile(p: String): List[String] =
    Option(getClass.getResourceAsStream(p)).map(scala.io.Source.fromInputStream)
      .map(_.getLines.toList)
      .getOrElse(throw new FileNotFoundException(p))
}
