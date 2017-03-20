package iot.command

import com.alibaba.fastjson.{JSON, JSONObject}
import iot.repository.DeviceRepository

/**
  * Created by kiddlee on 17-3-19.
  */
object Status {
  def process(cmds: Iterable[JSONObject]): Unit = {
    val infos = cmds.map(x=> {
      val deviceno = x.get("deviceno").toString
      val body = JSON.parse(x.get("body").toString).asInstanceOf[JSONObject]
      val temp = body.get("temp").toString
      val humidity = body.get("humidity").toString
      val pm25 = body.get("pm25").toString
      val mode = body.get("mode").toString
      (deviceno, temp, humidity, pm25, mode)
    })

    // TODO: Add device status
    if (infos.size > 0) {
      infos.map(x => {
        DeviceRepository.putStatus(x._1, x._2, x._3, x._4, x._5)
      })
    }
  }
}
