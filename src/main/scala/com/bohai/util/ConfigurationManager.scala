package com.bohai.util

import java.util.Properties

/**
  * Created by liukai on 2020/10/13.
  */
object ConfigurationManager {

  val properties = new Properties()
  val in = ConfigurationManager.getClass.getResourceAsStream("/app.properties")
  properties.load(in)

  def getProperty(key: String): String = {
    properties.getProperty(key)
  }

  @throws(classOf[Exception])
  def getInt(key: String): Int = properties.getProperty(key).toInt

  def main(args: Array[String]): Unit = {
    val property = ConfigurationManager.properties.getProperty("database.driver")
    println(property)
  }

}
