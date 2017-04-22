package es.own3dh2so4

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

import scala.io.Source

/**
  * Created by david on 1/04/17.
  */
object Properties {
  def apply(): Properties = new Properties("/conf.properties")
  def apply(file : String): Properties = new Properties(file)
}

class Properties (file: String) {

  val properties = new java.util.Properties()
  properties.load(new ByteArrayInputStream(Source.fromInputStream( getClass.getResourceAsStream(file) ).
    getLines.mkString("\n").getBytes(StandardCharsets.UTF_8)))



  def apply(key: String): Option[String] = {
    Option( properties.getProperty(key))
  }
}
