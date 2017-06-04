package es.own3dh2so4.ch6

import java.sql.Timestamp
import java.text.SimpleDateFormat

import es.own3dh2so4.Properties
import es.own3dh2so4.model.Order
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, PairDStreamFunctions}



/**
  * Created by david on 27/05/17.
  */
object SparkStreaming extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "orders-streaming/"
  val outputFiles = prop("output.folder").getOrElse("") + "orders-streaming/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()
  val sc = spark.sparkContext

  val ssc = new StreamingContext(sc, Seconds(5))

  val filestream = ssc.textFileStream(inputFiles)

  val orders = filestream.flatMap( line => {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val s = line.split(",")
    try {
      assert(s(6) == "B" || s(6) == "S")
      List(Order(new Timestamp(dateFormat.parse(s(0)).getTime), s(1).toLong,s(2).toLong, s(3), s(4).toInt, s(5).toDouble,
      s(6) == "B"))
    } catch {
      case _ : Throwable => println("Wrong line format")
        List()
    }
  })

  println("Contar el numero de orders de cada tipo")
  val numPerType = orders.map( o => (o.buy, 1L)).reduceByKey(_+_)

  numPerType.repartition(1).saveAsTextFiles(outputFiles,"txt")

  val amountPerClient= orders.map(o => (o.clientId, o.amount * o.price))


  val amountState = amountPerClient.updateStateByKey((vals, totalOpt: Option[Double]) => {
    totalOpt match {
      case Some(total) => Some(vals.sum + total)
      case None => Some(vals.sum)
    }
  })
  ssc.start()
  ssc.awaitTermination()


}
