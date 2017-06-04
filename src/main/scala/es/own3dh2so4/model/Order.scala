package es.own3dh2so4.model

/**
  * Created by david on 27/05/17.
  */
case class Order (time: java.sql.Timestamp, orderId: Long, clientId: Long, symbol: String, amount: Int, price: Double, buy: Boolean){

}
