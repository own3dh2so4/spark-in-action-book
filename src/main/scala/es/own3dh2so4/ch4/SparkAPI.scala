package es.own3dh2so4.ch4

import es.own3dh2so4.Properties
import org.apache.spark.sql.SparkSession

/**
  * Let’s say your marketing department wants to give complimentary products to customers according to some rules.
  * They want you to write a program that will go through yesterday’s transactions and add the complimentary ones.
  * The rules for adding complimentary products are these:
  **
  *Send a bear doll to the customer who made the most transactions
  *Give a 5% discount for two or more Barbie Shopping Mall Playsets bought
  *Add a toothbrush for more than five dictionaries bought
  *Send a pair of pajamas to the customer who spent the most money overall
  **
  *The complimentary products should be represented as additional transactions with price 0.00.
  *Marketing would also like to know which transactions were made by the customers who are getting the complimentary products.
  */
object SparkAPI extends App{

  val prop = Properties()

  //SparkConfig
  val sparkMaster = prop("spark.session.master").getOrElse("local[*]")
  val sparkAppName = prop("spark.session.appName").getOrElse("Spark App")

  //Folder paths
  val inputFiles = prop("input.folder").getOrElse("") + "products/"
  val outputFiles = prop("output.folder").getOrElse("") + "products/"

  val spark = SparkSession.builder.
    master(sparkMaster).appName(sparkAppName).getOrCreate()
  val sc = spark.sparkContext

  println(s"Spark version ${spark.version}")

  val startTime = System.nanoTime()

  val transactionRaw = sc.textFile(inputFiles + "ch04_data_transactions.txt")

  println("Transactions")
  transactionRaw.take(5).foreach(println)

  val productsRaw = sc.textFile(inputFiles + "ch04_data_products.txt")
  println("Products")
  productsRaw.take(5).foreach(println)

  val transactionClient = transactionRaw.map(_.split("#")).map( t => (t(2).toInt, t))
  val products = productsRaw.map(_.split("#")).map( p => (p(0).toInt,p))

  val maxTransactionsClient = transactionClient.countByKey().toSeq.maxBy(_._2)
  println("The most transactions")
  println(maxTransactionsClient)

  val transactionsC1 = transactionClient.lookup(maxTransactionsClient._1)
  println("Transactiosn C1")
  transactionsC1.foreach(t => println(t.mkString(", ")))
  var complTrans = Array(Array("2015-03-30", "11:59 PM", maxTransactionsClient._1.toString, "4", "1", "0.00"))

  println("Give a 5% discount for two or more Barbie Shopping Mall Playsets bought")
  println("Find barbie")
  val barbieSMP =  products.filter(_._2(1) == "Barbie Shopping Mall Playset").collect()
  barbieSMP.foreach(b => println(b._2.mkString(", ")))
  val barbieID = barbieSMP(0)._1
  val broadcastBarbieID = sc.broadcast(barbieID)

  val transactionClientBarbieDiscount = transactionClient.mapValues( t =>{
                    if (t(3).toInt == broadcastBarbieID.value && t(4).toDouble > 1) {
                      t(5) = (t(5).toDouble * 0.95).toString
                    }
                      t})

  println("Add a toothbrush for more than five dictionaries bought")
  val toothbrush = products.filter(_._2(1) == "Toothbrush").collect()
  val broadcastToothbrush = sc.broadcast(toothbrush(0)._1)
  val diccionary = products.filter(_._2(1) == "Dictionary").collect()
  val broadcastDiccionaryID = sc.broadcast(diccionary(0)._1)
  val transactionClientWithToothbrush = transactionClientBarbieDiscount.flatMapValues( t => {
              if ( t(3).toInt == broadcastDiccionaryID.value  && t(4).toDouble > 4){
                val gift = t.clone()
                gift(3)=broadcastToothbrush.value.toString
                gift(5)="0.00"
                gift(4)="1"
                List(t,gift)
              } else {
                List(t)
              }
  })

  println("Send a pair of pajamas to the customer who spent the most money overall")
  val amounts = transactionClientWithToothbrush.mapValues(_(5).toDouble)
  val clientSpent = amounts.foldByKey(0)(_ + _).collect().toSeq.maxBy(_._2)
  println(clientSpent)

  complTrans = complTrans :+ Array("2015-03-30", "11:59 PM", clientSpent._1.toString , "63", "1", "0.00")

  val finalTransf = transactionClientWithToothbrush.union(sc.parallelize(complTrans).map(t =>(t(2).toInt, t)))

  finalTransf.map(t => t._2.mkString("#")).saveAsTextFile(outputFiles)

  val elapsedTime = (System.nanoTime() - startTime) /1e9
  println("Elapsed time: %.2fseconds".format(elapsedTime))
}
