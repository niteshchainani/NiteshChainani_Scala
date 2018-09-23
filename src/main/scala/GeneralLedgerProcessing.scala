import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.util.control.NonFatal
import org.apache.spark.sql.functions.{col, _}

object GeneralLedgerProcessing {

  def main(Args:Array[String]): Unit ={
try{
    System.setProperty("hadoop.home.dir","C:/UBSAssignment/")
    val conf=new SparkConf().setMaster("local")

    val spark=SparkSession.builder().appName("GeneralLedgerProcessing").config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputTransactions=spark.read.option("multiLine","True").json("./UBS/1537277231233_Input_Transactions.json").toDF()
    val inputTransactionsByType = inputTransactions.groupBy(col("Instrument"), col("TransactionType")).agg(sum("TransactionQuantity") as "totalQuantity")

  /*  val inputTransactionsRDD = inputTransactions.rdd.map(x=>(x(0).toString,(x(1).asInstanceOf[Long],x(2).asInstanceOf[Long],x(3).toString))).groupByKey()
    val dayPositions=spark.sparkContext.textFile("./UBS/Input_StartOfDay_Positions.txt")
    val dayPositionsMappedRDD=dayPositions.mapPartitionsWithIndex((index,itr)=> if(index==0) itr.drop(1) else itr)
      .map(
        rec => {var temp=rec.split(",")
          (temp(0),(temp(1).toInt,temp(2),temp(3).toLong))
               }
          )
    val joinedResult =dayPositionsMappedRDD.join(inputTransactionsRDD)

    joinedResult.map(rec=>(rec._1,rec._2._1._1,rec._2._1._2,computeNetQuantity(rec._2._2,rec._2._1._3,rec._2._1._2))).collect().foreach(println)
    */
    val dayPositions = spark.read.format("csv").option("header", "true").load("./UBS/Input_StartOfDay_Positions.txt")

    val calculatedDF = dayPositions.join(inputTransactionsByType, dayPositions("Instrument") === inputTransactionsByType("Instrument"), "leftouter").drop(inputTransactionsByType("Instrument")).withColumn("derivedQuantity", when((col("AccountType") === "I" && col("TransactionType") === "S") || (col("AccountType") === "E" && col("TransactionType") === "B"), col("totalQuantity") * 1).otherwise(col("totalQuantity") * -1)).na.fill(0.0).withColumn("finalQuantity", col("derivedQuantity") + col("Quantity")).groupBy(col("Instrument"), col("Account"), col("AccountType")).agg(sum("finalQuantity")-sum("Quantity") as "Delta", sum("finalQuantity") as "Quantity")
    //calculatedDF.show()
    calculatedDF.coalesce(1).write.csv("./UBS/END_OF_DAY_POSITION.csv")
    calculatedDF.agg(max("Delta"),min("Delta"))
}
catch {
  case NonFatal(ex) => //Log the error ex
  case ex: InterruptedException => // handle InterruptedException
}
  }

/*  def computeNetQuantity(iterable: Iterable[(Long,Long,String)],quantity: Long,accountType: String) : Long= {
    var finalQuantity = quantity
    if (accountType == "E")
   {   iterable.foreach(
        x => {
         if (x._3 == "B")
         {finalQuantity = finalQuantity + x._2}
         else
         {finalQuantity = finalQuantity - x._2}
        }
        )
  }
    else if (accountType == "I")
    { iterable.foreach(
          x=>{
              if (x._3 == "B")
              {finalQuantity = finalQuantity - x._2}
              else
              {finalQuantity = finalQuantity + x._2}
            }
            )
    }
    finalQuantity
}*/
}