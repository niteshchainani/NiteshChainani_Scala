import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.control.NonFatal
import org.apache.spark.sql.functions.{col, _}

object GeneralLedgerProcessing {

  def main(Args:Array[String]): Unit ={
try{
    System.setProperty("hadoop.home.dir","C:/UBSAssignment/")
    val conf=new SparkConf().setMaster("local")

    val spark=SparkSession.builder().appName("GeneralLedgerProcessing").config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputTransactions=spark.read.option("multiLine","True").json("./UBS/1537277231233_Input_Transactions.json")
    val inputTransactionsByType = inputTransactions.groupBy(col("Instrument"), col("TransactionType")).agg(sum("TransactionQuantity") as "totalQuantity")
    val externalIinputTransactionsByType = inputTransactionsByType.withColumn("tempQuantity", when(col("TransactionType") === "B", col("totalQuantity")).otherwise(col("totalQuantity") * -1)).withColumn("AccountType1", lit("E"))
    val internalIinputTransactionsByType = inputTransactionsByType.withColumn("tempQuantity", when(col("TransactionType") === "B", col("totalQuantity") * -1).otherwise(col("totalQuantity") )).withColumn("AccountType1", lit("I"))
    val finalTransactionType = externalIinputTransactionsByType.union(internalIinputTransactionsByType).groupBy(col("Instrument"), col("AccountType1")).agg(sum("tempQuantity") as "derivedquantity")

    val dayPositions = spark.read.format("csv").option("header", "true").load("./UBS/Input_StartOfDay_Positions.txt")

    val calculatedDF = dayPositions.join(finalTransactionType, dayPositions("Instrument") === finalTransactionType("Instrument") && dayPositions("AccountType") === finalTransactionType("AccountType1"), "leftouter").drop(finalTransactionType("Instrument")).drop(finalTransactionType("AccountType1")).na.fill(0.0).withColumn("finalQuantity", col("derivedquantity") + col("Quantity")).withColumn("Delta" , col("finalQuantity") - col("Quantity")).select(col("Instrument"), col("Account"), col("AccountType"), col("finalQuantity") as "Quantity", col("Delta"))

    calculatedDF.collect().foreach(println)
    println("Instruments with largest and lowest net transaction volumes for the day are as below:")
    calculatedDF.orderBy(col("Delta").desc).take(1).foreach(println)
    calculatedDF.orderBy(col("Delta")).take(1).foreach(println)
    calculatedDF.coalesce(1).write.option("header", "true").csv("./UBS/END_OF_DAY_POSITION")
}
catch {
  case NonFatal(ex) => //Log the error ex
  case ex: InterruptedException => // handle InterruptedException
      }
}}