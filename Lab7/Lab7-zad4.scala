import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test extends App {
val spark = SparkSession.builder
.master("local[4]")
.appName("Moja-applikacja")
.getOrCreate()

  val filePath = "D:/studia/6 SEM/BIG DATA/lab7/dane.csv"
  var TitanicDf = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(filePath)
  TitanicDf = TitanicDf.drop("body")
  println("avg: "+
    TitanicDf.select(avg("age")).collect()(0)(0) + " \n")
  TitanicDf.groupBy("sex").avg("age")
}