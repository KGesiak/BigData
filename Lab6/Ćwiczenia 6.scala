// Databricks notebook source
val TDF = Seq(( 1, "2011-01-01", 500),
( 1, "2011-01-15", 50),
( 1, "2011-01-22", 250),
( 1, "2011-01-24", 75),
( 1, "2011-01-26", 125),
( 1, "2011-01-28", 175),
( 2, "2011-01-01", 500),
( 2, "2011-01-15", 50),
( 2, "2011-01-22", 25),
( 2, "2011-01-23", 125),
( 2, "2011-01-26", 200),
( 2, "2011-01-29", 250),
( 3, "2011-01-01", 500),
( 3, "2011-01-15", 50 ),
( 3, "2011-01-22", 5000),
( 3, "2011-01-25", 550),
( 3, "2011-01-27", 95 ),
( 3, "2011-01-30", 2500)).toDF("AccountId", "TranDate", "TranAmt")

val CDF = Seq((1,"George", 800),
(2,"Sam", 950),
(3,"Diane", 1100),
(4,"Nicholas", 1250),
(5,"Samuel", 1250),
(6,"Patricia", 1300),
(7,"Brian", 1500),
(8,"Thomas", 1600),
(9,"Fran", 2450),
(10,"Debbie", 2850),
(11,"Mark", 2975),
(12,"James", 3000),
(13,"Cynthia", 3000),
(14,"Christopher", 5000)).toDF("RowID" ,"FName" , "Salary" )

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val WindowFun  = Window.partitionBy(col("AccountId")).orderBy("TranDate")
val TotalRun=sum("TranAmt").over(WindowFun)
TDF.select(col("AccountId"), col("TranDate"), col("TranAmt") ,TotalRun.alias("RunTotalAmt") ).orderBy("AccountId").show()

// COMMAND ----------

val TDFWindows = TDF.withColumn("RunTotalAmt",TotalRun)
.withColumn("RunAvg",avg("TranAmt").over(WindowFun))
.withColumn("RunSmallAmt",min("TranAmt").over(WindowFun))
.withColumn("RunLargeAmt",max("TranAmt").over(WindowFun))
.withColumn("RunTranQty",count("*").over(WindowFun))
display(TDFWindows)

// COMMAND ----------

val WindowRange  = WindowFun.rowsBetween(-2, Window.currentRow) 
val TDFWindowsRange = TDF.withColumn("SlideTotal",TotalRun)
.withColumn("SlideAvg",avg("TranAmt").over(WindowFun))
.withColumn("SlideMin",min("TranAmt").over(WindowFun))
.withColumn("SlideMax",max("TranAmt").over(WindowFun))
.withColumn("SlideQty",count("*").over(WindowFun))
.withColumn("RN",row_number().over(WindowFun))
display(TDFWindows)

// COMMAND ----------


val WindowRows  = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow) 
val SpecRange  = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow) 
val CDFWindows = CDF.withColumn("SumByRows",sum(col("Salary")).over(WindowRows)).withColumn("SumByRange",sum(col("Salary")).over(SpecRange))
display(CDFWindows)

// COMMAND ----------

val table = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderheader")

val WindowFun  = Window.partitionBy("AccountNumber").orderBy("OrderDate")
val TableWindow =table.select(col("AccountNumber"), col("OrderDate"), col("TotalDue") ,row_number().over(WindowFun).alias("RN") ).orderBy("AccountNumber").limit(10)
display(TableWindow)

// COMMAND ----------

display(table)

// COMMAND ----------

val WindowFun =Window.partitionBy(col("Status")).orderBy("OrderDate")
val WindowRows = WindowFun.rowsBetween(Window.unboundedPreceding, -2) 

val TableRows = table.withColumn("lead",lead(col("SubTotal"), 2).over(WindowFun))
.withColumn("lag",lag(col("SubTotal"),1).over(WindowFun))
.withColumn("last",last(col("SubTotal")).over(WindowRows))
.withColumn("first",first(col("SubTotal")).over(WindowRows))
.withColumn("RN",row_number().over(WindowFun))
.withColumn("DR",dense_rank().over(WindowFun))


display(TableRows)

// COMMAND ----------

val TableDetails = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderdetail")


val LeftJoin = table.join(TableDetails, table.col("SalesOrderID") === TableDetails.col("SalesOrderID"), "leftsemi")
LeftJoin.show()

// COMMAND ----------

LeftJoin.explain()

// COMMAND ----------

val AntiLeftJoin = table.join(TableDetails, table.col("SalesOrderID") === TableDetails.col("SalesOrderID"), "leftanti")
AntiLeftJoin.show()

// COMMAND ----------

AntiLeftJoin.explain()

// COMMAND ----------

display(LeftJoin)

// COMMAND ----------

display(LeftJoin.distinct())

// COMMAND ----------

display(LeftJoin.dropDuplicates())

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast
val BroadcastJoin = table.join(broadcast(TableDetails), table.col("SalesOrderID") === TableDetails.col("SalesOrderID"))
BroadcastJoin.show()

// COMMAND ----------

BroadcastJoin.explain()

// COMMAND ----------


