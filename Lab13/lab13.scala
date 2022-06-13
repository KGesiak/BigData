// Databricks notebook source
// Zadanie 1
import org.apache.log4j.{LogManager,Level, Logger}
val log= LogManager.getRootLogger  
log.setLevel(Level.WARN) 
log.warn("Log działa")  

// COMMAND ----------

//Zadanie 3

//Przy tworzeniu clustera ustawiamy wartosci parametrow w sekcji spark
//spark.driver.memory 128m
//spark.executor.memory 128m

// COMMAND ----------

import org.apache.spark.sql.functions._
def base = spark.range(1, 500000, 1, 16).select($"id" as "key", rand(12) as "value")
base.write.format("parquet").bucketBy(16, "key").sortBy("value").saveAsTable("bucketed")

// COMMAND ----------

val bucketed = spark.table("bucketed")
val numPartitions = bucketed.queryExecution.toRdd.getNumPartitions

// COMMAND ----------

val t1 = spark
  .range(4)
  .repartition(4, $"id")  

t1.rdd.getNumPartitions

// COMMAND ----------

//zadanie 5

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
var namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

namesDf.select($"name",$"height",$"birth_details",$"date_of_birth").summary().show()
