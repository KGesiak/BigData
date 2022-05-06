// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

val catalog = spark.catalog
spark.sql("create database DB")
display(spark.catalog.listDatabases().select("name"))

// COMMAND ----------

val Movies = Seq(("Army of Thieves", "1999"), ("Choose or Die", "2014"), ("The Weekend Away", "2000"), ("The Call","1998"))
val Actors = Seq(("Chris Evans","44"), ("Robert Downey, Jr.","60"), ("Jennifer Lawrence","39"))

val par = spark.sparkContext.parallelize(Movies)
val par2 = spark.sparkContext.parallelize(Actors)
                
val MoviesDF = par.toDF("title","year")
val ActorsDF = par2.toDF("name","age")
                
MoviesDF.write.mode("overwrite").saveAsTable("DB.MoviesDF")
ActorsDF.write.mode("overwrite").saveAsTable("DB.ActorsDF")


// COMMAND ----------

catalog.listTables("DB").show()

// COMMAND ----------

val tabs = catalog.listTables().select("name").as[String].collect.toList

// COMMAND ----------

for( i <- tabs){
    spark.sql(s"DELETE FROM newDB.$i")
  }
