// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._

val before = unix_timestamp()

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

// COMMAND ----------

var namesDf1 = namesDf.withColumn("unix_timestamp",(before - unix_timestamp()).as("unix_timestamp"))
namesDf1 = namesDf1.withColumn("height feet",col("height")*0.0328)
display(namesDf1)

// COMMAND ----------

import org.apache.spark.sql.expressions._
val namesDf3 = namesDf.withColumn("just_name", split(col("name")," ").getItem(0))
namesDf3.select("just_name").rdd.map(r => r(0)).collect().groupBy(identity).maxBy(_._2.size)._1

// COMMAND ----------


var Actors_age = namesDf.withColumn("death_date",to_date($"date_of_death", "dd.MM.yyyy")).withColumn("current_date",current_date()).withColumn("birth_date",to_date($"date_of_birth", "dd.MM.yyyy"))

Actors_age = Actors_age.withColumn("Years old", when($"death_date".isNull or $"death_date" === "", abs(datediff($"birth_date",$"current_date"))/365).otherwise(abs(datediff($"birth_date",$"death_date")/365)))
display(Actors_age)

// COMMAND ----------

Actors_age = Actors_age.drop("bio").drop("death_details")
display(Actors_age)

// COMMAND ----------

val newColumns = Actors_age.columns.map(_.replace('_',' ')).map(_.capitalize)

// COMMAND ----------

display(namesDf3.orderBy($"just_name"))

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
var moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(moviesDf)

// COMMAND ----------

moviesDf.select($"year",$"country").explain()

// COMMAND ----------

moviesDf.select($"year",$"country").groupBy($"country").count().explain()

// COMMAND ----------

moviesDf = moviesDf.withColumn("unix_timestamp",unix_timestamp().as("unix_timestamp")).withColumn("Current date",current_date()).withColumn("Published date",to_date($"year","yyyy")).withColumn("Time passed",abs(datediff($"Published date",$"Current date"))/365).withColumn("Budget numerical", regexp_extract($"budget", "\\d+", 0)).na.drop("any")
display(moviesDf)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
var ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(ratingsDf)

// COMMAND ----------

val votes = List("votes_1","votes_2","votes_3","votes_4","votes_5","votes_6","votes_7","votes_8","votes_9","votes_10")
for( i <- votes){
  var score = ratingsDf.stat.approxQuantile(i, Array(0.5), 0.25)
  var string = score.mkString(",")
  println(i + " " + string)
}

// COMMAND ----------

import org.apache.spark.sql.types.{ArrayType, DoubleType}
import org.apache.spark.sql.functions._

var dfWithMean = ratingsDf.withColumn("mean",(col("votes_1")+col("votes_2")*lit(2)+col("votes_3")*lit(3)+col("votes_4")*lit(4)+col("votes_5")*lit(5)+col("votes_6")*lit(6)+col("votes_7")*lit(7)+col("votes_8")*lit(8)+col("votes_9")*lit(9)+col("votes_10")*lit(10)).cast(DoubleType)/(col("total_votes")))
display(dfWithMean)

// COMMAND ----------

val new_ratings = ratingsDf.na.drop("any").withColumn("median_diff", $"median_vote" - $"weighted_average_vote").withColumn("mean_diff", $"mean_vote" - $"weighted_average_vote")
display(new_ratings)

// COMMAND ----------

import org.apache.spark.sql.functions._
val ratio = ratingsDf.select(avg($"males_allages_avg_vote").alias("males"),avg($"females_allages_avg_vote").alias("females"))
display(ratio)

// COMMAND ----------

var username = "sqladmin"
var password = "$3bFHs56&o123$" 

val dataFromSqlServer = sqlContext.read
      .format("jdbc")
      .option("driver" , "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable", "(SELECT table_name FROM information_schema.tables) tmp")
      .option("user", username)
      .option("password",password)
      .load()

display(dataFromSqlServer)
