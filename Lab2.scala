// Databricks notebook source
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}

val schemat = StructType(Array(
  StructField("imdb_title_id",StringType,true),
  StructField("ordering",StringType,true),
  StructField("imdb_name_id",StringType,true),
  StructField("category",StringType,true),
  StructField("job",StringType,true),
  StructField("characters",StringType,true)
))

val filePath = "dbfs:/FileStore/tables/Files/actors.csv/"
val actorsDf = spark.read.format("csv")
            .option("header","true")
            .schema(schemat)
            .load(filePath)



// COMMAND ----------

display(actorsDf.head(3))

temp.write.mode("overwrite").json("dbfs:/FileStore/tables/Files/actors.json")

// COMMAND ----------

val filewithschema1 = spark.read.format("json").option("schema",schemat)
.option("mode","PERMISSIVE")
.load("dbfs:/FileStore/tables/Files/actors.json")

val filewithschema2 = spark.read.format("json").option("schema",schemat)
.option("mode","DROPMALFORMED")
.load("dbfs:/FileStore/tables/Files/actors.json")

val filewithschema3 = spark.read.format("json").option("schema",schemat)
.option("mode","FAILFAST")
.load("dbfs:/FileStore/tables/Files/actors.json")

val filewithschema4 = spark.read.format("json").option("schema",schemat)
.option("badRecordsPath", "dbfs:/FileStore/tables/Files/badrecords")
.load("dbfs:/FileStore/tables/Files/actors.json")


// COMMAND ----------

val output = "dbfs:/FileStore/tables/Files/actors.parquet"
actorsDf.write.json(path=output)
spark.read.schema(schemat).option("enforceSchema",true).parquet(path=output)

DataFrameReader.format("json").option(" key", "value").schema(schemat).load()


// COMMAND ----------


