// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

val SalesLT = tabela.where("TABLE_SCHEMA == 'SalesLT'")

val Tables=SalesLT.select("TABLE_NAME").as[String].collect.toList

for( i <- Tables){
  val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$i")
  .load()

  table.write.format("delta").mode("overwrite").saveAsTable(i)
}

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col,when, count}

def countCols(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

for( i <- Tables.map(x => x.toLowerCase())){
  val filePath = s"dbfs:/user/hive/warehouse/$i"
  val df = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
print(i)
df.select(countCols(df.columns):_*).show()
  
}

// COMMAND ----------

for( i <- Tables.map(x => x.toLowerCase())){
  val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  val tabNew= tab.na.fill("0", tab.columns)
    display(tabNew)
}

// COMMAND ----------

for( i <- Tables.map(x => x.toLowerCase())){
  val tab = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/$i")
  tab.na.drop("any").show(false)
}

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._

val tab = spark.read.format("delta")
            .option("header","true")
            .option("inferSchema","true")
            .load(s"dbfs:/user/hive/warehouse/salesorderheader")

  println("mean: "+
    tab.select(mean("Freight")).collect()(0)(0))

  println("stddev: "+
    tab.select(stddev("TaxAmt")).collect()(0)(0))

  println("variance: "+
    tab.select(variance("TaxAmt")).collect()(0)(0))

// COMMAND ----------

val tab = spark.read.format("delta")
            .option("header","true")
            .option("inferSchema","true")
            .load(s"dbfs:/user/hive/warehouse/product")

tab.groupBy("ProductModelId")
    .agg(
      sum("ProductModelId").as("sum_salary"),
      avg("ProductModelId").as("avg_salary"),
      sum("ProductModelId").as("sum_bonus"),
      max("ProductModelId").as("max_bonus"))
    .show(false)

// COMMAND ----------

tab.groupBy("Color").agg(Map("StandardCost" -> "avg"))
tab.groupBy("Size").agg(Map("Weight" -> "variance"))
tab.groupBy("SellStartDate").agg(Map("StandardCost" -> "avg"))
tab.groupBy("ProductCategoryID").agg(Map("StandardCost" -> "avg"))
tab.groupBy("SellStartDate").agg(Map("ModifiedDate" -> "min")).show()

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

val convertCase =  udf((strQuote:String) => {
    val arr = strQuote.split("_")
    arr.map(f=>  f.substring(0,1).toUpperCase + f.substring(1,f.length)).mkString("_")
})

val PowerOf2 = udf((x: Int) => x*x)

val Square =  udf((x: Double) => scala.math.pow(x,0.5))



// COMMAND ----------

tab.select( convertCase(col("ThumbnailPhotoFileName")).as("ThumbnailPhotoFileName") )
tab.select( PowerOf2(col("ProductCategoryID")).as("ProductCategoryID") )
tab.select( Square(col("ListPrice")).as("ListPrice") )

// COMMAND ----------

import org.apache.spark.sql.functions._
import spark.implicits._
val DF= spark.read.json(spark.createDataset(json :: Nil))

// COMMAND ----------

val multiline_df = spark.read.option("multiline", "true")
      .json("/FileStore/tables/brzydki.json")
    multiline_df.printSchema()
    multiline_df.show(false)

// COMMAND ----------

var df = spark.read.format().option(multiline_df)
selectExpr("","","","",explode())

df2 = df.selectExt("*","feature")

// COMMAND ----------

var jsonBrzydki = spark.read.option("multiline", "true")
  .json("/FileStore/tables/brzydki.json")


val Json3 = jsonBrzydki.selectExpr("features")

// COMMAND ----------

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.col

def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
  schema.fields.flatMap(f => {
    val colName = if (prefix == null) f.name else (prefix + "." + f.name)

    f.dataType match {
      case st: StructType => flattenSchema(st, colName)
      case _ => Array(col(colName))
    }
  })
}

val new_df = multiline_df.select(flattenSchema(multiline_df.schema):_*)
display(new_df)

// COMMAND ----------


