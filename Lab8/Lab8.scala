// Databricks notebook source
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.scalatest.Assertions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions.lower

// COMMAND ----------

def loadDataFrame(NamesDF: String): DataFrame = {
  assert(NamesDF.endsWith(".csv"))
  
  val schema = StructType(
  List(
      StructField("imdb_name_id", StringType, true),
      StructField("name", StringType, false),
      StructField("birth_name", StringType, false),
      StructField("height", IntegerType, false),
      StructField("bio", StringType, false),
    StructField("birth_details", StringType, true),
      StructField("date_of_birth", StringType, false),
      StructField("place_of_birth", StringType, false),
      StructField("death_details", StringType, false),
      StructField("date_of_death", StringType, false),
    StructField("place_of_death", StringType, true),
      StructField("reason_of_death", StringType, false),
      StructField("spouses_string", StringType, false),
      StructField("spouses", IntegerType, false),
      StructField("divorces", IntegerType, false),
    StructField("spouses_with_children", IntegerType, false),
      StructField("children", IntegerType, false)
    )
  )

  
  val df = spark.read
  .option("header", "true")
  .option("sep", ",")
  .schema(schema)
  .csv(NamesDF)
  return df
}


// COMMAND ----------

val filepath = "dbfs:/FileStore/tables/Files/names.csv"
var NamesDF = loadDataFrame(filepath)
display(NamesDF)

// COMMAND ----------

def AvgHeight(colName : String, groupcol : String, df :DataFrame) : DataFrame ={
  if(df.columns.contains(colName) & df.columns.contains(groupcol)){
      val DFAVG =df.groupBy(groupcol).avg(colName)
      return DFAVG
  }
  else
  {
      Logger.getLogger("No such column").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }
}

display(AvgHeight("height","spouses",NamesDF))

// COMMAND ----------

def replaceNone(column: String, df :DataFrame): DataFrame = {
   if(df.columns.contains(column)){
     //df.withColumn(column,regexp_replace(col(column), null, "0"))
     return df.na.fill(0,Array(column))
   }
  else
  {
      Logger.getLogger("No such column").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }
  
}
display(replaceNone("height",NamesDF))

// COMMAND ----------

def LowerCase(column: String, df :DataFrame): DataFrame = {
   if(df.columns.contains(column)){
     return df.withColumn(column, lower(col(column)));
   }
  else
  {
      Logger.getLogger("No such column").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }
  
}
display(LowerCase("birth_name",NamesDF))

// COMMAND ----------

def DescribeColumn(column: String, df :DataFrame): DataFrame = {
   if(df.columns.contains(column)){
     return df.describe(column)
   }
  else
  {
      Logger.getLogger("No such column").setLevel(Level.ERROR)
      return spark.emptyDataFrame
  }
  
}
display(DescribeColumn("height",NamesDF))

// COMMAND ----------

def CountNulls(column: String, df :DataFrame): Long = {
   if(df.columns.contains(column)){
     return df.filter(col(column).isNull).count()
   }
  else
  {
      Logger.getLogger("No such column").setLevel(Level.ERROR)
      return 0
  }
  
}
CountNulls("height",NamesDF)
