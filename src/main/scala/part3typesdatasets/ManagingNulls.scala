package part3typesdatasets

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  // select the first non-null value with coalesce.
  // Seleccionará el primer valor no nulo de las columnas.
  // Si Rotten_Tomatoes_Rating no es nulo, mostrará Rotten_Tomatoes_Rating.
  // Si Rotten_Tomatoes_Rating es nulo, mostrará IMDB_Rating *10
  // Si ambos son nulos, mostrará null.
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).as("Actual_Rating")
  )
    .write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/moviesDF_WithNulls.parquet")

  // checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)
  moviesDF.show()
  // removing nulls∂
  moviesDF.select("Title", "IMDB_Rating").na.drop().show() // remove rows containing nulls

  // replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
  ).show()
}
