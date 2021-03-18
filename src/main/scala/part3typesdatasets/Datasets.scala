package part3typesdatasets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import java.sql.Date


object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    // ENABLING ADAPTIVE QUERY EXECUTION planner.
    .config("spark.sql.adaptive.enabled","true")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // convert a DF to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  // dataset of a complex type
  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String
                )

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))
  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  // cars.json have a weird dateFormat...
  val carsDF = spark.read
    .option("dateFormat", "yyyy-MM-dd")
    .schema(carsSchema)
    .json(s"src/main/resources/data/cars.json")

  // 3 - define an encoder (importing the implicits)
  // Aquí están tooooodos los encoders, por lo que de esta manera podemos hacer carsDF.as[Car]
  // Es como tener Encoders.product[Car]
  import spark.implicits._
  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  // map, flatMap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  /**
    * Exercises
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have (HP > 140)
    * 3. Average HP for the entire dataset
    */

  // 1
  val carsCount = carsDS.count
  println(carsCount)

  // 2
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  // 3
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)

  // also use the DF functions!
  carsDS.select(avg(col("Horsepower")))


  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")

  /**
    * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
    * (hint: use array_contains)
    */

  guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

  // Grouping DS

  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations

}
