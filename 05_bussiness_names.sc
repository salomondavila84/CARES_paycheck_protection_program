

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{count, lit, mean, sum, udf}

import java.io.File

// Turning Logging off
Logger.getLogger("org").setLevel(Level.OFF)

// Creating Spark session with Delta Lake package and configurations
val spark = SparkSession
  .builder
  .master("local[*]")
  .appName("ppp")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .getOrCreate()

// Dataframe Column Manipulations
import spark.implicits._
import org.apache.spark.sql.functions._

/*
ToDo: Importing Delta Lake table and select only loans with Business names (loans above $150k)
  counting most frequent terms
 */



// Constructing file location of data store of Delta Lake table
val file = new File("/tables/ppp_final.delta") // Defaults to C:\, otherwise include <drive>:/
val path = file.getAbsolutePath // getCanonicalPath

// Loading Delta Lake table in Parquet format with latest version
println("Reading the table")
val ppp_DF = spark.read.format("delta").option("versionAsOf", 18).load(path)

// Verification of dataframe
ppp_DF.show(5)

// Number of loans
ppp_DF.count()

val business_names = ppp_DF
  .select($"LoanRange", $"BusinessName", $"State", $"NAICSCode", $"JobsRetained", $"CD")
  .where($"BusinessName" =!= "")

// Number of loans with business names
business_names.count()

// Confirming loan ranges are above the $150k
val business_names_loan_ranges = business_names
  .select($"LoanRange", $"JobsRetained")
  .groupBy($"LoanRange")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained")
  )
  .orderBy($"LoanRange")

business_names_loan_ranges.show(false)

// Counting the words in business names
val business_name_arrays = business_names
  .select(split($"BusinessName", " ").as("words"))

val business_name_occurrences = business_name_arrays
  .select(explode($"words"))

// Grouping words, counting and sorting
val business_name_counts = business_name_occurrences
  .withColumn("word", trim($"col", ",.|"))
  .where($"word" =!= "" || $"word" =!= "##")
  .groupBy($"word")
  .count()
  .orderBy($"count".desc)

business_name_counts.show(100, false)

// Studying Eagle Rock Businesses
val ER_businesses = ppp_DF
  .select($"LoanRange", $"BusinessName", $"NAICSCode", $"JobsRetained", $"Zip")
  .where($"BusinessName" =!= "" && ($"Zip" === 90041 || $"Zip" === 90042))

ER_businesses.count()

val ER_loan_ranges = ER_businesses
  .select($"LoanRange", $"JobsRetained")
  .groupBy($"LoanRange")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained")
  )
  .orderBy($"LoanRange")

ER_loan_ranges.show()

val ER_business_names = ER_businesses
  .select($"LoanRange", $"BusinessName", $"JobsRetained")

ER_business_names.show(75, false)

// Studying Pasadena Businesses
val Pasadena_businesses = ppp_DF
  .select($"LoanRange", $"BusinessName", $"NAICSCode", $"JobsRetained", $"Zip")
  .where($"BusinessName" =!= "" && ($"Zip" === 91101 || $"Zip" === 91103 || $"Zip" === 91104 || $"Zip" === 91105 || $"Zip" === 91106 || $"Zip" === 91107 || $"Zip" === 91108))

Pasadena_businesses.count()

val Pasadena_loan_ranges = Pasadena_businesses
  .select($"LoanRange", $"JobsRetained")
  .groupBy($"LoanRange")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained")
  )
  .orderBy($"LoanRange")

Pasadena_loan_ranges.show()

val Pasadena_business_names = Pasadena_businesses
  .select($"LoanRange", $"BusinessName", $"JobsRetained")

Pasadena_business_names.show(200, false)

Pasadena_business_names
  .select("*")
  .where($"BusinessName".contains("WESTRIDGE"))
  .show(false)

// ToDo: Select NAICS Code and look at top words


ppp_DF.write.parquet("E:\\IntelliJ-IdeaProjects\\CARES_paycheck_protection_program\\src\\main\\scala\\ppp_final.parquet")
spark.stop()