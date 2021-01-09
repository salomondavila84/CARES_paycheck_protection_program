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

/*
ToDo: Importing two Delta Lake tables for SBA profiles and PPP Loans for Congressional Districts and state comparisons.
 */

// Defining the location of the Delta Lake tables and importing Delta (Parquet) tables
val file = new File("/tables/ppp_final.delta") // Defaults to C:\, otherwise include <drive>:/
val path = file.getAbsolutePath // getCanonicalPath

// Loading Delta Lake table in Parquet format with latest version
println("Reading the table")
val ppp_DF = spark.read.format("delta").option("versionAsOf", 18).load(path)

// Verification of dataframe
ppp_DF.show(5)


// ToDo: Selecting loans for NAICS codes begining with 61XXX, Educational Services
// https://www.bls.gov/oes/current/oessrci.htm#61
val edNAICSCode = ppp_DF
  .select($"LoanRange", $"LoanAmount", $"BusinessName", $"State", $"NAICSCode", $"JobsRetained", $"CD")
  .where($"NAICSCode".like("61%"))

// Educational Services Loans Across Loan Ranges
edNAICSCode
  .groupBy($"LoanRange")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanRange")
  .show(false)

/*
+--------------------+----------+------------+-------------------+
|LoanRange           |LoanCounts|JobsRetained|AverageJobsRetained|
+--------------------+----------+------------+-------------------+
|a $5-10 million     |95        |32596       |343.11578947368423 |
|b $2-5 million      |958       |231697      |241.85490605427975 |
|c $1-2 million      |1643      |214784      |130.72671941570297 |
|d $350,000-1 million|4648      |278386      |59.89371772805508  |
|e $150,000-350,000  |6514      |182656      |28.04052809333743  |
|f $0-149,999        |67529     |390908      |5.788742614284233  |
+--------------------+----------+------------+-------------------+
 */

// Which educational NAICS groups saved the most jobs?
edNAICSCode
  .groupBy($"NAICSCode")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"JobsRetained".desc)
  .show(3,false)

/*
+---------+----------+------------+-------------------+
|NAICSCode|LoanCounts|JobsRetained|AverageJobsRetained|
+---------+----------+------------+-------------------+
|611110   |15087     |581603      |38.549943660104724 | Elementary and Secondary Schools
|611310   |2031      |175229      |86.27720334810438  | Colleges, Universities, and Professional Schools
|611710   |13170     |130442      |9.904479878511768  | Educational Support Services
+---------+----------+------------+-------------------+
 */


spark.stop()