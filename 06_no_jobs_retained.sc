// Any loans where no jobs retained?


import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{count, lit, mean, sum, udf, when}

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

// Percentage of no job loans?
ppp_DF
  .select("*")
  .agg(
    count(lit(1)).as("TotalLoans"),
    count(when(ppp_DF("JobsRetained") === 0, lit(1))).as("NoJobLoans")
  )
  .withColumn("NoJobLoanPercentage", $"NoJobLoans" / $"TotalLoans")
  .show()

/*
+----------+----------+-------------------+
|TotalLoans|NoJobLoans|NoJobLoanPercentage|
+----------+----------+-------------------+
|   4885388|    878268| 0.1797744621307458|
+----------+----------+-------------------+
 */

// ToDo: Is there an NAICS industry that is represented in these null jobs retained
val noJobsRetained = ppp_DF
  .select($"LoanRange", $"LoanAmount", $"BusinessName", $"State", $"NAICSCode", $"JobsRetained", $"CD")
  .where($"JobsRetained" === 0)

// How many no jobs retained loans in the ranges were not jobs retained?
noJobsRetained
  .groupBy($"LoanRange")
  .count()
  .withColumnRenamed("count", "NoJobsLoans")
  .orderBy($"LoanRange")
  .show(false)

/*
+--------------------+-----------+
|LoanRange           |NoJobsLoans|
+--------------------+-----------+
|a $5-10 million     |633        |
|b $2-5 million      |3226       |
|c $1-2 million      |6807       |
|d $350,000-1 million|25823      |
|e $150,000-350,000  |52939      |
|f $0-149,999        |788840     |
+--------------------+-----------+
 */

// Which State had most amount of loans with no jobs retained?
noJobsRetained
  .groupBy($"State")
  .count()
  .withColumnRenamed("count", "NoJobsLoans")
  .orderBy($"NoJobsLoans".desc)
  .show(10,false)

// Which industries sustained the most amount of loans with no jobs retained?
noJobsRetained
  .groupBy($"NAICSCode")
  .count()
  .withColumnRenamed("count", "NoJobsLoans")
  .orderBy($"NoJobsLoans".desc)
  .show(10,false)

/*
+---------+-----------+
|NAICSCode|NoJobsLoans|
+---------+-----------+
|999990   |47084      |
|722511   |26667      |
|812112   |23281      |
|812990   |22648      |
|531210   |22519      |
|621111   |21034      |
|541110   |20401      |
|524210   |15601      |
|541990   |15301      |
|621210   |15152      |
+---------+-----------+
only showing top 10 rows
 */

// Which Congressional District sustained the most amount of loans with no jobs retained?
noJobsRetained
  .groupBy($"CD")
  .count()
  .withColumnRenamed("count", "NoJobsLoans")
  .orderBy($"NoJobsLoans".desc)
  .show(10,false)
/*
+-------+-----------+
|CD     |NoJobsLoans|
+-------+-----------+
|FL - 20|7561       |
|CO - 02|7242       |
|CO - 01|6800       |
|PA - 01|6281       |
|NY - 07|6201       |
|NY - 10|6138       |
|CA - 33|6068       |
|TX - 02|5961       |
|PA - 06|5870       |
|VA - 01|5857       |
+-------+-----------+
only showing top 10 rows
 */

// Which CD sustained the most amount of loans with no industry and no jobs retained?
noJobsRetained
  .select($"NAICSCode", $"CD")
  .where($"NAICSCode" === "999990")
  .groupBy($"CD")
  .count()
  .withColumnRenamed("count", "NoJobsLoans")
  .orderBy($"NoJobsLoans".desc)
  .show(10,false)

/*
+-------+-----------+
|CD     |NoJobsLoans|
+-------+-----------+
|NJ - 05|857        |
|NJ - 07|764        |
|NJ - 01|761        |
|NY - 10|713        |
|NY - 07|622        |
|NJ - 04|602        |
|PA - 01|584        |
|NJ - 03|515        |
|NY - 03|498        |
|PA - 08|473        |
+-------+-----------+
 */

// Which CD sustained the largest reported loan amounts ($0-149,999) and no jobs retained?
noJobsRetained
  .select($"LoanRange", $"LoanAmount", $"CD")
  .where($"LoanRange" === "f $0-149,999")
  .groupBy($"CD")
  .agg(
    count(lit(1)).as("NoJobsLoans"),
    sum($"LoanAmount").as("SumLoanAmount")
  )
  .orderBy($"SumLoanAmount".desc)
  .show(10,false)

/*
+-------+-----------+--------------------+
|CD     |NoJobsLoans|SumLoanAmount       |
+-------+-----------+--------------------+
|FL - 20|6989       |1.852940667299996E8 |
|CO - 02|6571       |1.8293444220999908E8|
|WA - 01|4839       |1.7613580244000012E8|
|PA - 06|5256       |1.7511098652000004E8|
|NY - 10|4926       |1.7380465821999952E8|
|PA - 01|5625       |1.7288201145E8      |
|VA - 01|5206       |1.7253997106000012E8|
|NY - 07|5460       |1.7008038881999978E8|
|CO - 01|6084       |1.6857303897999975E8|
|VA - 02|4420       |1.6805420359500006E8|
+-------+-----------+--------------------+
 */

// Of known loan amounts ($0-149,999) what was the total loan amount with no jobs retained?
noJobsRetained
  .select($"LoanRange", $"LoanAmount")
  .where($"LoanRange" === "f $0-149,999")
  .agg(
    count(lit(1)).as("NoJobsLoans"),
    sum($"LoanAmount").as("SumLoanAmount")
  )
  .show(false)

spark.stop()