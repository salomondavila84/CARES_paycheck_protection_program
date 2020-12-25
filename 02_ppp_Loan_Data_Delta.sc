/*
Goal is to transform the Cares Act Paycheck Protection Program CSV files (680 MB) into a Parquet file as a Delta Lake table.
New Delta Table to be used in
- Upload CSV files of PPP/Cares Act business loans from one file for loans > $150k and over 50 files
  from states, territories and districts.
- Perform initial statistics of data with and without nulls for Jobs Retained which affect means and loans/ratio
- Use Delta to export to data warehouse and create versions recording any changes with further refinement
https://home.treasury.gov/policy-issues/cares-act/assistance-for-small-businesses/sba-paycheck-protection-program-loan-level-data
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables._
import org.apache.commons.io.FileUtils
import java.io.File

// Turning Logging off
Logger.getLogger("org").setLevel(Level.OFF)

// Creating Spark session with Delta Lake package and configurations
val spark = SparkSession
  .builder
  .master("local[*]")
  .appName("ppp_dea2Delta")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .getOrCreate()

// Dataframe Column Manipulations
import spark.implicits._

// File for loans more than (>) 150K. File is 128 MB
val ppp150Kplus = "E:\\datasets\\ppp\\150k plus\\PPP Data 150k plus.csv"

// Folder containing all states and DC for loans up to (<) 150K. All files are 550 MB
val pppupto150k = "E:\\datasets\\ppp\\_States\\*"

// Schema for (>) 150k plus loans
val ppp150Kplus_schema = StructType(Array(
  StructField("LoanRange", StringType, false),
  StructField("BusinessName", StringType, false),
  StructField("Address", StringType, false),
  StructField("City", StringType, false),
  StructField("State", StringType, false),
  StructField("Zip", StringType, false),
  StructField("NAICSCode", StringType, false),
  StructField("BusinessType", StringType, false),
  StructField("RaceEthnicity", StringType, false),
  StructField("Gender", StringType, false),
  StructField("Veteran", StringType, false),
  StructField("NonProfit", StringType, true),
  StructField("JobsRetained", IntegerType, false),
  StructField("DateApproved", StringType, false),
  StructField("Lender", StringType, false),
  StructField("CD", StringType, false)
))

// Schema for up to (<) 150k loans
val pppupto150k_schema = StructType(Array(
  StructField("LoanAmount", DoubleType, false),
  StructField("City", StringType, false),
  StructField("State", StringType, false),
  StructField("Zip", StringType, false),
  StructField("NAICSCode", StringType, false),
  StructField("BusinessType", StringType, false),
  StructField("RaceEthnicity", StringType, false),
  StructField("Gender", StringType, false),
  StructField("Veteran", StringType, false),
  StructField("NonProfit", StringType, true),
  StructField("JobsRetained", IntegerType, false),
  StructField("DateApproved", StringType, false),
  StructField("Lender", StringType, false),
  StructField("CD", StringType, false)
))

// Loading of all (>) 150k plus loans
val ppp150kplusDF = spark
  .read
  .format("csv")
  .option("header", "true")
  .schema(ppp150Kplus_schema)
  .csv(ppp150Kplus)

// Loading for up to (<) 150k loans
val ppp_up_to_150kDF = spark
  .read
  .format("csv")
  .option("header", "true")
  .schema(pppupto150k_schema)
  .csv(pppupto150k)

// Test print out of data and schema of dataframes
ppp150kplusDF.show(5, false)
println(ppp150kplusDF.printSchema())
/*
+---------------+-------------------------------------+--------------------------------------+---------+-----+-----+---------+-------------------------------+-------------+----------+----------+---------+------------+------------+-----------------------------------------------+-------+
|LoanRange      |BusinessName                         |Address                               |City     |State|Zip  |NAICSCode|BusinessType                   |RaceEthnicity|Gender    |Veteran   |NonProfit|JobsRetained|DateApproved|Lender                                         |CD     |
+---------------+-------------------------------------+--------------------------------------+---------+-----+-----+---------+-------------------------------+-------------+----------+----------+---------+------------+------------+-----------------------------------------------+-------+
|a $5-10 million|ARCTIC SLOPE NATIVE ASSOCIATION, LTD.|7000 Uula St                          |BARROW   |AK   |99723|813920   |Non-Profit Organization        |Unanswered   |Unanswered|Unanswered|Y        |295         |04/14/2020  |National Cooperative Bank, National Association|AK - 00|
|a $5-10 million|CRUZ CONSTRUCTION INC                |7000 East Palmer Wasilla Hwy          |PALMER   |AK   |99645|238190   |Subchapter S Corporation       |Unanswered   |Unanswered|Unanswered|null     |215         |04/15/2020  |First National Bank Alaska                     |AK - 00|
|a $5-10 million|I. C. E. SERVICES, INC               |2606 C Street                         |ANCHORAGE|AK   |99503|722310   |Corporation                    |Unanswered   |Unanswered|Unanswered|null     |367         |04/11/2020  |KeyBank National Association                   |AK - 00|
|a $5-10 million|KATMAI HEALTH SERVICES LLC           |11001 O'MALLEY CENTRE DRIVE, SUITE 204|ANCHORAGE|AK   |99515|621111   |Limited  Liability Company(LLC)|Unanswered   |Unanswered|Unanswered|null     |0           |04/29/2020  |Truist Bank d/b/a Branch Banking & Trust Co    |AK - 00|
|a $5-10 million|MATANUSKA TELEPHONE ASSOCIATION      |1740 S. CHUGACH ST                    |PALMER   |AK   |99645|517311   |Cooperative                    |Unanswered   |Unanswered|Unanswered|null     |267         |06/10/2020  |CoBank ACB                                     |AK - 00|
+---------------+-------------------------------------+--------------------------------------+---------+-----+-----+---------+-------------------------------+-------------+----------+----------+---------+------------+------------+-----------------------------------------------+-------+
only showing top 5 rows
root
 |-- LoanRange: string (nullable = true)
 |-- BusinessName: string (nullable = true)
 |-- Address: string (nullable = true)
 |-- City: string (nullable = true)
 |-- State: string (nullable = true)
 |-- Zip: string (nullable = true)
 |-- NAICSCode: string (nullable = true)
 |-- BusinessType: string (nullable = true)
 |-- RaceEthnicity: string (nullable = true)
 |-- Gender: string (nullable = true)
 |-- Veteran: string (nullable = true)
 |-- NonProfit: string (nullable = true)
 |-- JobsRetained: integer (nullable = true)
 |-- DateApproved: string (nullable = true)
 |-- Lender: string (nullable = true)
 |-- CD: string (nullable = true)
 */

ppp_up_to_150kDF.show(5, false)
println(ppp_up_to_150kDF.printSchema())
/*
+----------+----------+-----+-----+---------+-------------------------------+-------------+----------+-----------+---------+------------+------------+----------------------+-------+
|LoanAmount|City      |State|Zip  |NAICSCode|BusinessType                   |RaceEthnicity|Gender    |Veteran    |NonProfit|JobsRetained|DateApproved|Lender                |CD     |
+----------+----------+-----+-----+---------+-------------------------------+-------------+----------+-----------+---------+------------+------------+----------------------+-------+
|149999.98 |FREEDOM   |CA   |95019|722511   |Sole Proprietorship            |Unanswered   |Male Owned|Unanswered |null     |30          |04/29/2020  |Santa Cruz County Bank|CA - 20|
|149999.98 |SANTA CRUZ|CA   |95060|722511   |Limited  Liability Company(LLC)|Unanswered   |Male Owned|Non-Veteran|null     |8           |04/30/2020  |Santa Cruz County Bank|CA - 18|
|149999.0  |CALABASAS |CA   |91302|999990   |Limited  Liability Company(LLC)|Unanswered   |Unanswered|Unanswered |null     |12          |04/29/2020  |First Home Bank       |CA - 30|
|149999.0  |VAN NUYS  |CA   |91406|443142   |Corporation                    |Unanswered   |Unanswered|Unanswered |null     |2           |04/29/2020  |First Home Bank       |CA - 29|
|149999.0  |CYPRESS   |CA   |90630|531210   |Subchapter S Corporation       |Unanswered   |Unanswered|Unanswered |null     |2           |04/29/2020  |First Home Bank       |CA - 38|
+----------+----------+-----+-----+---------+-------------------------------+-------------+----------+-----------+---------+------------+------------+----------------------+-------+
only showing top 5 rows
root
 |-- LoanAmount: double (nullable = true)
 |-- City: string (nullable = true)
 |-- State: string (nullable = true)
 |-- Zip: string (nullable = true)
 |-- NAICSCode: string (nullable = true)
 |-- BusinessType: string (nullable = true)
 |-- RaceEthnicity: string (nullable = true)
 |-- Gender: string (nullable = true)
 |-- Veteran: string (nullable = true)
 |-- NonProfit: string (nullable = true)
 |-- JobsRetained: integer (nullable = true)
 |-- DateApproved: string (nullable = true)
 |-- Lender: string (nullable = true)
 |-- CD: string (nullable = true)
 */

// Describing dataframes for identifying null values by comparing count rows
ppp150kplusDF.describe().show()

/*
+-------+------------------+--------------------+--------------------+-----------------+------+------------------+------------------+------------+--------------------+------------+-----------+----------+------------------+-----------------+--------------------+-------+
|summary|         LoanRange|        BusinessName|             Address|             City| State|               Zip|         NAICSCode|BusinessType|       RaceEthnicity|      Gender|    Veteran| NonProfit|      JobsRetained|     DateApproved|              Lender|     CD|
+-------+------------------+--------------------+--------------------+-----------------+------+------------------+------------------+------------+--------------------+------------+-----------+----------+------------------+-----------------+--------------------+-------+
|  count|            661218|              661218|              661218|           661218|661218|            661202|            654436|      659788|              661218|      661218|     661218|     42464|            620710|           661218|              661218| 661218|
|   mean|              null| 7.808774313642858E9| 9.848333505555555E7|         23757.75|  null|51309.943567755596|504692.13930009963|    541310.0|                null|        null|       null|      null|50.678202381144175|             23.0|                null|   null|
| stddev|              null|2.108777827190813E10| 7.793390157245345E8|21677.60384590511|  null|30751.859649974504|177886.61532322777|         NaN|                null|        null|       null|      null| 71.19774100808884|4.242640687119285|                null|   null|
|    min|   a $5-10 million|"BEER WHOLESALE "...| INC. DBA ICE EXP...|         , ALBANY|    AK|             00256|            111110|      541310|American Indian o...|Female Owned|Non-Veteran|Unanswered|                 0|       04/03/2020|	Yankee Farm Cred...|AK - 00|
|    max|e $150,000-350,000|      [24]7.AI, INC.|w7622 STATE HIGHW...|           ZWOLLE|    XX|                TX|            999990|       Trust|               White|  Unanswered|    Veteran|         Y|               500|               26|the Farmers State...|WY - 03|
+-------+------------------+--------------------+--------------------+-----------------+------+------------------+------------------+------------+--------------------+------------+-----------+----------+------------------+-----------------+--------------------+-------+
*/

ppp_up_to_150kDF.describe().show()
/*
+-------+-----------------+--------------------+-------+-----------------+------------------+------------+--------------------+------------+-----------+---------+------------------+------------+--------------------+-------+
|summary|       LoanAmount|                City|  State|              Zip|         NAICSCode|BusinessType|       RaceEthnicity|      Gender|    Veteran|NonProfit|      JobsRetained|DateApproved|              Lender|     CD|
+-------+-----------------+--------------------+-------+-----------------+------------------+------------+--------------------+------------+-----------+---------+------------------+------------+--------------------+-------+
|  count|          4224170|             4224170|4224170|          4223962|           4097426|     4220876|             4224170|     4224170|    4224170|   139218|           3940554|     4224170|             4224170|4224170|
|   mean|33568.83289917653|1.1518278406923077E8|   null|51075.12862284273| 544365.4833407608|        null|                null|        null|       null|     null| 4.991537738094694|        null|                null|   null|
| stddev|33399.31714979236| 7.404247689317782E8|   null|29967.89376116639|189100.79166969546|        null|                null|        null|       null|     null|11.290913946873172|        null|                null|   null|
|    min|        -199659.0|               # 306|     AE|            00092|            111110| Cooperative|American Indian o...|Female Owned|Non-Veteran|        Y|               -10|  04/03/2020|	Farm Credit Serv...|AE - 00|
|    max|        149999.98|     {PEACHTREE CITY|     XX|            99950|            999990|       Trust|               White|  Unanswered|    Veteran|        Y|               500|  06/30/2020|the Farmers State...|WY - 11|
+-------+-----------------+--------------------+-------+-----------------+------------------+------------+--------------------+------------+-----------+---------+------------------+------------+--------------------+-------+
*/

// ToDo: Analytics nulls need to converted to 0s before mean calculations.
// ToDo: Inspect text in Delta Lake table in downstream data wrangling for updating

// Replacing JobsRetained null values with, changing column headers and dropping columns for dataframe concatenation
// (>) 150k plus loans
val ppp150kplusDF_final = ppp150kplusDF
  .na.fill(0, Array("JobsRetained"))
  .withColumn("LoanAmount", lit(0))
  .drop($"Address")
  .drop($"RaceEthnicity")
  .drop($"Gender")
  .drop($"Veteran")
  .drop($"NonProfit")

// Replacing JobsRetained null values with, changing column headers and dropping columns for dataframe concatenation
// (<) 150k loans
val ppp_up_to_150kDF_final = ppp_up_to_150kDF
  .na.fill(0,  Array("JobsRetained"))
  .withColumn("LoanRange", lit("f $0-149,999")) // adding column with identifier
  .withColumn("BusinessName", lit(""))
  .drop($"RaceEthnicity")
  .drop($"Gender")
  .drop($"Veteran")
  .drop($"NonProfit")

// Aggregations of total loans, total jobs retained and average jobs retained
// (>) 150k plus loans
val totals_ppp150kplusDF = ppp150kplusDF_final
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").alias("AverageJobsRetained")
  )

// Inspecting dataframe
totals_ppp150kplusDF.show(false)
/*
+----------+------------+-------------------+
|LoanCounts|JobsRetained|AverageJobsRetained|
+----------+------------+-------------------+
|661218    |31456467    |47.57351886972224  |
+----------+------------+-------------------+
 */

// Aggregations of total loans, total jobs retained and average jobs retained
// (<) 150k loans
val totals_ppp_up_to_150kDF = ppp_up_to_150kDF_final
  .agg(
    count(lit(1)).alias("LoanCounts"),
    mean($"LoanAmount").as("AverageLoanAmount"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").alias("AverageJobsRetained")
  )

// Inspecting dataframe
totals_ppp_up_to_150kDF.show(false)
/*
+----------+-----------------+------------+-------------------+
|LoanCounts|AverageLoanAmount|JobsRetained|AverageJobsRetained|
+----------+-----------------+------------+-------------------+
|4224170   |33568.83289917653|19669424    |4.656399718761318  |
+----------+-----------------+------------+-------------------+
 */

// Aggregations of total loans for (>) 150k plus loans AND (<) 150k loans
val total_loans = totals_ppp150kplusDF.select($"LoanCounts").collect().toList(0).getLong(0) +
                  totals_ppp_up_to_150kDF.select($"LoanCounts").collect().toList(0).getLong(0)

// Defining numerical format
val Locale = new java.util.Locale("en", "US")

println(f"Total loan counts: "+ "%,1d".formatLocal(Locale, total_loans))
//Total loan counts: 4,885,388

// Aggregations of total jobs retained for (>) 150k plus loans AND (<) 150k loans
val total_jobs_retained = totals_ppp150kplusDF.select($"JobsRetained").collect().toList(0).getLong(0) +
  totals_ppp_up_to_150kDF.select($"JobsRetained").collect().toList(0).getLong(0)

println(f"Total jobs retained: "+ "%,1d".formatLocal(Locale, total_jobs_retained))
// Total jobs retained: 51,125,891

// Comparing schema of final dataframes to validate union
ppp150kplusDF_final.printSchema()
ppp_up_to_150kDF_final.printSchema()

// Concatenation of dataframes of (>) 150k plus loans AND (<) 150k loans
// Note ordered columns
val ppp_finalDF = ppp150kplusDF_final
  .select($"LoanRange", $"LoanAmount", $"BusinessName", $"BusinessType", $"City", $"State",
    $"Zip", $"NAICSCode", $"JobsRetained", $"DateApproved", $"Lender", $"CD")
  .union(
    ppp_up_to_150kDF_final
      .select($"LoanRange", $"LoanAmount", $"BusinessName", $"BusinessType", $"City", $"State",
        $"Zip", $"NAICSCode", $"JobsRetained", $"DateApproved", $"Lender", $"CD")
  )

// Creating a Delta Lake table of final concatenated dataframe
println("Creating a Delta Lake table")
val file = new File("/tables/ppp_final.delta")
val path = file.getAbsolutePath // getCanonicalPath

// Exporting dataframe to Delta Lake table
ppp_finalDF.write.format("delta").save(path)

/*
file: java.io.File = \tables\ppp_final.delta
path: String = C:\tables\ppp_final.delta
Final file 68MB
 */
spark.stop()