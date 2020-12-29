/*
ToDo:
- Under the > 150k loans search by business name using word count
  - use top NAICS filter
- Sum jobs retained, loans for total loans > 150k and < 150k

ToDo:
- Questions:
  - Totals
  - By State
  - By CD
    - How many jobs were retained and loans processed by CD of over 150k?
 */
import java.awt.geom.Path2D.Double
import java.io.File

import io.delta.tables.DeltaTable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

// Defining the location of the Delta Lake table
val file = new File("/tables/ppp_final.delta") // Defaults to C:\, otherwise include <drive>:/
val path = file.getAbsolutePath // getCanonicalPath

// Uploading  Delta Lake table in Parquet format from local store C:\
println("Reading the table")
val ppp_DF = spark.read.format("delta").load(path)

// Validating dataframe load
ppp_DF.show(5, false)

// ToDo: Inspect dataframe Congressional districts to verify correct value (435 plus any territories)
//  How many unique congressional districts?
val cds = ppp_DF.select($"CD").distinct()
println("The are %d initial Congressional Districts labeled".format(cds.count()))

// Preparing Congressional Districts for visual inspection
val cds_list = cds.sort($"CD").collect()

for (cd <- cds_list) {
  println(cd) // Printing each Congressional District label
}
/*
Inspection of Congressional Districts yields:
 - Typos of states (e.g. AE, MP, NA)
 - Includes districts of :
    - American Simoa (AS), Puerto Rico (PR), Guam (GU), and District of Columbia (DC)
 - Non-identified districts only states and hyphen
 - CD with an identifier of "- 00" need to be explored
 - Mislabeled districts [JPMorgan Chase Bank, National Association]
 */

// Counting the records which do not identify the district
val non_cd = ppp_DF
  .select("*")
  .where(length($"CD") < 5) // Less than 5 characters since the label only contains state and dash

// Printing sample non-congressional district and found invalid zip code
non_cd.head()

println("Total records with no Congressional District defined: %d".format(non_cd.count()))

// Counting loans with CD identified with "- 00"
val double_O_cds = ppp_DF
  .select("*")
  .where($"CD".contains(" - 00"))
  .groupBy($"CD")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained")
  )
  .orderBy($"CD".asc)

double_O_cds.show(33, false)

/*
+-------+----------+------------+
|CD     |LoanCounts|JobsRetained|
+-------+----------+------------+
|AK - 00|11166     |114123      |
|DC - 00|12480     |165493      |
|DE - 00|12243     |133185      |
|MT - 00|23007     |214797      |
|ND - 00|19645     |175759      |
|PR - 00|37815     |385512      |
|SD - 00|21851     |177119      |
|VT - 00|11923     |113810      |
|WY - 00|12549     |105302      |
+-------+----------+------------+
States above only have one (1) Congressional District and labeled as - 00
 */


// CDs with wrong text of "JPMorgan Chase Bank, National Association"
val fix_cd = ppp_DF
  .select("*")
  .where($"CD".contains("JPMorgan Chase Bank, National Association"))

fix_cd.show(false)

/*
+------------------+----------+--------------------------+------------+------------------+----------+---+---------+------------+------------+----------+-----------------------------------------+
|LoanRange         |LoanAmount|BusinessName              |BusinessType|City              |State     |Zip|NAICSCode|JobsRetained|DateApproved|Lender    |CD                                       |
+------------------+----------+--------------------------+------------+------------------+----------+---+---------+------------+------------+----------+-----------------------------------------+
|e $150,000-350,000|100.0     |"STUDIO ""A"" ARCHITECTURE|541310      |2330 FRANKFORT AVE|LOUISVILLE|KY |40206    |0           |20          |04/27/2020|JPMorgan Chase Bank, National Association|
|e $150,000-350,000|100.0     |"BEER WHOLESALE ""JR.""   |null        |8257 GULF FWY     |HOUSTON   |TX |77017    |0           |26          |05/01/2020|JPMorgan Chase Bank, National Association|
+------------------+----------+--------------------------+------------+------------------+----------+---+---------+------------+------------+----------+-----------------------------------------+
 */

// ToDo: Which industries were served?
val naics_loans = ppp_DF
  .select($"NAICSCode", $"JobsRetained")
  .groupBy($"NAICSCode")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanCounts".desc)

/*
+---------+----------+------------+-------------------+
|NAICSCode|LoanCounts|JobsRetained|AverageJobsRetained|
+---------+----------+------------+-------------------+
|     null|    133526|      790075|  5.917012417057352| null == Unclassified ?
|   999990|     88569|      617073| 6.9671442604071405| Unclassified Establishments

Updating records of null NAICSCode to 999990 - Unclassified Establishments
 */

naics_loans.show(25)

// Creating an instance of Delta Lake table using path to saved delta table
val deltaTable = DeltaTable.forPath(spark, path)

// ToDo: Updating table entries of incorrectly labeled CD, "JPMorgan Chase Bank, National Association"
deltaTable
  .update(
    condition = expr("CD = 'JPMorgan Chase Bank, National Association' and BusinessName = '\\\"STUDIO \\\"\\\"A\\\"\\\" ARCHITECTURE' "),
    set = Map("State" -> expr("'KY'"))
  )
deltaTable
  .update(
    condition = expr("CD = 'JPMorgan Chase Bank, National Association' and BusinessName = '\"STUDIO \"\"A\"\" ARCHITECTURE' "),
    set = Map("NAICSCode" -> expr("'541310'"))
  )

deltaTable
  .update(
    condition = expr("CD = 'JPMorgan Chase Bank, National Association' and BusinessName = '\"STUDIO \"\"A\"\" ARCHITECTURE' "),
    set = Map("JobsRetained" -> expr("20"))
  )

deltaTable
  .update(
    condition = expr("CD = 'JPMorgan Chase Bank, National Association' and BusinessName = '\"STUDIO \"\"A\"\" ARCHITECTURE' "),
    set = Map("CD" -> expr("'KY - 03'"))
  )

// Updating states
deltaTable
  .update(
    condition = expr("CD = 'JPMorgan Chase Bank, National Association' and BusinessName = '\"BEER WHOLESALE \"\"JR.\"\"' "),
    set = Map("State" -> expr("'TX'"))
  )

deltaTable
  .update(
    condition = expr("CD = 'JPMorgan Chase Bank, National Association' and BusinessName = '\"BEER WHOLESALE \"\"JR.\"\"' "),
    set = Map("NAICSCode" -> expr("'999990'"))
  )

deltaTable
  .update(
    condition = expr("CD = 'JPMorgan Chase Bank, National Association' and BusinessName = '\"BEER WHOLESALE \"\"JR.\"\"' "),
    set = Map("JobsRetained" -> expr("26"))
  )

deltaTable
  .update(
    condition = expr("CD = 'JPMorgan Chase Bank, National Association' and BusinessName = '\"BEER WHOLESALE \"\"JR.\"\"' "),
    set = Map("CD" -> expr("'TX - 29'"))
  )

// ToDo: Update null values for NAICS to 999990  Unclassified
deltaTable
  .update(
    condition = expr("NAICSCode is null"),
    set = Map("NAICSCode" -> expr("'999990'"))
  )

// ToDo: Update Congressional Districts identifier from "- 00" to "- 01"
/*
AK - 00, DC - 00, DE - 00, MT - 00, ND - 00, PR - 00, SD - 00, VT - 00, WY - 00
 */

deltaTable
  .update(
    condition = expr(" CD = 'AK - 00' "),
    set = Map("CD" -> expr("'AK - 01'"))
  )

deltaTable
  .update(
    condition = expr(" CD = 'DC - 00' "),
    set = Map("CD" -> expr("'DC - 01'"))
  )

deltaTable
  .update(
    condition = expr(" CD = 'DE - 00' "),
    set = Map("CD" -> expr("'DE - 01'"))
  )

deltaTable
  .update(
    condition = expr(" CD = 'MT - 00' "),
    set = Map("CD" -> expr("'MT - 01'"))
  )

deltaTable
  .update(
    condition = expr(" CD = 'ND - 00' "),
    set = Map("CD" -> expr("'ND - 01'"))
  )

deltaTable
  .update(
    condition = expr(" CD = 'PR - 00' "),
    set = Map("CD" -> expr("'PR - 01'"))
  )

deltaTable
  .update(
    condition = expr(" CD = 'SD - 00' "),
    set = Map("CD" -> expr("'SD - 01'"))
  )

deltaTable
  .update(
    condition = expr(" CD = 'VT - 00' "),
    set = Map("CD" -> expr("'VT - 01'"))
  )

deltaTable
  .update(
    condition = expr(" CD = 'WY - 00' "),
    set = Map("CD" -> expr("'WY - 01'"))
  )

// Showing updates
deltaTable.history().show(false)
/*
+-------+-----------------------+------+--------+---------+------------------------------------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+
|version|timestamp              |userId|userName|operation|operationParameters                                                                                                     |job |notebook|clusterId|readVersion|isolationLevel|isBlindAppend|
+-------+-----------------------+------+--------+---------+------------------------------------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+
|18     |2020-12-28 17:36:07.022|null  |null    |UPDATE   |[predicate -> (CD#691 = WY - 00)]                                                                                       |null|null    |null     |17         |null          |false        |
|17     |2020-12-28 17:35:52.653|null  |null    |UPDATE   |[predicate -> (CD#691 = VT - 00)]                                                                                       |null|null    |null     |16         |null          |false        |
|16     |2020-12-28 17:35:41.405|null  |null    |UPDATE   |[predicate -> (CD#691 = SD - 00)]                                                                                       |null|null    |null     |15         |null          |false        |
|15     |2020-12-28 17:35:30.458|null  |null    |UPDATE   |[predicate -> (CD#691 = PR - 00)]                                                                                       |null|null    |null     |14         |null          |false        |
|14     |2020-12-28 17:35:21.435|null  |null    |UPDATE   |[predicate -> (CD#691 = ND - 00)]                                                                                       |null|null    |null     |13         |null          |false        |
|13     |2020-12-28 17:35:14.366|null  |null    |UPDATE   |[predicate -> (CD#691 = MT - 00)]                                                                                       |null|null    |null     |12         |null          |false        |
|12     |2020-12-28 17:35:06.809|null  |null    |UPDATE   |[predicate -> (CD#691 = DE - 00)]                                                                                       |null|null    |null     |11         |null          |false        |
|11     |2020-12-28 17:35:02.978|null  |null    |UPDATE   |[predicate -> (CD#691 = DC - 00)]                                                                                       |null|null    |null     |10         |null          |false        |
|10     |2020-12-28 17:34:51.676|null  |null    |UPDATE   |[predicate -> (CD#691 = AK - 00)]                                                                                       |null|null    |null     |9          |null          |false        |
|9      |2020-12-26 22:51:30.349|null  |null    |UPDATE   |[predicate -> isnull(NAICSCode#993)]                                                                                    |null|null    |null     |8          |null          |false        |
|8      |2020-12-26 22:50:51.867|null  |null    |UPDATE   |[predicate -> ((CD#997 = JPMorgan Chase Bank, National Association) && (BusinessName#988 = "BEER WHOLESALE ""JR.""))]   |null|null    |null     |7          |null          |false        |
|7      |2020-12-26 22:50:23.848|null  |null    |UPDATE   |[predicate -> ((CD#997 = JPMorgan Chase Bank, National Association) && (BusinessName#988 = "BEER WHOLESALE ""JR.""))]   |null|null    |null     |6          |null          |false        |
|6      |2020-12-26 22:49:59.828|null  |null    |UPDATE   |[predicate -> ((CD#997 = JPMorgan Chase Bank, National Association) && (BusinessName#988 = "BEER WHOLESALE ""JR.""))]   |null|null    |null     |5          |null          |false        |
|5      |2020-12-26 22:49:39.347|null  |null    |UPDATE   |[predicate -> ((CD#997 = JPMorgan Chase Bank, National Association) && (BusinessName#988 = "BEER WHOLESALE ""JR.""))]   |null|null    |null     |4          |null          |false        |
|4      |2020-12-26 22:49:20.072|null  |null    |UPDATE   |[predicate -> ((CD#997 = JPMorgan Chase Bank, National Association) && (BusinessName#988 = "STUDIO ""A"" ARCHITECTURE))]|null|null    |null     |3          |null          |false        |
|3      |2020-12-26 22:48:29.348|null  |null    |UPDATE   |[predicate -> ((CD#997 = JPMorgan Chase Bank, National Association) && (BusinessName#988 = "STUDIO ""A"" ARCHITECTURE))]|null|null    |null     |2          |null          |false        |
|2      |2020-12-26 22:47:49.565|null  |null    |UPDATE   |[predicate -> ((CD#997 = JPMorgan Chase Bank, National Association) && (BusinessName#988 = "STUDIO ""A"" ARCHITECTURE))]|null|null    |null     |1          |null          |false        |
|1      |2020-12-26 22:47:18.823|null  |null    |UPDATE   |[predicate -> ((CD#997 = JPMorgan Chase Bank, National Association) && (BusinessName#988 = "STUDIO ""A"" ARCHITECTURE))]|null|null    |null     |0          |null          |false        |
|0      |2020-12-25 01:19:03.225|null  |null    |WRITE    |[mode -> ErrorIfExists, partitionBy -> []]                                                                              |null|null    |null     |null       |null          |true         |
+-------+-----------------------+------+--------+---------+------------------------------------------------------------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+
Table size up to 270 MB
 */

// Updating the dataframe with the correct version
val ppp_DF = spark.read.format("delta").option("versionAsOf", 9).load(path)

// ToDo: Sample data exploration with updated dataframe

// What are the total loans, total jobs retained and average jobs retained per loan for each loan range?
val loansDF = ppp_DF.select($"LoanRange", $"JobsRetained")
  .groupBy($"LoanRange")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanRange")

loansDF.show(false)

/*
 +--------------------+----------+------------+-------------------+
 |LoanRange           |LoanCounts|JobsRetained|AverageJobsRetained|
 +--------------------+----------+------------+-------------------+
 |a $5-10 million     |4840      |1639326     |338.70371900826444 |
 |b $2-5 million      |24838     |5167844     |208.06200177147917 |
 |c $1-2 million      |53030     |5906794     |111.38589477654158 |
 |d $350,000-1 million|199456    |10015580    |50.21448339483395  |
 |e $150,000-350,000  |379054    |8726969     |23.02302310488743  |
 |f $0-149,999        |4224170   |19669424    |4.656399718761318  |
 +--------------------+----------+------------+-------------------+
*/

// What are the total loans, total jobs retained and average jobs retained per loan for each NAICS code?
val naics_loans = ppp_DF
  .select($"NAICSCode", $"JobsRetained")
  .groupBy($"NAICSCode")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanCounts".desc)

naics_loans.show(false)

/*
+---------+----------+------------+-------------------+
|NAICSCode|LoanCounts|JobsRetained|AverageJobsRetained|
+---------+----------+------------+-------------------+
|   999990|    222096|     1407174| 6.3358817808515235| Unclassified Establishments
|   722511|    170471|     3470010| 20.355427022778066| Full-Service Restaurants
|   541110|    122616|      748971|  6.108264826776277| Offices of Lawyers
|   531210|    114144|      308578|  2.703409728062798| Offices of Real Estate Agents and Brokers
|   621111|    109148|     1184979|   10.8566258657969| Offices of Physicians (except Mental Health Specialists)
|   621210|    100078|      744024|  7.434441135913987| Offices of Dentists
|   813110|     88411|     1051114| 11.888950469964145| Religious Organizations
|   812112|     86561|      320814|  3.706218735920334| Beauty Salons
|   524210|     82922|      360081|  4.342406116591496| Insurance Agencies and Brokerages
|   722513|     78013|     1820214| 23.332188225039417| Limited-Service Restaurants
|   453998|     63728|      401973|  6.307635576198845| All Other Miscellaneous Store Retailers (except Tobacco Stores)
|   236115|     59011|      311914|  5.285692498008846| New Single-Family Housing Construction (except For-Sale Builders)
|   238220|     58756|      666188|   11.3382122676833| Plumbing, Heating, and Air-Conditioning Contractors
|   812990|     57747|      228974| 3.9651237293712227| All Other Personal Services
|   541990|     57323|      359431|  6.270275456622996| All Other Professional, Scientific, and Technical Services
|   811111|     51681|      275085| 5.3227491728101235| General Automotive Repair
|   484110|     50554|      300342|  5.941013569648297| General Freight Trucking, Local
|   236118|     49820|      226845|  4.553291850662385| Residential Remodelers
|   541618|     43884|      213308|  4.860723726187221| Other Management Consulting Services
|   561730|     43427|      401136|  9.237018444746356| Landscaping Services
|   238210|     43007|      538042| 12.510568047062106| Electrical Contractors and Other Wiring Installation Contractors
|   238990|     41891|      413546|    9.8719534028789| All Other Specialty Trade Contractors
|   721110|     40986|      886585| 21.631410725613623| Hotels (except Casino Hotels) and Motels
|   541611|     40097|      267615|  6.674190089034092| Administrative Management and General Management Consulting Services
|   624410|     39222|      467827| 11.927668145428585| Child Day Care Services
+---------+----------+------------+-------------------+
 */

spark.stop()

