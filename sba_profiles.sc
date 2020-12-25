/*
Using converted PDF files from SBA state profiles of 2019 as text, the files were processed as
unstructured data in order to iterate searching for ... Data is extracted from the text files
extract specific aggregate lines of each state's congressional district

Creating a Delta Table as an external data source each file is processed to update table
then saved  in parquet format.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.io.File

// Turing off logging to exclude text output
Logger.getLogger("org").setLevel(Level.OFF)

// Creating local Spark session
val spark = SparkSession
  .builder
  .master("local[*]")
  .appName("sba_profiles")
  .getOrCreate()

// Managing column manipulation in Spark Dataframes
import spark.implicits._

// Function to create a list of text files from a directory
def getListOfFiles(dir: String):List[File] = {
  val d = new File(dir)
  if (d.exists && d.isDirectory) {
    d.listFiles.filter(_.isFile).toList
  } else {
    List[File]()
  }
}

// Using a list of files to use in a iterative function to process each file
val files = getListOfFiles("E:\\IntelliJ-IdeaProjects\\CARES_paycheck_protection_program\\src\\main\\scala\\sba_profiles\\")

// Iterating through list of files to confirm addition
for (file <- files)
  println(file.toString)

/*
Function to process a row by  replacing the thousands-separator,
splitting across two spaces and selecting with the following columns:
small_business_count, pct_sb, employ_count, pct_employ, payroll ($1,000), pct_payroll
 */
val split_row = udf((str: String) => {
  val splitted_row = str.replace(",", "").split("  ").lift // two spaces, lift as an option otherwise none
  Array(splitted_row(0),splitted_row(1), splitted_row(2), splitted_row(3), splitted_row(4), splitted_row(5), splitted_row(6))
})

// Schema used when creating empty Dataframe to insert into Delta Table
val schema = StructType(Array(
  StructField("small_business_count", IntegerType, true),
  StructField("pct_sb", FloatType, true),
  StructField("employ_count", IntegerType, true),
  StructField("pct_employ", FloatType, true),
  StructField("payroll", IntegerType, true),
  StructField("pct_payroll", FloatType, true),
  StructField("state", StringType, true),
  StructField("CD", IntegerType, true)
))

// Defining location of Delta Table in local data store C:\tables
val file = new File("/tables")
val path = file.getAbsolutePath // getCanonicalPath

// Creating an empty dataframe to add the state information from text files
val stateBusProfiles = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

// saveAsTable for writing initial table and saving to parquet from path defaults
stateBusProfiles.write.option("path", path+"/sba_profiles.parquet").mode(SaveMode.Overwrite).saveAsTable("sbaProfilesTable")

// ToDo: Using the list above containing file paths as strings, iterate to:
//  1. Load the text file into Spark Dataframe per state
//  2. Filter all rows that contain the words "District Total" per Congressional District
//  3. Split each row into columns in preparation to insert into Delta Table
//  4. Load Delta Table into Spark Dataframe for processing
//  find the total
//  for each Congressional District below for processing in iteration

for (file <- files) {
  val df = spark.read.text(file.toString)
  val dfCD = df.filter($"value".contains("District Total"))
  val stateCD = dfCD
    .select(split_row($"value").as("data"))
    .select($"data"(1).as("small_business_count"),
      $"data"(2).as("pct_sb"),
      $"data"(3).as("employ_count"),
      $"data"(4).as("pct_employ"),
      $"data"(5).as("payroll"),
      $"data"(6).as("pct_payroll"))
    // ToDo: substring of file name changes with location of text files
    .withColumn("state", lit(file.toString.substring(87, 89)))
    .withColumn("CD",  monotonically_increasing_id()+1)

  // insertInto empty Delta Table and append to parquet file
  stateCD.write.mode(SaveMode.Append).insertInto("sbaProfilesTable")
}

// Loading Delta Table into dataframe using sql
val sbaStateProfilesDF = spark.sql("select * from sbaProfilesTable")

// Aggregation of Congressional Districts, Small Businesses, Employment and
// Payroll for manual verification
sbaStateProfilesDF
  .select($"small_business_count", $"employ_count", $"payroll", $"state")
  .groupBy($"state")
  .agg(
    sum($"small_business_count").as("Total Small Businesses"),
    sum($"employ_count").as("Total Employment"),
    sum($"payroll").as("Total Payroll"),
    count(lit(1)).as("CDs")
  )
  .withColumnRenamed("state", "State")
  .orderBy($"State")
  .show(false)

/*
+-----+----------------------+----------------+-------------+---+
|State|Total Small Businesses|Total Employment|Total Payroll|CDs|
+-----+----------------------+----------------+-------------+---+
|AK   |16408                 |141147          |7124700      |1  |
|AL   |73058                 |785669          |29910028     |7  |
|AR   |49426                 |487999          |16561301     |4  |
|AZ   |106121                |1037218         |40936294     |9  |
|CA   |767258                |7117110         |357436654    |53 |
|CO   |134888                |1115055         |49614785     |7  |
|CT   |70237                 |748495          |38042846     |5  |
|DC   |16685                 |247461          |17036469     |1  |
|DE   |19000                 |187556          |8405445      |1  |
|FL   |448122                |3387133         |136200326    |27 |
|GA   |176448                |1632121         |67349742     |14 |
|HI   |24657                 |273973          |10973480     |2  |
|IA   |61682                 |650982          |24556007     |4  |
|ID   |37315                 |315024          |10925114     |2  |
|IL   |257178                |2480377         |117084073    |18 |
|IN   |107685                |1218564         |46159172     |9  |
|KS   |56763                 |602622          |23025590     |4  |
|KY   |67392                 |700968          |25270357     |6  |
|LA   |80443                 |899290          |35036316     |6  |
|MA   |142096                |1488640         |80608411     |9  |
+-----+----------------------+----------------+-------------+---+
only showing top 20 rows
 */

// Total Aggregates
sbaStateProfilesDF
  .select($"state",$"small_business_count", $"employ_count", $"payroll")
  .agg(
    sum($"small_business_count").as("Total Small Businesses"),
    sum($"employ_count").as("Total Employment"),
    sum($"payroll").as("Total Payroll"),
    count(lit(1)).as("CDs")
  )
  .withColumnRenamed("state", "State")
  .show(false)

/*
+----------------------+----------------+-------------+---+
|Total Small Businesses|Total Employment|Total Payroll|CDs|
+----------------------+----------------+-------------+---+
|6140352               |59775708        |2610774684   |436|
+----------------------+----------------+-------------+---+
436 includes DC, which isn't typically counted as Congressional District
 */
spark.stop()