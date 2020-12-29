// ToDo: Conduct a study of Cares Act Paycheck Protection Program (PPP) total loans, total jobs retained and
//   average jobs retained per loan by state, Congressional District, zip code, NAICS code (industry)
//   and lenger to compare with Small Business Administration (SBA) 2019 profiles of total small businesses, jobs,
//   and payroll within state and congressional district.
//  Note: PPP data includes loan amounts ($) for loans < $150k but not the business name
//  yet the business name is included for loans > $150k but only the loan categorical rage.

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{count, lit, mean, sum, udf}

import java.io.File
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types._


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
// Importing SBA Profiles
val file = new File("/tables/sba_profiles.parquet") // Defaults to C:\, otherwise include <drive>:/
val path = file.getAbsolutePath // getCanonicalPath

println("Reading the table")
val sba_profiles_DF = spark.read.format("parquet").load(path)

// Verification of dataframe
sba_profiles_DF.show(5)

/*
+--------------------+------+------------+----------+-------+-----------+-----+---+
|small_business_count|pct_sb|employ_count|pct_employ|payroll|pct_payroll|state| CD|
+--------------------+------+------------+----------+-------+-----------+-----+---+
|               12670|  95.9|      107178|      62.5|3840495|       56.8|   CA|  1|
|               18730|  96.5|      162859|      71.8|7725622|       64.5|   CA|  2|
|                9135|  92.4|       85137|      51.3|3357196|       45.1|   CA|  3|
|               15671|  95.1|      124526|      55.9|5206102|       50.0|   CA|  4|
|               14298|  95.0|      136107|      58.0|6290668|       50.6|   CA|  5|
+--------------------+------+------------+----------+-------+-----------+-----+---+
 */

// ToDo: Construct Congressional District identifier similar to PPP table in order to group.
// Defining User-Defined Function and using 2 places in integer for the leading zero in single digits.
val concat_columns = udf(
  (first_column: String, second_column: Int) => {first_column + " - " + "%02d".format(second_column)}
)

// Creating CD identifier column by concatenating two columns (string + int)
val sba_profiles_with_CD = sba_profiles_DF
  .withColumn("CD_update", concat_columns($"state", $"CD"))

sba_profiles_with_CD.show(5, false)
/*
+--------------------+------+------------+----------+-------+-----------+-----+---+---------+
|small_business_count|pct_sb|employ_count|pct_employ|payroll|pct_payroll|state|CD |CD_update|
+--------------------+------+------------+----------+-------+-----------+-----+---+---------+
|12670               |95.9  |107178      |62.5      |3840495|56.8       |CA   |1  |CA - 01  |
|18730               |96.5  |162859      |71.8      |7725622|64.5       |CA   |2  |CA - 02  |
|9135                |92.4  |85137       |51.3      |3357196|45.1       |CA   |3  |CA - 03  |
|15671               |95.1  |124526      |55.9      |5206102|50.0       |CA   |4  |CA - 04  |
|14298               |95.0  |136107      |58.0      |6290668|50.6       |CA   |5  |CA - 05  |
+--------------------+------+------------+----------+-------+-----------+-----+---+---------+
only showing top 5 rows
 */

// Constructing file location of data store of Delta Lake table
val file = new File("/tables/ppp_final.delta") // Defaults to C:\, otherwise include <drive>:/
val path = file.getAbsolutePath // getCanonicalPath

// Loading Delta Lake table in Parquet format with latest version
println("Reading the table")
val ppp_DF = spark.read.format("delta").option("versionAsOf", 18).load(path)

// Verification of dataframe
ppp_DF.show(5)

/*
+------------+----------+------------+--------------------+-------------+-----+-----+---------+------------+------------+--------------------+-------+
|   LoanRange|LoanAmount|BusinessName|        BusinessType|         City|State|  Zip|NAICSCode|JobsRetained|DateApproved|              Lender|     CD|
+------------+----------+------------+--------------------+-------------+-----+-----+---------+------------+------------+--------------------+-------+
|f $0-149,999|  149990.0|            |Subchapter S Corp...|STATEN ISLAND|   NY|10309|   238340|           8|  05/01/2020|JPMorgan Chase Ba...|NY - 11|
|f $0-149,999|  149990.0|            |         Corporation|   WHITESTONE|   NY|11357|   621210|          16|  04/29/2020|JPMorgan Chase Ba...|NY - 03|
|f $0-149,999|  149987.0|            |         Corporation|     BROOKLYN|   NY|11205|   484110|          13|  04/28/2020|      East West Bank|NY - 07|
|f $0-149,999|  149982.0|            |Subchapter S Corp...|ISLIP TERRACE|   NY|11752|   236115|           8|  05/01/2020|JPMorgan Chase Ba...|NY - 02|
|f $0-149,999|  149979.0|            |Subchapter S Corp...|     NEW YORK|   NY|10005|   541840|          11|  05/01/2020| Haddon Savings Bank|NY - 10|
+------------+----------+------------+--------------------+-------------+-----+-----+---------+------------+------------+--------------------+-------+
only showing top 5 rows
 */

// ToDo: Finding Top 3 State, Congressional District, Zip Code, NAICSCode and Lender for
//  loan counts, jobs retained and average jobs per loan

// Dataframe of states and jobs retained
val state_DF = ppp_DF
  .select($"State", $"JobsRetained")
  .groupBy($"State")

// Top 3 states with in total loans
state_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanCounts".desc)
  .show(3,false)

/*
+-----+----------+------------+-------------------+
|State|LoanCounts|JobsRetained|AverageJobsRetained|
+-----+----------+------------+-------------------+
|CA   |581125    |6505547     |11.19474639707464  |
|FL   |393016    |3224664     |8.2049178659393    |
|TX   |389387    |4519665     |11.607128640658265 |
+-----+----------+------------+-------------------+
 */

// Top 3 states with in total jobs retained
state_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"JobsRetained".desc)
  .show(3,false)

/*
+-----+----------+------------+-------------------+
|State|LoanCounts|JobsRetained|AverageJobsRetained|
+-----+----------+------------+-------------------+
|CA   |581125    |6505547     |11.19474639707464  |
|TX   |389387    |4519665     |11.607128640658265 |
|FL   |393016    |3224664     |8.2049178659393    |
+-----+----------+------------+-------------------+
 */

// Top 3 states with in average jobs per loan
state_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"AverageJobsRetained".desc)
  .show(3,false)

/*
+-----+----------+------------+-------------------+
|State|LoanCounts|JobsRetained|AverageJobsRetained|
+-----+----------+------------+-------------------+
|AS   |223       |3731        |16.730941704035875 |
|MP   |473       |7456        |15.763213530655392 |
|UT   |50691     |796849      |15.719733285987651 |
+-----+----------+------------+-------------------+
 */

// Dataframe of CDs and jobs retained
val CD_DF = ppp_DF
  .select($"JobsRetained", $"CD")
  .groupBy($"CD")

// Top 3 CDs with in total loans
CD_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanCounts".desc)
  .show(3,false)

/*
+-------+----------+------------+-------------------+
|CD     |LoanCounts|JobsRetained|AverageJobsRetained|
+-------+----------+------------+-------------------+
|PR - 00|37815     |385512      |10.19468464894883  |
|CA - 33|34211     |302383      |8.838765309403408  |
|TX - 02|33918     |456767      |13.466802287870747 |
+-------+----------+------------+-------------------+
 */

// Top 3 CDs with in total jobs retained
CD_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"JobsRetained".desc)
  .show(3,false)

/*
+-------+----------+------------+-------------------+
|CD     |LoanCounts|JobsRetained|AverageJobsRetained|
+-------+----------+------------+-------------------+
|TX - 02|33918     |456767      |13.466802287870747 |
|PR - 00|37815     |385512      |10.19468464894883  |
|NY - 10|31567     |367253      |11.634079893559731 |
+-------+----------+------------+-------------------+
 */

// Top 3 CDs with in average jobs per loan
CD_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"AverageJobsRetained".desc)
  .show(3,false)

/*
+-------+----------+------------+-------------------+
|CD     |LoanCounts|JobsRetained|AverageJobsRetained|
+-------+----------+------------+-------------------+
|UT - 12|1         |150         |150.0              |
|PR - 30|1         |107         |107.0              |
|NV - 42|1         |90          |90.0               |
+-------+----------+------------+-------------------+
 */

// Dataframe of zip codes and jobs retained
val zip_code_DF = ppp_DF
  .select($"JobsRetained", $"Zip")
  .groupBy($"Zip")

// Top 3 zip codes with in total loans
zip_code_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanCounts".desc)
  .show(3,false)

/*
+-----+----------+------------+-------------------+
|Zip  |LoanCounts|JobsRetained|AverageJobsRetained|
+-----+----------+------------+-------------------+
|10001|4889      |65774       |13.453466966659848 | New York, NY
|10018|4349      |69301       |15.93492756955622  | New York, NY
|10016|3647      |40100       |10.995338634494106 | New York, NY
+-----+----------+------------+-------------------+
 */

// Top 3 zip codes with in total jobs retained
zip_code_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"JobsRetained".desc)
  .show(3,false)

/*
+-----+----------+------------+-------------------+
|Zip  |LoanCounts|JobsRetained|AverageJobsRetained|
+-----+----------+------------+-------------------+
|10018|4349      |69301       |15.93492756955622  | New York, NY
|10001|4889      |65774       |13.453466966659848 | New York, NY
|10016|3647      |40100       |10.995338634494106 | New York, NY
+-----+----------+------------+-------------------+
 */

// Top 3 zip codes with in average jobs per loan
zip_code_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"AverageJobsRetained".desc)
  .show(3,false)

/*
+-----+----------+------------+-------------------+
|Zip  |LoanCounts|JobsRetained|AverageJobsRetained|
+-----+----------+------------+-------------------+
|96312|1         |500         |500.0              | Armed Forces Pacific ZIP Code
|87828|1         |500         |500.0              | Polvadera, NM
|17375|1         |500         |500.0              | Peach Glen, PA
+-----+----------+------------+-------------------+
 */

// Dataframe of NAICS codes and jobs retained
val naics_code_DF = ppp_DF
  .select($"JobsRetained", $"NAICSCode")
  .groupBy($"NAICSCode")

// Top 3 NAICS codes with in total loans
naics_code_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanCounts".desc)
  .show(3,false)

/*
+---------+----------+------------+-------------------+
|NAICSCode|LoanCounts|JobsRetained|AverageJobsRetained|
+---------+----------+------------+-------------------+
|999990   |222096    |1407174     |6.3358817808515235 | Unclassified Establishments
|722511   |170471    |3470010     |20.355427022778066 | Full-Service Restaurants
|541110   |122616    |748971      |6.108264826776277  | Offices of Lawyers
+---------+----------+------------+-------------------+
 */

// Top 3 NAICS codes with in total jobs retained
naics_code_DF
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
|722511   |170471    |3470010     |20.355427022778066 | Full-Service Restaurants
|722513   |78013     |1820214     |23.332188225039417 | Limited-Service Restaurants
|999990   |222096    |1407174     |6.3358817808515235 | Unclassified Establishments
+---------+----------+------------+-------------------+
 */

// Top 3 NAICS codes with in average jobs per loan
naics_code_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"AverageJobsRetained".desc)
  .show(3,false)

/*
+---------+----------+------------+-------------------+
|NAICSCode|LoanCounts|JobsRetained|AverageJobsRetained|
+---------+----------+------------+-------------------+
|422410   |1         |270         |270.0              | 424410 General Line Grocery Merchant Wholesalers
|325181   |1         |124         |124.0              | 325180 Other Basic Inorganic Chemical Manufacturing
|314991   |1         |120         |120.0              | 314910 Textile Bag and Canvas Mills
+---------+----------+------------+-------------------+
 */

// Dataframe of lenders and jobs retained
val lender_DF = ppp_DF
  .select($"JobsRetained", $"Lender")
  .groupBy($"Lender")

// Top 3 lenders with in total loans
lender_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanCounts".desc)
  .show(3,false)

/*
+-----------------------------------------+----------+------------+-------------------+
|Lender                                   |LoanCounts|JobsRetained|AverageJobsRetained|
+-----------------------------------------+----------+------------+-------------------+
|Bank of America, National Association    |334761    |3112313     |9.297119437449405  |
|JPMorgan Chase Bank, National Association|269422    |2762355     |10.252893230693855 |
|Wells Fargo Bank, National Association   |185598    |0           |0.0                |
+-----------------------------------------+----------+------------+-------------------+
 */

// Top 3 lenders with in total jobs retained
lender_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"JobsRetained".desc)
  .show(3,false)

/*
+-----------------------------------------+----------+------------+-------------------+
|Lender                                   |LoanCounts|JobsRetained|AverageJobsRetained|
+-----------------------------------------+----------+------------+-------------------+
|Bank of America, National Association    |334761    |3112313     |9.297119437449405  |
|JPMorgan Chase Bank, National Association|269422    |2762355     |10.252893230693855 |
|Zions Bank, A Division of                |46707     |1959265     |41.94799494722418  |
+-----------------------------------------+----------+------------+-------------------+
 */

// Top 3 lenders with in average jobs per loan
lender_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"AverageJobsRetained".desc)
  .show(3,false)

/*
+-----------------------------+----------+------------+-------------------+
|Lender                       |LoanCounts|JobsRetained|AverageJobsRetained|
+-----------------------------+----------+------------+-------------------+
|CFG Community Bank           |250       |23174       |92.696             |
|Toyota Financial Savings Bank|428       |38865       |90.80607476635514  |
|CoBank ACB                   |288       |21772       |75.59722222222223  |
+-----------------------------+----------+------------+-------------------+
 */

// Dataframe of state and loan ranges of jobs retained
val state_loan_ranges_DF = ppp_DF
  .select($"LoanRange", $"State", $"JobsRetained")
  .groupBy($"State", $"LoanRange")

// Top 3 State Loan Ranges in total loans
state_loan_ranges_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanCounts".desc)
  .show(3,false)

/*
+-----+------------+----------+------------+-------------------+
|State|LoanRange   |LoanCounts|JobsRetained|AverageJobsRetained|
+-----+------------+----------+------------+-------------------+
|CA   |f $0-149,999|493436    |2364647     |4.792206081437106  |
|FL   |f $0-149,999|350809    |1357580     |3.869855106339917  |
|TX   |f $0-149,999|337237    |1776705     |5.268416573507652  |
+-----+------------+----------+------------+-------------------+
 */

// Top 3 State Loan Ranges in total jobs retained
state_loan_ranges_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"JobsRetained".desc)
  .show(3,false)

/*
+-----+------------+----------+------------+-------------------+
|State|LoanRange   |LoanCounts|JobsRetained|AverageJobsRetained|
+-----+------------+----------+------------+-------------------+
|CA   |f $0-149,999|493436    |2364647     |4.792206081437106  |
|TX   |f $0-149,999|337237    |1776705     |5.268416573507652  |
|FL   |f $0-149,999|350809    |1357580     |3.869855106339917  |
+-----+------------+----------+------------+-------------------+
 */

// Top 3 State Loan Ranges in highest average jobs/loan
state_loan_ranges_DF
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"AverageJobsRetained".desc)
  .show(5,false)

/*
+-----+---------------+----------+------------+-------------------+
|State|LoanRange      |LoanCounts|JobsRetained|AverageJobsRetained|
+-----+---------------+----------+------------+-------------------+
|PR   |a $5-10 million|10        |5000        |500.0              |
|VI   |a $5-10 million|1         |493         |493.0              |
|MS   |a $5-10 million|17        |7103        |417.8235294117647  |
|KS   |a $5-10 million|37        |15274       |412.81081081081084 |
|TN   |a $5-10 million|87        |34596       |397.6551724137931  |
+-----+---------------+----------+------------+-------------------+
MS, KS and TN businesses in the highest rage have the largest average jobs retained.
 */

// ToDo: Create temporary views of SBA profiles and PPP data to join based on CD identifier
//  to develop ratios of businesses served, employment saved and payroll (only for loans up to $150k)
//  by PPP within state and CD

// Grouping by CD and creating temporary views for SQL join
ppp_DF
  .select( $"JobsRetained", $"CD")
  .groupBy($"CD")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained")
  )
  .orderBy($"CD")
  .createOrReplaceTempView("ppp_CD")

// Inspecting view
spark.sql("""
           |select *
           |from ppp_CD
           |""".stripMargin).show(3, false)

/*
+-------+----------+------------+
|CD     |LoanCounts|JobsRetained|
+-------+----------+------------+
|AE - 00|1         |0           |
|AK - 00|11166     |114123      |
|AK - 01|1         |1           |
+-------+----------+------------+
 */

sba_profiles_with_CD
  .select($"small_business_count", $"employ_count", $"state", $"CD_update")
  .withColumnRenamed("CD_update", "CD")
  .createOrReplaceTempView("sba_CD")

// Inspecting view
spark.sql("""
            |select *
            |from sba_CD
            |""".stripMargin).show(3, false)

/*
+--------------------+------------+-----+-------+
|small_business_count|employ_count|state|CD     |
+--------------------+------------+-----+-------+
|12670               |107178      |CA   |CA - 01|
|18730               |162859      |CA   |CA - 02|
|9135                |85137       |CA   |CA - 03|
+--------------------+------------+-----+-------+
 */

// Inspecting tables
spark.catalog.listTables().show()

/*
+------+--------+-----------+---------+-----------+
|  name|database|description|tableType|isTemporary|
+------+--------+-----------+---------+-----------+
|ppp_cd|    null|       null|TEMPORARY|       true|
|sba_cd|    null|       null|TEMPORARY|       true|
+------+--------+-----------+---------+-----------+
 */

// Creating SQL join construct of views into a dataframe
val ppp_sba_join = spark.sql("""
                               |select p.CD, p.JobsRetained, p.LoanCounts, s.CD, s.state, s.small_business_count, s.employ_count
                               |from ppp_CD as p
                               |inner join sba_CD as s
                               |on p.CD = s.CD
                               |""".stripMargin)

ppp_sba_join.show(3, false)

/*
+-------+------------+----------+-------+-----+--------------------+------------+
|CD     |JobsRetained|LoanCounts|CD     |state|small_business_count|employ_count|
+-------+------------+----------+-------+-----+--------------------+------------+
|AK - 01|1           |1         |AK - 01|AK   |16408               |141147      |
|AL - 01|114691      |12143     |AL - 01|AL   |11362               |119396      |
|AL - 02|108961      |10177     |AL - 02|AL   |10227               |113644      |
+-------+------------+----------+-------+-----+--------------------+------------+
ToDo: Drop duplicate column of CD as result of join
 */

// Creating ratio columns of Jobs Retained / Employment Count and Loan Counts / Business Count
// Dropping duplicated column
val ppp_sba_ratio = ppp_sba_join
  .select("*")
  .withColumn("RetainedRatio", $"JobsRetained" / $"employ_count")
  .withColumn("ServedRatio", $"LoanCounts" / $"small_business_count")
  .drop($"s.CD")

println("Number of Congressional District matches:")
ppp_sba_ratio.count()
//Number of Congressional District matches: 433

ppp_sba_ratio.show(3, false)

/*
+-------+------------+----------+-----+--------------------+------------+--------------------+--------------------+
|CD     |JobsRetained|LoanCounts|state|small_business_count|employ_count|RetainedRatio       |ServedRatio         |
+-------+------------+----------+-----+--------------------+------------+--------------------+--------------------+
|AK - 01|1           |1         |AK   |16408               |141147      |7.084812287898432E-6|6.094588005850804E-5|
|AL - 01|114691      |12143     |AL   |11362               |119396      |0.960593319709203   |1.068737898257349   |
|AL - 02|108961      |10177     |AL   |10227               |113644      |0.958792369152793   |0.9951109807372641  |
+-------+------------+----------+-----+--------------------+------------+--------------------+--------------------+
 */

// How many CDs served more businesses or retained more employment than their 2019 profiles?
ppp_sba_ratio
  .select("*")
  .where($"RetainedRatio" > 1 || $"ServedRatio" > 1)
  .count()

// 132 of the CD had higher that 1 as a ratio of employed or businesses from their 2019 profiles

// What were the top 3 CDs with the highest/lowest jobs percentage retained?
ppp_sba_ratio
  .select("*")
  .sort($"RetainedRatio".desc)
  .show(3)

/*
+-------+------------+----------+-----+--------------------+------------+------------------+------------------+
|     CD|JobsRetained|LoanCounts|state|small_business_count|employ_count|     RetainedRatio|       ServedRatio|
+-------+------------+----------+-----+--------------------+------------+------------------+------------------+
|CA - 21|      155994|      9530|   CA|                5291|       54702| 2.851705604913897|1.8011718011718012|
|TX - 02|      456767|     33918|   TX|               16637|      188707| 2.420509043119757|2.0387089018452844|
|NY - 07|      315369|     32500|   NY|               20954|      152632|2.0662049897793384|1.5510165123604085|
+-------+------------+----------+-----+--------------------+------------+------------------+------------------+
 */

ppp_sba_ratio
  .select("*")
  .sort($"RetainedRatio".asc)
  .show(3)

/*
+-------+------------+----------+-----+--------------------+------------+--------------------+--------------------+
|     CD|JobsRetained|LoanCounts|state|small_business_count|employ_count|       RetainedRatio|         ServedRatio|
+-------+------------+----------+-----+--------------------+------------+--------------------+--------------------+
|AK - 01|           1|         1|   AK|               16408|      141147|7.084812287898432E-6|6.094588005850804E-5|
|MT - 01|         474|        96|   MT|               31557|      245411|0.001931453765316...|0.003042114269417245|
|SD - 01|        1832|       295|   SD|               21328|      210534|0.008701682388592816|0.013831582895723931|
+-------+------------+----------+-----+--------------------+------------+--------------------+--------------------+
 */

// What were the top 3 CDs with the highest/lowest small businesses served?
ppp_sba_ratio
  .select("*")
  .sort($"ServedRatio".desc)
  .show(3)

/*
+-------+------------+----------+-----+--------------------+------------+------------------+------------------+
|     CD|JobsRetained|LoanCounts|state|small_business_count|employ_count|     RetainedRatio|       ServedRatio|
+-------+------------+----------+-----+--------------------+------------+------------------+------------------+
|TX - 02|      456767|     33918|   TX|               16637|      188707| 2.420509043119757|2.0387089018452844|
|FL - 20|      225468|     29015|   FL|               14247|      118934|1.8957404947281686| 2.036569102267144|
|IL - 01|      181682|     18934|   IL|                9781|       89864|2.0217439686637584|1.9357938861057151|
+-------+------------+----------+-----+--------------------+------------+------------------+------------------+
 */

ppp_sba_ratio
  .select("*")
  .sort($"ServedRatio".asc)
  .show(3)

/*
+-------+------------+----------+-----+--------------------+------------+--------------------+--------------------+
|     CD|JobsRetained|LoanCounts|state|small_business_count|employ_count|       RetainedRatio|         ServedRatio|
+-------+------------+----------+-----+--------------------+------------+--------------------+--------------------+
|AK - 01|           1|         1|   AK|               16408|      141147|7.084812287898432E-6|6.094588005850804E-5|
|MT - 01|         474|        96|   MT|               31557|      245411|0.001931453765316...|0.003042114269417245|
|WY - 01|        2389|       209|   WY|               17158|      131499|0.018167438535654265|0.012180906865602052|
+-------+------------+----------+-----+--------------------+------------+--------------------+--------------------+
 */


// Recreating with grouping by states
val state_ratio = ppp_sba_join
  .select("*")
  .groupBy($"state")
  .agg(
    sum($"JobsRetained").as("JobsRetained"),
    sum($"LoanCounts").as("LoanCounts"),
    sum($"small_business_count").as("small_business_count"),
    sum($"employ_count").as("employ_count")
  )
  .withColumn("RetainedRatio", $"JobsRetained" / $"employ_count")
  .withColumn("ServedRatio", $"LoanCounts" / $"small_business_count")
  .drop($"s.CD")
  .sort($"state".asc)

state_ratio.show(50)

/*
+-----+------------+----------+--------------------+------------+--------------------+--------------------+
|state|JobsRetained|LoanCounts|small_business_count|employ_count|       RetainedRatio|         ServedRatio|
+-----+------------+----------+--------------------+------------+--------------------+--------------------+
|   AK|           1|         1|               16408|      141147|7.084812287898432E-6|6.094588005850804E-5|
|   AL|      672633|     65786|               73058|      785669|  0.8561277077242452|   0.900462646116784|
|   AR|      374290|     42158|               49426|      487999|  0.7669892766173702|  0.8529518876704568|
|   AZ|     1027650|     80996|              106121|     1037218|  0.9907753239916778|  0.7632419596498337|
|   CA|     6504766|    581040|              767258|     7117110|  0.9139617063667697|   0.757294156594001|
|   CO|      931700|    104382|              134888|     1115055|  0.8355641649963454|  0.7738420022537216|
|   CT|      601586|     60838|               70237|      748495|  0.8037274798094843|  0.8661816421544201|
|   DE|        2899|       257|               19000|      187556|0.015456716927211072|0.013526315789473685|
|   FL|     3225221|    393006|              448122|     3387133|  0.9521979207784282|  0.8770067079946978|
|   GA|     1471763|    156804|              176448|     1632121|  0.9017487061314694|  0.8886697497279652|
|   HI|      225436|     24534|               24657|      273973|     0.8228402068817|  0.9950115585837693|
|   IA|      523311|     58463|               61682|      650982|  0.8038793699364959|  0.9478129762329367|
|   ID|      305355|     30164|               37315|      315024|  0.9693071004113972|  0.8083612488275492|
|   IL|     2158290|    201933|              257178|     2480377|  0.8701459495875022|  0.7851876910155612|
|   IN|      951708|     79139|              107685|     1218564|  0.7810078091918028|  0.7349120118865209|
|   KS|      521381|     51872|               56763|      602622|  0.8651874641151501|  0.9138347162764476|
|   KY|      598448|     48354|               67392|      700968|  0.8537451067666427|  0.7175035612535613|
|   LA|      800132|     73819|               80443|      899290|  0.8897374595514239|  0.9176559800106908|
|   MA|     1143507|    112985|              142096|     1488640|  0.7681554976354257|  0.7951314604211237|
|   MD|      938401|     81312|              108799|     1142518|  0.8213446090127245|   0.747359810292374|
|   ME|      243352|     27192|               33087|      288696|  0.8429351289938205|  0.8218333484450087|
|   MI|     1553464|    121107|              174421|     1862153|  0.8342300552102861|  0.6943372644349017|
|   MN|     1090741|     98121|              118612|     1253038|  0.8704771922319994|  0.8272434492294203|
|   MO|      936038|     91498|              125189|     1167199|  0.8019523663060026|  0.7308789110864373|
|   MS|      412539|     45813|               43279|      439244|  0.9392023567766435|  1.0585503361907622|
|   MT|         474|        96|               31557|      245411|0.001931453765316...|0.003042114269417245|
|   NC|     1247021|    121910|              174546|     1667282|  0.7479364618582819|  0.6984405257066905|
|   NE|      327613|     42498|               42409|      411977|  0.7952215779036208|   1.002098611143861|
|   NH|      209979|     23823|               29529|      295407|  0.7108125399872041|  0.8067662298079854|
|   NJ|     1450326|    147149|              193462|     1807432|  0.8024235489910547|    0.76060931862588|
|   NM|      247892|     21923|               33157|      333905|  0.7424027792336143|  0.6611876828422354|
|   NV|      524991|     42120|               49922|      485582|  1.0811582801668924|  0.8437161972677377|
|   NY|     3162575|    323865|              466419|     4100314|  0.7713006857523594|   0.694364937963505|
|   OH|     1879664|    140250|              186476|     2185363|  0.8601152302843967|  0.7521075098135953|
|   OK|      620323|     64276|               71767|      708679|   0.875322960042558|  0.8956205498348824|
|   OR|      611330|     62767|               92867|      852097|  0.7174417935986162|  0.6758805603712836|
|   PA|     1821741|    165908|              231914|     2492796|  0.7308022798496147|  0.7153858757987874|
|   RI|      157411|     17160|               23306|      229818|  0.6849376463114291|   0.736291083841071|
|   SC|      658323|     63173|               79698|      792641|  0.8305437139890568|  0.7926547717634068|
|   SD|        1832|       295|               21328|      210534|0.008701682388592816|0.013831582895723931|
|   TN|      917084|     93291|               96928|     1091772|  0.8399958965791392|  0.9624773027401783|
|   TX|     4515187|    388575|              442996|     4691617|  0.9623946285470446|  0.8771523896378297|
|   UT|      796308|     50682|               63379|      570724|  1.3952593547844492|  0.7996655043468657|
|   VA|      974002|    109217|              151680|     1531614|  0.6359317687093484|  0.7200487869198312|
|   WA|      906640|    101040|              152365|     1377918|   0.657978196090043|  0.6631444229317757|
|   WI|      999004|     85453|              108636|     1257537|  0.7944132061323047|  0.7865992856879855|
|   WV|      204062|     17322|               26138|      273493|  0.7461324421465997|  0.6627132909939552|
|   WY|        2389|       209|               17158|      131499|0.018167438535654265|0.012180906865602052|
+-----+------------+----------+--------------------+------------+--------------------+--------------------+
 */

// ToDo: What were the top 3 states with the highest and lowest ratio for:
//  jobs percentage retained
//  serviced businesses

// Top 3 states with highest job retained percentage
state_ratio
  .orderBy($"RetainedRatio".desc)
  .show(3,false)

/*
+-----+------------+----------+--------------------+------------+------------------+------------------+
|state|JobsRetained|LoanCounts|small_business_count|employ_count|RetainedRatio     |ServedRatio       |
+-----+------------+----------+--------------------+------------+------------------+------------------+
|UT   |796308      |50682     |63379               |570724      |1.3952593547844492|0.7996655043468657|
|NV   |524991      |42120     |49922               |485582      |1.0811582801668924|0.8437161972677377|
|AZ   |1027650     |80996     |106121              |1037218     |0.9907753239916778|0.7632419596498337|
+-----+------------+----------+--------------------+------------+------------------+------------------+
 */

// Top 3 states with lowest job retained percentage
state_ratio
  .orderBy($"RetainedRatio".asc)
  .show(3,false)

/*
+-----+------------+----------+--------------------+------------+---------------------+--------------------+
|state|JobsRetained|LoanCounts|small_business_count|employ_count|RetainedRatio        |ServedRatio         |
+-----+------------+----------+--------------------+------------+---------------------+--------------------+
|AK   |1           |1         |16408               |141147      |7.084812287898432E-6 |6.094588005850804E-5|
|MT   |474         |96        |31557               |245411      |0.0019314537653161432|0.003042114269417245|
|SD   |1832        |295       |21328               |210534      |0.008701682388592816 |0.013831582895723931|
+-----+------------+----------+--------------------+------------+---------------------+--------------------+
 */

// Top 3 states with highest small businesses served percentage
state_ratio
  .orderBy($"ServedRatio".desc)
  .show(3,false)

/*
+-----+------------+----------+--------------------+------------+------------------+------------------+
|state|JobsRetained|LoanCounts|small_business_count|employ_count|RetainedRatio     |ServedRatio       |
+-----+------------+----------+--------------------+------------+------------------+------------------+
|MS   |412539      |45813     |43279               |439244      |0.9392023567766435|1.0585503361907622|
|NE   |327613      |42498     |42409               |411977      |0.7952215779036208|1.002098611143861 |
|HI   |225436      |24534     |24657               |273973      |0.8228402068817   |0.9950115585837693|
+-----+------------+----------+--------------------+------------+------------------+------------------+
 */

// Top 3 states with lowest small business served percentage
state_ratio
  .orderBy($"ServedRatio".asc)
  .show(3,false)

/*
+-----+------------+----------+--------------------+------------+---------------------+--------------------+
|state|JobsRetained|LoanCounts|small_business_count|employ_count|RetainedRatio        |ServedRatio         |
+-----+------------+----------+--------------------+------------+---------------------+--------------------+
|AK   |1           |1         |16408               |141147      |7.084812287898432E-6 |6.094588005850804E-5|
|MT   |474         |96        |31557               |245411      |0.0019314537653161432|0.003042114269417245|
|WY   |2389        |209       |17158               |131499      |0.018167438535654265 |0.012180906865602052|
+-----+------------+----------+--------------------+------------+---------------------+--------------------+
 */

// ToDo: Export dataframe containing RetainedRatio and ServedRatio to CSV for visualizing
state_ratio.write.parquet("E:\\IntelliJ-IdeaProjects\\CARES_paycheck_protection_program\\src\\main\\scala\\sba_ppp.parquet")

spark.stop()

