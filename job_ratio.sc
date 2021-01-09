
// ToDo: Check which CDs are in the 150kplus vs up_to_150k CDsk using the two lists
// Done: the diff1_CD below shows the additional CDs in 150kplus not in upto150k
// ToDo: Use neighboring CD to compute the loanAmount/jobsRetained ratio

// ToDo: Obtain the number of jobs per CD and using the average Loan Amount/Job in CD extrapolate loan amount
// ToDo: Get all the CD from the 150plus and see of they are present in the less150


// Creating an instance of delta table using path to saved delta table
// val deltaTable = DeltaTable.forPath(spark, path)
/*
ToDo: Analysis to inquire if all Congressional Districts represented in
loans up to (<) 150k are represented in the (>) 150k plus loans
 */

// Create a list of Congressional Districts > 150k loans
val ppp150kplus_CD = ppp_DF.select($"CD")
  .where($"LoanRange" =!= "f $0-149,999")
  .map(r => r.getString(0))
  .distinct()
  .collect()
  .toList


// Create a list of all the Congressional Districts
val ppp_up_to_150k_CD = ppp_DF
  .select($"CD")
  .where($"LoanRange" === "f $0-149,999")
  .map(r => r.getString(0))
  .distinct()
  .collect()
  .toList

// Congressional Districts in >150k loans not having ratio
val diff_CD = ppp150kplus_CD.diff(ppp_up_to_150k_CD)
//List(PR - 09, WA - 21, AL - 10, UT - 12, FL - 29, AL - 13, AZ - 00, OK - 11, PR - 30, NJ - 00)

/*



// Approximating loan amounts with average loan/jobs ratio of CD
val loan_jobs_ratio = ppp_DF
  .select($"LoanAmount", $"JobsRetained")
  .na.drop()  // dropping any rows with null values
  .where($"LoanRange" === "f $0-149,999")
  .agg(
    mean($"LoanAmount").as("AverageLoanAmount"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .withColumn("LoanJobRatio", $"AverageLoanAmount" / $"AverageJobsRetained")

loan_jobs_ratio.show(false)
/*
+-----------------+-------------------+-----------------+
|AverageLoanAmount|AverageJobsRetained|LoanJobRatio     |
+-----------------+-------------------+-----------------+
|33568.83289917501|4.656399718761318  |7209.181970336707|
+-----------------+-------------------+-----------------+
 */


/*
// Creating an instance of delta table using path to saved delta table
val deltaTable = DeltaTable.forPath(spark, path)

val ratioCalc = (jobs: Long) => {
  val ratio = loan_jobs_ratio
    .select($"LoanJobRatio")
    .collect().toList(0).getDouble(0)
  ratio * jobs
}


// Approximating loan amounts with average loan/jobs ratio of CD
val cd_jobs_loans = ppp_DF
  .select($"CD", $"NAICSCode", $"LoanAmount", $"JobsRetained")
  .na.drop()  // dropping any rows with null values
  .where($"LoanRange" === "f $0-149,999")
  .groupBy($"CD", $"NAICSCode")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"LoanAmount").as("AverageLoanAmount"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .withColumn("LoanJobRatio", $"AverageLoanAmount" / $"AverageJobsRetained")
  .orderBy($"CD")


// Creating an instance of delta table using path to saved delta table
val deltaTable = DeltaTable.forPath(spark, path)

val ratioCalc = (CD: String, naics:String, jobs: Long) => {
  val ratio = cd_jobs_loans
    .select($"LoanJobRatio")
    .where($"CD" === CD && $"NAICSCode" === naics)
    .collect().toList(0).getDouble(0)
  ratio * jobs
}

val testRatioCalc = ratioCalc("AK - 00", "483212", 2)


spark.udf.register("ratioCalc", ratioCalc)


// ToDo: After calculating the null values above, remove for caculations
// ToDo: Develop a try exempt option for the update clause below
// ToDo: Approximate the loan amount by using the LoanJobRatio for Jobs/NAICS/CD

deltaTable
  .update(
    condition = expr("LoanRange != 'f $0-149,999' "), //and JobsRetained is not null
    set = Map("LoanAmount" -> expr("JobsRetained * 7209.18"))
    //set = Map("LoanAmount" -> expr("ratioCalc(JobsRetained)"))
    //set = Map("LoanAmount" -> expr("ratioCalc(CD, NAICSCode, JobsRetained)"))
  )

val updated_ppp_DF = deltaTable.toDF

// delta table history
println("Describe History for the table")
deltaTable.history().show(false)

/*

//ToDO: Amount of loans only provided in LoanRange f, jobs retained, average/NAICS/CD. Note only LoanRange f
val cd_naics_loans = ppp_DF
  .select($"CD", $"LoanAmount", $"NAICSCode", $"JobsRetained")
  .na.drop()  // dropping any rows with null values
  .where($"LoanRange" === "f $0-149,999")
  .groupBy($"CD", $"NAICSCode")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"LoanAmount").as("AverageLoanAmount"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .withColumn("LoanJobRatio", $"AverageLoanAmount" / $"AverageJobsRetained")
  .orderBy($"CD")

cd_naics_loans.show(50, false)


// User-defined function to
val ratioCalc = (CD: String, naics: String, jobs: Long) => {
  val ratio = cd_naics_loans
    .select($"LoanJobRatio")
    .where($"CD" === CD && $"NAICSCode" === naics)
    .collect().toList(0).getDouble(0)
  ratio * jobs
}

spark.udf.register("ratioCalc", ratioCalc)

val testRatioCalc = ratioCalc("AK - 00", "483212", 2)
// ToDo: After calculating the null values above, remove for caculations
// ToDo: Develop a try exempt option for the update clause below
// ToDo: Approximate the loan amount by using the LoanJobRatio for Jobs/NAICS/CD
deltaTable
  .update(
    condition = expr("LoanRange != 'f $0-149,999'"),
    set = Map("LoanAmount" -> expr("ratioCalc(CD, NAICSCode, JobsRetained)"))
  )

val updated_ppp_DF = deltaTable.toDF

// delta table history
println("Describe History for the table")
deltaTable.history().show(false)
*/

*
// Analytical computations for NAICS Code loans, mean of loan, jobs retained and loan job ratio
val naics_ppp_up_to_150kDF = ppp_DF
  .select($"NAICSCode", $"LoanAmount", $"JobsRetained")
  .where($"LoanRange" === "f $0-149,999")
  .groupBy($"NAICSCode")
  .agg(
    sum($"LoanAmount").as("LoansAmount"),
    mean($"LoanAmount").as("AverageLoanAmount"),
    sum($"JobsRetained").as("JobsRetained"),
    count(lit(1)).as("LoanCounts")
  )
  .withColumn("LoanJobRatio", $"LoansAmount" / $"JobsRetained")
  .sort($"LoanJobRatio".desc)

naics_ppp_up_to_150kDF.show(20, false)

+---------+--------------------+------------------+------------+----------+------------------+
|NAICSCode|LoansAmount         |AverageLoanAmount |JobsRetained|LoanCounts|LoanJobRatio      |
+---------+--------------------+------------------+------------+----------+------------------+
|339932   |68702.0             |34351.0           |1           |2         |68702.0           |
  |315231   |56202.0             |56202.0           |1           |1         |56202.0           |
  |525930   |73354.0             |36677.0           |2           |2         |36677.0           |
  |314129   |27750.0             |27750.0           |1           |1         |27750.0           |
  |311711   |97169.68            |97169.68          |4           |1         |24292.42          |
  |235810   |67600.0             |33800.0           |3           |2         |22533.333333333332|
  |485112   |294112.0            |32679.11111111111 |14          |9         |21008.0           |
  |454319   |19645.0             |19645.0           |1           |1         |19645.0           |
  |333210   |252800.0            |42133.333333333336|13          |6         |19446.153846153848|
  |235610   |247110.85           |41185.14166666667 |13          |6         |19008.526923076923|
  |311823   |18700.0             |18700.0           |1           |1         |18700.0           |
  |421120   |37200.0             |37200.0           |2           |1         |18600.0           |
  |421510   |143400.0            |71700.0           |8           |2         |17925.0           |
  |333242   |3252753.41          |57065.849298245616|184         |57        |17678.007663043478|
  |221113   |560199.1            |40014.22142857143 |32          |14        |17506.221875      |
  |332213   |16592.0             |16592.0           |1           |1         |16592.0           |
  |325131   |113827.0            |113827.0          |7           |1         |16261.0           |
  |541710   |942488.47           |72499.11307692308 |58          |13        |16249.80120689655 |
  |541715   |7.462726267E7       |41575.076696378834|4627        |1795      |16128.649809811974|
  |541830   |1.0042421719999999E7|32605.26532467532 |625         |308       |16067.874751999998|
  +---------+--------------------+------------------+------------+----------+------------------+
only showing top 20 rows
*/

val updatedLoansDF = updated_ppp_DF
  .select($"LoanRange", $"LoanAmount", $"JobsRetained")
  .groupBy($"LoanRange")
  .agg(
    count(lit(1)).as("LoanCounts"),
    sum($"LoanAmount").as("LoanAmounts"),
    mean($"LoanAmount").as("AverageLoan"),
    sum($"JobsRetained").as("JobsRetained"),
    mean($"JobsRetained").as("AverageJobsRetained")
  )
  .orderBy($"LoanRange")

updatedLoansDF.show(false)

/*
Using the national average of loans to jobs retained for LoanRanges -> too low
+--------------------+----------+--------------------+------------------+------------+-------------------+
|LoanRange           |LoanCounts|LoanAmounts         |AverageLoan       |JobsRetained|AverageJobsRetained|
+--------------------+----------+--------------------+------------------+------------+-------------------+
|a $5-10 million     |4840      |1.181819621267999E10|2441776.076999998 |1639326     |338.70371900826444 |
|b $2-5 million      |24838     |3.725591760791993E10|1499956.4219309094|5167844     |208.06200177147917 |
|c $1-2 million      |53030     |4.258314116891997E10|803000.9649051474 |5906794     |111.38589477654158 |
|d $350,000-1 million|199456    |7.220411902439871E10|362005.24940036255|10015580    |50.21448339483395  |
|e $150,000-350,000  |379054    |6.291429037541835E10|165977.117707288  |8726969     |23.02302310488743  |
|f $0-149,999        |4224170   |1.418004568677081E11|33568.83289917501 |19669424    |4.656399718761318  |
+--------------------+----------+--------------------+------------------+------------+-------------------+
 */