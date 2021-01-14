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

// Constructing file location of data store of Delta Lake table
val file = new File("/tables/ppp_final.delta") // Defaults to C:\, otherwise include <drive>:/
val path = file.getAbsolutePath // getCanonicalPath

// Loading Delta Lake table in Parquet format with latest version
println("Reading the table")
val ppp_DF = spark.read.format("delta").option("versionAsOf", 18).load(path)

// Verification of dataframe
ppp_DF.show(5)

// ToDo: Identify incorrectly identified Congressional Districts outside the state
val state_errors = ppp_DF
  .select($"State", $"Zip", $"CD")
  .where($"State" =!= $"CD".substr(0,2))

printf("Total incorrectly identified Congressional Districts: %d".format(state_errors.count()))
//Total incorrectly identified Congressional Districts: 414

state_errors.show(414, truncate = false)

spark.stop()

// ToDo: Configure methods to eliminate entries with no State, Zip and CD and manage
// partial information lookup zip code in a city somewhere in a website to identify state
/*
+-----+-----+-------+
|State|Zip  |CD     |
+-----+-----+-------+
|IL   |60585|IA - 01|
|IL   |60661|SC - 06|
|IL   |60661|IA - 02|
|IL   |62450|IN - 09|
|MA   |01775|MT - 01|
|VA   |17545|PA - 06|
|VA   |24614|WV - 03|
|VA   |23927|NC - 01|
|VA   |23513|GA - 05|
|VA   |20176|MD - 04|
|CO   |92503|CA - 41|
|CO   |92660|CA - 45|
|WA   |99016|MT - 01|
|WA   |99403|OR - 02|
|WA   |78414|TX - 27|
|WA   |98282|CA - 02|
|MN   |49506|MI - 03|
|TN   |37604|FL - 11|
|TN   |77598|TX - 14|
|TN   |37086|TX - 20|
|WI   |53711|FL - 21|
|AZ   |85268|PA - 14|
|AZ   |85377|FL - 07|
|MD   |21703|VA - 03|
|MD   |21014|SC - 02|
|MD   |01720|MA - 03|
|IN   |47802|IL - 15|
|IN   |46112|MI - 06|
|LA   |40503|KY - 06|
|LA   |70538|TX - 24|
|LA   |70119|MS - 03|
|SC   |29621|WY - 02|
|SC   |35209|AL - 06|
|SC   |30655|GA - 10|
|CA   |91730|WI - 04|
|CA   |91730|IL - 06|
|CA   |91730|WI - 02|
|CA   |93109|MA - 09|
|CA   |95062|HI - 02|
|OK   |74132|FL - 20|
|OK   |97219|OR - 01|
|CT   |30907|GA - 12|
|OR   |97401|MO - 03|
|KS   |67202|CO - 02|
|KS   |66061|NE - 03|
|KS   |66224|MO - 05|
|KY   |41017|AK - 01|
|KY   |42223|TN - 01|
|KY   |40202|OH - 07|
|KY   |41017|AK - 01|
|KY   |41011|OH - 01|
|KY   |40475|FL - 07|
|KY   |42223|TN - 01|
|KY   |41017|AK - 01|
|KY   |42223|TN - 01|
|KY   |42223|TN - 01|
|NV   |11702|NY - 02|
|NV   |89121|CA - 51|
|AR   |72764|MO - 03|
|AR   |72764|IL - 12|
|AR   |72757|TX - 04|
|AR   |72764|IL - 12|
|AR   |72764|IL - 13|
|AR   |72764|MS - 02|
|AR   |72764|IL - 15|
|AR   |72936|WY - 01|
|ID   |83712|NV - 02|
|NH   |63341|MO - 02|
|NM   |87110|CO - 03|
|NM   |87401|TX - 31|
|WV   |26062|PA - 12|
|WV   |26062|PA - 12|
|FL   |33166|NJ - 02|
|FL   |32162|OH - 06|
|FL   |32952|MA - 05|
|FL   |34293|MN - 01|
|FL   |30269|GA - 03|
|FL   |34104|OH - 12|
|FL   |31522|GA - 01|
|FL   |30605|GA - 09|
|FL   |30252|GA - 03|
|FL   |31636|GA - 08|
|FL   |30240|GA - 03|
|TX   |77064|AZ - 08|
|TX   |75204|CA -   |
|TX   |75217|GA - 05|
|NC   |27103|GA - 05|
|NJ   |07501|IL - 09|
|NJ   |08016|PA - 15|
|NM   |87111|OK - 05|
|KY   |41017|FL - 18|
|KY   |41017|MN - 03|
|KY   |41017|MI - 04|
|KY   |41017|AZ - 07|
|KY   |41017|TX - 16|
|KY   |41017|CT - 01|
|KY   |41017|AL - 06|
|KY   |41017|OH - 01|
|KY   |41017|OH - 01|
|MA   |02135|CA - 51|
|MA   |02111|MD - 07|
|MD   |21075|GA - 06|
|MD   |21704|FL - 20|
|MI   |48326|AR - 03|
|MI   |48083|VI - 00|
|MI   |48326|IL - 06|
|MI   |48326|TX - 01|
|MI   |48326|NC - 07|
|GA   |30024|TX - 10|
|GA   |32202|FL - 05|
|GA   |33952|FL - 17|
|IL   |60661|CA - 02|
|IL   |60661|MN - 04|
|IL   |60661|OH - 01|
|IL   |60661|TX - 32|
|IL   |60661|AR - 03|
|IL   |60661|GA - 09|
|IL   |60661|IN - 09|
|IL   |60661|IA - 02|
|IL   |60661|TN - 05|
|IL   |60661|MS - 01|
|IL   |60661|WA - 07|
|IL   |60661|TN - 05|
|IL   |46947|IN - 04|
|IL   |60661|MI - 12|
|IL   |60661|CA - 13|
|IL   |60661|VA - 05|
|IL   |60661|NE - 01|
|IL   |60661|VA - 03|
|IL   |60068|IN - 01|
|IL   |60661|CT - 03|
|IL   |60661|NC - 04|
|IL   |60661|TN - 02|
|IL   |60661|WI - 02|
|IL   |60661|AZ - 09|
|IL   |60661|CT - 02|
|IL   |60661|PA - 05|
|IL   |60423|IN - 01|
|IL   |60606|OH - 15|
|IL   |60661|OH - 10|
|IN   |62959|IL - 12|
|NY   |10309|NJ - 11|
|OH   |44131|NC - 01|
|OH   |45150|IN - 09|
|OH   |45150|KY - 04|
|OH   |43219|NC - 04|
|OH   |45404|WY - 01|
|OH   |45150|KY - 04|
|OH   |45202|FL - 19|
|OH   |45419|NC - 08|
|OH   |45405|SD - 01|
|OH   |45209|SC - 01|
|OH   |31904|GA - 02|
|OH   |15143|PA - 12|
|OH   |45150|IN - 06|
|OH   |48162|MI - 07|
|OH   |44515|PA - 03|
|OK   |73703|KS - 04|
|OR   |97239|CA - 06|
|OR   |97239|CA - 39|
|OR   |97239|CA - 06|
|OR   |97239|AZ - 01|
|OR   |97239|CA - 36|
|OR   |97239|CA - 08|
|OR   |97239|AZ - 07|
|PA   |19107|NJ - 03|
|RI   |02920|MA - 03|
|SC   |29047|FL - 03|
|TX   |77494|CA - 15|
|AL   |30269|GA - 03|
|CA   |92530|AZ - 04|
|GA   |33782|FL - 13|
|GA   |32520|FL - 01|
|GA   |31820|SC - 01|
|GA   |30339|OH - 03|
|GA   |35080|AL - 06|
|GA   |30339|TN - 03|
|GA   |22314|VA - 08|
|GA   |29365|SC - 04|
|NJ   |08701|NY - 03|
|OH   |43950|WV - 01|
|OH   |44907|CA - 49|
|OH   |60062|IL - 09|
|NC   |28134|SC - 05|
|NC   |28782|SC - 04|
|MI   |48326|IL - 16|
|MI   |48083|FL - 26|
|MI   |48326|TX - 16|
|CA   |90021|TX - 24|
|CO   |67401|KS - 01|
|CO   |80962|TX - 24|
|DC   |20001|LA - 04|
|DC   |20006|MD - 03|
|DE   |19711|PA - 08|
|TX   |79768|OK - 01|
|VA   |22301|DC - 01|
|WV   |25801|FL - 18|
|WV   |25801|FL - 18|
|WV   |26062|PA - 12|
|XX   |null |NA -   |
|XX   |98112|WA - 07|
|XX   |92407|CA - 08|
|XX   |29420|SC - 01|
|XX   |29150|SC - 05|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |32259|FL - 04|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|WY   |32837|FL - 10|
|DE   |32819|FL - 10|
|AK   |99507|TX - 15|
|VI   |00803|PR - 01|
|FI   |33069|FL - 20|
|XX   |null |NA -   |
|XX   |29456|SC - 01|
|XX   |29102|SC - 05|
|XX   |null |NA -   |
|XX   |48239|MI - 11|
|XX   |29303|SC - 04|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |91101|CA - 27|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |92014|CA - 49|
|XX   |91764|CA - 31|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |33408|FL - 18|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |90703|CA - 38|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |36106|AL - 02|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |92880|CA - 35|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |29492|SC - 01|
|XX   |33647|FL - 14|
|XX   |null |NA -   |
|XX   |01776|MA - 03|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |11501|NY - 03|
|XX   |07005|NJ - 11|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |11753|NY - 03|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |89135|NV - 03|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |90277|CA - 33|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |27410|NC - 06|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |33762|FL - 13|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |29078|SC - 02|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |06314|IL -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |03443|NH - 02|
|XX   |null |NA -   |
|XX   |05530|IL -   |
|XX   |null |NA -   |
|XX   |06074|CT - 01|
|XX   |03530|IL -   |
|XX   |01423|IL -   |
|XX   |02910|RI - 01|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |03800|CO -   |
|XX   |01812|MA - 06|
|XX   |08067|NJ - 02|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |03800|IL -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |05493|IL -   |
|XX   |02744|MA - 09|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |04924|ME - 02|
|XX   |02414|IL -   |
|XX   |null |NA -   |
|XX   |04787|ME - 02|
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |02006|IL -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
|XX   |null |NA -   |
+-----+-----+-------+
 */