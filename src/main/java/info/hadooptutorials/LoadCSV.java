package info.hadooptutorials;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class LoadCSV {
  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("LoadCSV")
      .getOrCreate();

    StructType schema = new StructType()
.add("policyID", "string")
.add("statecode", "string")
.add("county", "string")
.add("eq_site_limit", "long")
.add("hu_site_limit", "long")
.add("fl_site_limit", "long")
.add("fr_site_limit", "long")
.add("tiv_2011", "long")
.add("tiv_2012", "long")
.add("eq_site_deductible", "long")
.add("hu_site_deductible", "long")
.add("fl_site_deductible", "long")
.add("fr_site_deductible", "long")
.add("point_latitude", "string")
.add("point_longitude", "string")
.add("line", "string")
.add("construction", "string")
.add("point_granularity", "long");

    Dataset<Row> df = spark.read()
    .option("mode", "DROPMALFORMED")
    .schema(schema)
    .csv("/projects/SparkLoadCSV/FL_insurance_sample.csv");    
    //.csv("hdfs:///localhost/SparkLoadCSV/FL_insurance_sample.csv");    

    df.createOrReplaceTempView("insurance");

    Dataset<Row> sqlResult = spark.sql(
        "SELECT statecode, county, SUM(hu_site_limit) as TotalSiteLimit, COUNT(policyID) as NoOfPolicies" 
            + " FROM insurance GROUP BY statecode, county order by NoOfPolicies DESC");
    
    sqlResult.show(1000,false); //for testing

sqlResult = spark.sql(
        "SELECT count(*)" 
            + " FROM insurance");
    
    sqlResult.show(1000,false); //for testing


    }
}
