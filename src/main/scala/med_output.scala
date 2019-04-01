import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object med_output {

  def main (args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Mortality Data")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

	    val eventsSchema = StructType(List(
	      StructField("patient_id", IntegerType, true),
	      StructField("event_id", StringType, true),
	      StructField("event_description", StringType, true),
	      StructField("timestamp", StringType, true),
	      StructField("value", StringType, true)
	    ))

    val mortalitySchema = StructType(List(
      StructField("patient_id", IntegerType, true),
      StructField("timeStamp", StringType, true),
      StructField("label", IntegerType, true)
    ))

    val statsSchema = StructType(List(
      StructField("stat", StringType, true),
      StructField("value", StringType, true)
    ))

    val eventsDF = spark.read.schema(eventsSchema).csv("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/med_input/events.csv")
    val mortalityDF = spark.read.schema(mortalitySchema).csv("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/med_input/mortality.csv")

    eventsDF.createOrReplaceTempView("events")
    mortalityDF.createOrReplaceTempView("mortality")

    val deadDF = spark.sql("SELECT * FROM events WHERE EXISTS (SELECT patient_id FROM mortality WHERE mortality.patient_id == events.patient_id)")
    val aliveDF = spark.sql("SELECT * FROM events WHERE NOT EXISTS (SELECT patient_id FROM mortality WHERE mortality.patient_id == events.patient_id)")

    val deadEventCount = deadDF.groupBy("patient_id").agg(count(deadDF.columns(1)))
    val aliveEventCount = aliveDF.groupBy("patient_id").agg(count(aliveDF.columns(1)))

    val deadStats = deadEventCount.agg(min(deadEventCount.columns(1)), max(deadEventCount.columns(1)),avg(deadEventCount.columns(1)) )
    val aliveStats = aliveEventCount.agg(min(aliveEventCount.columns(1)), max(aliveEventCount.columns(1)),avg(aliveEventCount.columns(1)) )


    deadStats.write.format("csv").save("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/med_output/deadStats.csv")
    aliveStats.write.format("csv").save("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/med_output/aliveStats.csv")

  }

}
