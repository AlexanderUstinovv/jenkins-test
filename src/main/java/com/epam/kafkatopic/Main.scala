package com.epam.kafkatopic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, DoubleType, IntegerType, LongType, StringType, StructType, TimestampType}

import org.elasticsearch.spark.sql._


object Main {

  def main(args: Array[String]): Unit = {

    val schemaForFile = new StructType()
      .add("id",LongType)
      .add("date_time", BinaryType)
      .add("posa_continent",IntegerType)
      .add("user_location_country",IntegerType)
      .add("user_location_region",IntegerType)
      .add("user_location_city",IntegerType)
      .add("orig_destination_distance",DoubleType)
      .add("user_id",IntegerType)
      .add("is_mobile",IntegerType)
      .add("is_package",IntegerType)
      .add("channel",IntegerType)
      .add("srch_ci",BinaryType)
      .add("srch_co",BinaryType)
      .add("srch_adults_cnt",IntegerType)
      .add("srch_children_cnt",IntegerType)
      .add("srch_rm_cnt",IntegerType)
      .add("srch_destination_id",IntegerType)
      .add("srch_destination_type_id",IntegerType)
      .add("hotel_id",LongType)
      .add("timestp",LongType)
      .add("idle",LongType)

    val sparkSession = getSparkSession

    val expedia = sparkSession.read
      .parquet("hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/valid_expedia/year=2016")

    val streamingExpedia = sparkSession.readStream.schema(schemaForFile)
      .parquet("hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/valid_expedia/year=2017")

    val hotelsTopicRawDataRdd = sparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "hotelsWeather1")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .rdd.map(_.getString(0))

    val hotelsDf = sparkSession.read.json(hotelsTopicRawDataRdd)

    hotelsDf.show(5, false)

    val joinedExpedia = expedia.join(hotelsDf, hotelsDf("id") === expedia("hotel_id")).drop(hotelsDf("id"))

    val joinedStreamingExpedia = streamingExpedia.join(hotelsDf, hotelsDf("id") === streamingExpedia("hotel_id")).drop(hotelsDf("id"))

    //Filter incoming data by having average temperature more than 0 Celsius degrees.

    val joinedExpediaFiltered = joinedExpedia.filter(col("avg_tmpr_c").gt(0))
    val joinedStreamingExpediaFiltered = joinedStreamingExpedia.filter(col("avg_tmpr_c").gt(0))

    val hotels = joinedExpediaFiltered.withColumn("type_stay",
      when(col("idle").cast("bigint").divide(86400) === 1, "Short stay")
        .when(col("idle").cast("bigint").divide(86400) >= 2 && col("idle") <= 7, "Standart stay")
        .when(col("idle").cast("bigint").divide(86400) > 7 && col("idle") <= 14, "Standart extended stay")
        .when(col("idle").cast("bigint").divide(86400) > 14 && col("idle") <= 28, "Long stay")
        .when(col("idle") === null || col("idle").cast("bigint").divide(86400) > 30
          || col("idle").cast("bigint").divide(86400) <= 0, "Erroneous data")
    )

    val streamedHotels = joinedStreamingExpediaFiltered.withColumn("type_stay",
      when(col("idle").divide(86400) === 1, "Short stay")
        .when(col("idle").divide(86400) >= 2 && col("idle").divide(86400) <= 7, "Standart stay")
        .when(col("idle").divide(86400) > 7 && col("idle").divide(86400) <= 14, "Standart extended stay")
        .when(col("idle").divide(86400) > 14 && col("idle").divide(86400) <= 28, "Long stay")
        .when(col("idle") === null || col("idle").divide(86400) > 30
          || col("idle").divide(86400) <= 0, "Erroneous data")
    )

    val hotelsRollup = hotels.rollup("idle").sum("idle")

    val streamedHotelsRollup = streamedHotels.rollup("idle", "srch_children_cnt").sum("idle", "srch_children_cnt")

    hotels.saveToEs("spark/hotels")

    streamedHotels.saveToEs("spark/streamedHotels")

    hotelsRollup.show(10, false)
    streamedHotelsRollup.show(10, false)
  }

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .appName("kafka-reader")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "10.8.0.2")
      .config("es.port", "9200")
      .config("es.http.timeout", "5m")
      .config("es.scroll.size", "50")
      .getOrCreate()
  }
}