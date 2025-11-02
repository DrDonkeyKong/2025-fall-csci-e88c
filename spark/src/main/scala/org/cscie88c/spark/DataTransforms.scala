package org.cscie88c.spark

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

case class TaxiZone(
    LocationID: Int,
    Borough: String,
    Zone: String,
    service_zone: String
)

object DataTransforms {

  // Assume taxi_zone_lookup.csv has columns: "LocationID","Borough","Zone","service_zone"
  def loadZoneData(
      filePath: String
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[TaxiZone]
      .toDF()
  }

  def prepareDataForKPIs(
      tripData: DataFrame,
      zoneData: DataFrame
  ): DataFrame = {
    val pickupZoneDF = zoneData
      .withColumnRenamed("Borough", "pickup_borough")
      .withColumnRenamed("Zone", "pickup_zone")
      .withColumnRenamed("service_zone", "pickup_service_zone")

    val dropoffZoneDF = zoneData
      .withColumnRenamed("Borough", "dropoff_borough")
      .withColumnRenamed("Zone", "dropoff_zone")
      .withColumnRenamed("service_zone", "dropoff_service_zone")

    tripData
      // Cast timestamps and calculate trip duration in minutes
      .withColumn(
        "pickup_timestamp",
        F.to_timestamp(F.col("tpep_pickup_datetime"))
      )
      .withColumn(
        "dropoff_timestamp",
        F.to_timestamp(F.col("tpep_dropoff_datetime"))
      )
      .withColumn(
        "trip_duration_minutes",
        (F.col("dropoff_timestamp")
          .cast("long") - F.col("pickup_timestamp").cast("long")) / 60
      )
      // Add week of year
      .withColumn("week", F.weekofyear(F.col("pickup_timestamp")))
      // Join with zone data to get pickup and dropoff boroughs
      .join(
        pickupZoneDF,
        tripData("PULocationID") === pickupZoneDF("LocationID"),
        "left_outer"
      )
      .join(
        dropoffZoneDF,
        tripData("DOLocationID") === dropoffZoneDF("LocationID"),
        "left_outer"
      )
  }

  def filterByWeek(
      data: DataFrame,
      startWeek: Option[Int],
      endWeek: Option[Int]
  ): DataFrame = {
    var filteredData = data
    startWeek.foreach { week =>
      filteredData = filteredData.filter(F.col("week") >= week)
    }
    endWeek.foreach { week =>
      filteredData = filteredData.filter(F.col("week") <= week)
    }
    filteredData
  }

}
