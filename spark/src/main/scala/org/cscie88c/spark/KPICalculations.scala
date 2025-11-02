package org.cscie88c.spark

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window

object KPICalculations {

  // City Planner KPIs

  def peakHourTripPercentage(
      preparedData: DataFrame
  )(implicit spark: SparkSession): Dataset[PeakHourTripPercentage] = {
    import spark.implicits._

    val peakHours = F.hour(F.col("pickup_timestamp")).isin(7, 8, 9, 17, 18, 19)

    val resultDF = preparedData
      .groupBy("pickup_borough")
      .agg(
        F.sum(F.when(peakHours, 1).otherwise(0)).as("peak_hour_trips"),
        F.count("*").as("total_trips")
      )
      .withColumn(
        "peakHourPercentage",
        (F.col("peak_hour_trips") / F.col("total_trips")) * 100
      )
      .select(
        F.col("pickup_borough").as("borough"),
        F.col("peakHourPercentage")
      )
      .filter(F.col("borough").isNotNull)

    resultDF.as[PeakHourTripPercentage]
  }

  def weeklyTripVolumeByBorough(
      preparedData: DataFrame
  )(implicit spark: SparkSession): DataFrame = { // Returns DataFrame for anomaly detection
    import spark.implicits._

    preparedData
      .groupBy("pickup_borough", "pickup_zone", "pickup_service_zone", "week")
      .count()
      .select(
        F.col("pickup_borough").as("borough"),
        F.col("pickup_zone").as("zone"),
        F.col("pickup_service_zone").as("service_zone"),
        F.col("week"),
        F.col("count").as("tripVolume")
      )
      .filter(F.col("borough").isNotNull)
  }

  def avgTripTimeVsDistance(
      preparedData: DataFrame
  )(implicit spark: SparkSession): Dataset[AvgTripTimeVsDistance] = {
    import spark.implicits._

    preparedData
      .groupBy("PULocationID", "DOLocationID")
      .agg(
        F.avg("trip_duration_minutes").as("avgTripTimeMinutes"),
        F.avg("trip_distance").as("avgTripDistance")
      )
      .as[AvgTripTimeVsDistance]
  }

  // Fleet Operator KPIs

  def weeklyTotalTripsAndRevenue(
      preparedData: DataFrame
  )(implicit spark: SparkSession): Dataset[WeeklyTotalTripsAndRevenue] = {
    import spark.implicits._

    preparedData
      .groupBy("week")
      .agg(
        F.count("*").as("totalTrips"),
        F.sum("total_amount").as("totalRevenue")
      )
      .as[WeeklyTotalTripsAndRevenue]
  }

  def avgRevenuePerMileByBorough(
      preparedData: DataFrame
  )(implicit spark: SparkSession): Dataset[AvgRevenuePerMileByBorough] = {
    import spark.implicits._

    preparedData
      .filter(F.col("trip_distance") > 0)
      .groupBy("pickup_borough", "pickup_zone", "pickup_service_zone")
      .agg(
        (F.sum("total_amount") / F.sum("trip_distance"))
          .as("avgRevenuePerMile")
      )
      .select(
        F.col("pickup_borough").as("borough"),
        F.col("pickup_zone").as("zone"),
        F.col("pickup_service_zone").as("service_zone"),
        F.col("avgRevenuePerMile")
      )
      .filter(F.col("borough").isNotNull)
      .as[AvgRevenuePerMileByBorough]
  }

  def nightTripPercentageByBorough(
      preparedData: DataFrame
  )(implicit spark: SparkSession): Dataset[NightTripPercentageByBorough] = {
    import spark.implicits._

    // Night hours defined as 10 PM (22) to 4 AM (4)
    val nightHours =
      F.hour(F.col("pickup_timestamp")).isin(22, 23, 0, 1, 2, 3, 4)

    preparedData
      .groupBy("pickup_borough", "pickup_zone", "pickup_service_zone")
      .agg(
        F.sum(F.when(nightHours, 1).otherwise(0)).as("night_trips"),
        F.count("*").as("total_trips")
      )
      .withColumn(
        "nightTripPercentage",
        (F.col("night_trips") / F.col("total_trips")) * 100
      )
      .select(
        F.col("pickup_borough").as("borough"),
        F.col("pickup_zone").as("zone"),
        F.col("pickup_service_zone").as("service_zone"),
        F.col("nightTripPercentage")
      )
      .filter(F.col("borough").isNotNull)
      .as[NightTripPercentageByBorough]
  }

  // Anomaly Detection
  def detectAnomalies(weeklyVolume: DataFrame): DataFrame = {
    val windowSpec =
      Window.partitionBy("borough", "zone", "service_zone").orderBy("week")

    val weeklyVolumeWithStats = weeklyVolume
      .withColumn("avg_volume", F.avg("tripVolume").over(windowSpec))
      .withColumn("stddev_volume", F.stddev("tripVolume").over(windowSpec))

    weeklyVolumeWithStats
      .withColumn(
        "is_anomaly",
        // If stddev is null or 0 (e.g., only one data point), it's not an anomaly.
        F.when(
          F.col("stddev_volume").isNull || F.col("stddev_volume") === 0,
          false
        ).otherwise(
          F.abs(F.col("tripVolume") - F.col("avg_volume")) > (F.lit(2) * F
            .col("stddev_volume"))
        )
      )
      .na
      .fill(
        0.0,
        Seq("stddev_volume")
      ) // Replace null stddev with 0.0 for cleaner output
  }
}
