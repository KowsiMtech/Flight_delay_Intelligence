# Databricks notebook source
# MAGIC %run "/Flight_Analytics/00_setup/includes/config"

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, to_timestamp, trim, upper, when, lit,
    regexp_replace, coalesce, lag, unix_timestamp,
    current_timestamp, row_number, avg, round as spark_round
)
from pyspark.sql.types import IntegerType, FloatType, StringType
from pyspark.sql.window import Window



# COMMAND ----------

# MAGIC %md ## 1. Silver Flights - Cleaned and Validated

# COMMAND ----------

@dlt.table(
    name="silver_flights",
    comment="Cleaned and validated flight records from BTS data.",
    table_properties={"delta.enableChangeDataFeed": "true"}  # needed for downstream incremental reads
)
@dlt.expect_or_drop("valid_flight_id",   "flight_id IS NOT NULL")
@dlt.expect_or_drop("valid_origin",      "origin IS NOT NULL AND LENGTH(origin) = 3")
@dlt.expect_or_drop("valid_dest",        "dest IS NOT NULL AND LENGTH(dest) = 3")
@dlt.expect_or_drop("valid_flight_date", "flight_date IS NOT NULL")
@dlt.expect("reasonable_delay",          "arr_delay_minutes BETWEEN -60 AND 600")  # warn, don't drop
def silver_flights():
    return (
        dlt.read("bronze_flights")  # DLT reads from Bronze using the table name, not path
            # ── Cast and clean ──────────────────────────────────────────────
            .withColumn("flight_date",        to_timestamp(col("flight_date"), "yyyy-MM-dd"))
            .withColumn("origin",             trim(upper(col("origin"))))
            .withColumn("dest",               trim(upper(col("dest"))))
            .withColumn("carrier",            trim(upper(col("carrier"))))
            .withColumn("flight_num",         col("mkt_carrier_fl_num").cast(IntegerType()))
            .withColumn("dep_delay_minutes",  col("dep_delay").cast(FloatType()))
            .withColumn("arr_delay_minutes",  col("arr_delay").cast(FloatType()))
            .withColumn("cancelled",          col("cancelled").cast(IntegerType()))
            .withColumn("diverted",           col("diverted").cast(IntegerType()))
            .withColumn("air_time_minutes",   col("air_time").cast(FloatType()))
            .withColumn("distance_miles",     col("distance").cast(FloatType()))
            .withColumn("tail_number",        trim(upper(col("tail_number"))))

            # ── Delay reason flags ────────────────────────────────────────
            .withColumn("delay_carrier_min",  coalesce(col("carrier_delay").cast(FloatType()),  lit(0.0)))
            .withColumn("delay_weather_min",  coalesce(col("weather_delay").cast(FloatType()),  lit(0.0)))
            .withColumn("delay_nas_min",      coalesce(col("nas_delay").cast(FloatType()),      lit(0.0)))
            .withColumn("delay_security_min", coalesce(col("security_delay").cast(FloatType()), lit(0.0)))
            .withColumn("delay_aircraft_min", coalesce(col("late_aircraft_delay").cast(FloatType()), lit(0.0)))

            # ── Derived flags ─────────────────────────────────────────────
            .withColumn("is_delayed",  when(col("arr_delay_minutes") >= 15, 1).otherwise(0))
            .withColumn("is_cancelled", col("cancelled"))

            # ── Primary delay reason ──────────────────────────────────────
            .withColumn("primary_delay_reason",
                when(col("delay_carrier_min")  == col("arr_delay_minutes"), "CARRIER")
                .when(col("delay_weather_min") == col("arr_delay_minutes"), "WEATHER")
                .when(col("delay_nas_min")     == col("arr_delay_minutes"), "NAS")
                .when(col("delay_aircraft_min")== col("arr_delay_minutes"), "LATE_AIRCRAFT")
                .when(col("delay_security_min")== col("arr_delay_minutes"), "SECURITY")
                .otherwise("UNKNOWN")
            )

            # ── Select final columns ──────────────────────────────────────
            .select(
                "flight_date", "carrier", "flight_num", "tail_number",
                "origin", "dest", "distance_miles",
                "dep_delay_minutes", "arr_delay_minutes",
                "is_delayed", "is_cancelled", "diverted",
                "delay_carrier_min", "delay_weather_min", "delay_nas_min",
                "delay_security_min", "delay_aircraft_min",
                "primary_delay_reason", "air_time_minutes",
                "_ingested_at", "_source_file"
            )
    )

# COMMAND ----------

# MAGIC %md ## 2. Silver Weather - Cleaned

# COMMAND ----------

@dlt.table(
    name="silver_weather",
    comment="Cleaned hourly weather observations per airport."
)
@dlt.expect_or_drop("valid_airport_code", "airport_code IS NOT NULL AND LENGTH(airport_code) = 3")
@dlt.expect_or_drop("valid_obs_time",     "observation_time IS NOT NULL")
def silver_weather():
    return (
        dlt.read("bronze_weather")
            .withColumn("airport_code",      trim(upper(col("airport_code"))))
            .withColumn("observation_time",  to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm"))
            .withColumn("temperature_c",     col("temperature_2m").cast(FloatType()))
            .withColumn("precipitation_mm",  col("precipitation").cast(FloatType()))
            .withColumn("wind_speed_kmh",    col("windspeed_10m").cast(FloatType()))
            .withColumn("visibility_km",     col("visibility").cast(FloatType()))
            .withColumn("weather_code",      col("weathercode").cast(IntegerType()))

            # Classify weather severity
            .withColumn("weather_severity",
                when(col("weather_code").isin(95, 96, 99), "SEVERE")       # thunderstorm
                .when(col("weather_code").between(71, 77), "MODERATE")     # snow
                .when(col("weather_code").between(51, 67), "LIGHT")        # rain/drizzle
                .when(col("visibility_km") < 1.0, "LOW_VISIBILITY")
                .otherwise("CLEAR")
            )

            .select(
                "airport_code", "observation_time",
                "temperature_c", "precipitation_mm",
                "wind_speed_kmh", "visibility_km",
                "weather_code", "weather_severity",
                "_ingested_at"
            )
    )

# COMMAND ----------

# MAGIC %md ## 3. Silver Airports - Reference

# COMMAND ----------

@dlt.table(
    name="silver_airports",
    comment="Cleaned airport reference data with geo info."
)
@dlt.expect_or_drop("valid_iata", "iata_code IS NOT NULL AND LENGTH(iata_code) = 3")
def silver_airports():
    return (
        dlt.read("bronze_airports")
            .withColumn("iata_code",   trim(upper(col("iata_code"))))
            .withColumn("airport_name", trim(col("name")))
            .withColumn("city",         trim(col("municipality")))
            .withColumn("country",      trim(col("iso_country")))
            .withColumn("latitude",     col("latitude_deg").cast(FloatType()))
            .withColumn("longitude",    col("longitude_deg").cast(FloatType()))
            .withColumn("airport_type", trim(col("type")))
            .select(
                "iata_code", "airport_name", "city", "country",
                "latitude", "longitude", "airport_type",
                "_ingested_at"
            )
            .dropDuplicates(["iata_code"])
    )

# COMMAND ----------

# MAGIC %md ## 4. Silver Flight and Weather Join

# COMMAND ----------

@dlt.table(
    name="silver_flights_enriched",
    comment="Flights joined with weather at origin airport on flight date."
)
def silver_flights_enriched():
    flights = dlt.read("silver_flights")
    weather = dlt.read("silver_weather")
    airports = dlt.read("silver_airports")

    # Round weather observations to the hour for join
    from pyspark.sql.functions import date_trunc
    weather_hourly = weather.withColumn("obs_hour", date_trunc("hour", col("observation_time")))

    # Join flights with weather at origin on flight date
    enriched = (
        flights
        .join(
            weather_hourly.alias("wx"),
            (flights["origin"] == col("wx.airport_code")) &
            (date_trunc("hour", flights["flight_date"]) == col("wx.obs_hour")),
            "left"
        )
        .join(
            airports.alias("ap"),
            flights["origin"] == col("ap.iata_code"),
            "left"
        )
        .select(
            flights["*"],
            col("wx.temperature_c"),
            col("wx.precipitation_mm"),
            col("wx.wind_speed_kmh"),
            col("wx.visibility_km"),
            col("wx.weather_severity"),
            col("ap.city").alias("origin_city"),
            col("ap.airport_name").alias("origin_airport_name")
        )
    )
    return enriched

# COMMAND ----------

# MAGIC %md ## 5. Cascade Delay Detection

# COMMAND ----------

@dlt.table(
    name="silver_delay_cascades",
    comment="""
    Detects cascade delays: one late aircraft causes multiple downstream delays.
    Logic: same tail_number, same day, consecutive flights where arr_delay of flight N
    predicts dep_delay of flight N+1 (late_aircraft_delay > 0).
    """
)
def silver_delay_cascades():
    flights = dlt.read("silver_flights")

    # Window: per tail number, ordered by flight date
    w = Window.partitionBy("tail_number").orderBy("flight_date")

    cascades = (
        flights
        .filter(col("tail_number").isNotNull())
        .filter(col("is_cancelled") == 0)
        .withColumn("prev_arr_delay",    lag("arr_delay_minutes", 1).over(w))
        .withColumn("prev_flight_date",  lag("flight_date", 1).over(w))
        .withColumn("prev_dest",         lag("dest", 1).over(w))

        # Cascade condition: previous flight was late AND this flight has late_aircraft delay
        .filter(
            (col("delay_aircraft_min") > 0) &
            (col("prev_arr_delay") > 15) &
            (col("prev_dest") == col("origin"))   # previous dest = current origin (same aircraft routing)
        )
        .withColumn("cascade_delay_minutes", col("delay_aircraft_min"))
        .withColumn("caused_by_prev_arr_delay", col("prev_arr_delay"))

        .select(
            "tail_number", "flight_date", "carrier",
            "origin", "dest", "flight_num",
            "cascade_delay_minutes", "caused_by_prev_arr_delay",
            "prev_flight_date"
        )
    )
    return cascades