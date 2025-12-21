package dev.farzadski.gold.aggregates;

import static org.apache.spark.sql.functions.*;

import dev.farzadski.core.aggregate.IAggregation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class FleetSupplyDemandAggregation implements IAggregation {

  @Override
  public String name() {
    return "FLEET_SUPPLY_DEMAND";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/recommender/SUPPLY_DEMAND.parquet";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {

    return silver
        .withColumn("HOUR", hour(col("METER_ENGAGED_AT")))
        .withColumn("DAY_OF_WEEK", dayofweek(col("METER_ENGAGED_AT")))
        .groupBy("HOUR", "DAY_OF_WEEK")
        .agg(count(lit(1)).alias("TRIP_DEMAND"), sum("TOTAL_CHARGE_AMOUNT_USD").alias("TOTAL_REV"));
  }
}
