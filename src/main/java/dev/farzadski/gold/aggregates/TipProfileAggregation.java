package dev.farzadski.gold.aggregates;

import static org.apache.spark.sql.functions.*;

import dev.farzadski.core.aggregate.IAggregation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TipProfileAggregation implements IAggregation {

  @Override
  public String name() {
    return "TIP_PROFILE";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/recommender/TIP_PROFILE.parquet";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {
    Dataset<Row> tips =
        silver
            .filter(col("BASE_FARE_AMOUNT_USD").gt(0))
            .withColumn("TIP_PCT", col("TIP_AMOUNT_USD").divide(col("BASE_FARE_AMOUNT_USD")))
            .withColumn("HOUR", hour(col("METER_ENGAGED_AT")))
            .withColumn("DOW", dayofweek(col("METER_ENGAGED_AT")));

    return tips.groupBy("PICKUP_TAXI_ZONE_ID", "HOUR", "DOW")
        .agg(
            avg("TIP_PCT").alias("AVG_TIP_PCT"),
            expr("percentile_approx(TIP_PCT, 0.75)").alias("HIGH_TIP_ZONE"));
  }
}
