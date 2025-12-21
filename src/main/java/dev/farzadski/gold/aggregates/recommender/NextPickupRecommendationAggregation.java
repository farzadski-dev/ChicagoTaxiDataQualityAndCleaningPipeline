package dev.farzadski.gold.aggregates.recommender;

import static org.apache.spark.sql.functions.*;

import dev.farzadski.core.aggregate.IAggregation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class NextPickupRecommendationAggregation implements IAggregation {

  @Override
  public String name() {
    return "NEXT_PICKUP_RECOMMENDATION";
  }

  @Override
  public String outputPath() {
    return "data/YellowTaxiTripRecord/gold/recommender/NEXT_PICKUP_RECOMMENDATION.parquet";
  }

  @Override
  public Boolean isNeedToSaved() {
    return false;
  }

  @Override
  public Dataset<Row> aggregate(Dataset<Row> silver) {

    Dataset<Row> base =
        silver
            .withColumn("PICKUP_TS", col("METER_ENGAGED_AT"))
            .withColumn("DROPOFF_TS", col("METER_DISENGAGED_AT"))
            .withColumn("HOUR", hour(col("DROPOFF_TS")))
            .withColumn("DOW", dayofweek(col("DROPOFF_TS")));

    WindowSpec w =
        Window.partitionBy(col("DROP_OFF_TAXI_ZONE_ID"), col("HOUR"), col("DOW"))
            .orderBy(col("PICKUP_TS"));

    Dataset<Row> idle =
        base.withColumn("NEXT_PICKUP_TS", lead("PICKUP_TS", 1).over(w))
            .withColumn(
                "IDLE_MIN",
                unix_timestamp(col("NEXT_PICKUP_TS"))
                    .minus(unix_timestamp(col("DROPOFF_TS")))
                    .divide(60))
            .filter(col("IDLE_MIN").between(0, 30));

    return idle.groupBy("DROP_OFF_TAXI_ZONE_ID", "PICKUP_TAXI_ZONE_ID", "HOUR", "DOW")
        .agg(
            avg("IDLE_MIN").alias("AVG_IDLE_MIN"),
            expr("percentile_approx(IDLE_MIN, 0.25)").alias("FAST_PICKUP_MIN"),
            count(lit(1)).alias("OBS"))
        .withColumn("PICKUP_PROB_5MIN", col("FAST_PICKUP_MIN").leq(5).cast("int"));
  }
}
