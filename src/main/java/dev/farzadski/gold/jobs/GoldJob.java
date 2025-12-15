package dev.farzadski.gold.jobs;

import dev.farzadski.core.aggregate.IAggregation;
import dev.farzadski.core.jobs.IJob;
import dev.farzadski.gold.aggregates.*;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GoldJob implements IJob {
  private final SparkSession spark;

  public GoldJob(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public String name() {
    return "GOLD_TAXI_STANDARDIZATION_JOB";
  }

  @Override
  public void run() {
    Dataset<Row> df = spark.read().parquet("data/YellowTaxiTripRecord/silver/silver.parquet");
    List<IAggregation> aggregations =
        List.of(
            new TripFactHourlyAggregation(),
            new TripFactDailyAggregation(),
            new ZoneDemandHourlyAggregation(),
            new DemandVolatilityDailyAggregation(),
            new DemandVolatilityHourlyAggregation(),
            new RevenueVolatilityDailyAggregation(),
            new RevenueVolatilityWeeklyAggregation());

    GoldAggregationRunner runner = new GoldAggregationRunner(aggregations);
    runner.run(df);
  }
}
