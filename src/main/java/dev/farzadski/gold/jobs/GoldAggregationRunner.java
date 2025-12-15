package dev.farzadski.gold.jobs;

import dev.farzadski.core.aggregate.IAggregation;
import dev.farzadski.core.aggregate.IAggregationRunner;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class GoldAggregationRunner implements IAggregationRunner {
  private final List<IAggregation> aggregations;

  public GoldAggregationRunner(List<IAggregation> aggregations) {
    this.aggregations = aggregations;
  }

  @Override
  public void run(Dataset<Row> silver) {
    for (IAggregation agg : aggregations) {
      Dataset<Row> result = agg.aggregate(silver);
      System.out.println(agg.name() + " : ");
      result.show(20, false);
      if (agg.isNeedToSaved()) {
        GoldWriter.write(result, agg.outputPath());
      }
    }
  }
}
