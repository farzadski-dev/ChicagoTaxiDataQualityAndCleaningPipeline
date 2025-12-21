package dev.farzadski.gold.jobs;

import dev.farzadski.core.aggregate.IAggregation;
import dev.farzadski.core.aggregate.IAggregationRunner;
import dev.farzadski.shared.profiling.SchemaJsonGenerator;
import java.util.ArrayList;
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
    List<String> _ls = new ArrayList<>();

    for (IAggregation agg : aggregations) {
      Dataset<Row> result = agg.aggregate(silver);
      System.out.println(agg.name() + " : ");
      _ls.addAll(List.of(result.columns()));
      result.show(500, false);
      if (agg.isNeedToSaved()) {
        GoldWriter.write(result, agg.outputPath());
      }
    }

    SchemaJsonGenerator.generate(_ls, "data/YellowTaxiTripRecord/gold/_schema.json");
  }
}
