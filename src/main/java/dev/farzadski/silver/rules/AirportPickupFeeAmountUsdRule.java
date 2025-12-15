package dev.farzadski.silver.rules;

import dev.farzadski.core.rules.IRule;
import dev.farzadski.silver.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class AirportPickupFeeAmountUsdRule implements IRule<Dataset<Row>> {

  @Override
  public Dataset<Row> apply(Dataset<Row> input) {

    String col = ColumnName.AIRPORT_PICKUP_FEE_AMOUNT_USD.canonical();
    String missingCol = ColumnName.IS_AIRPORT_PICKUP_FEE_AMOUNT_USD_MISSING.canonical();

    return input
        // create missing indicator as 0/1
        .withColumn(
            missingCol,
            functions
                .when(functions.col(col).isNull(), functions.lit(1))
                .otherwise(functions.lit(0)))
        // fill nulls with 0
        .withColumn(col, functions.coalesce(functions.col(col), functions.lit(0.0)));
  }
}
