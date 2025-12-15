package dev.farzadski.silver.rules;

import dev.farzadski.core.rules.IRule;
import dev.farzadski.silver.enums.ColumnName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class StoredBeforeTransmissionFlagRule implements IRule<Dataset<Row>> {

  @Override
  public Dataset<Row> apply(Dataset<Row> input) {

    String flagCol = ColumnName.STORED_BEFORE_TRANSMISSION_FLAG.canonical();
    String missingCol = ColumnName.IS_STORED_BEFORE_TRANSMISSION_FLAG_MISSING.canonical();

    return input
        // preserve missingness as 0/1
        .withColumn(
            missingCol,
            functions
                .when(functions.col(flagCol).isNull(), functions.lit(1))
                .otherwise(functions.lit(0)))
        // normalize NULL → 'N'
        .withColumn(
            flagCol,
            functions
                .when(functions.col(flagCol).isNull(), functions.lit("N"))
                .otherwise(functions.col(flagCol)));
  }
}
