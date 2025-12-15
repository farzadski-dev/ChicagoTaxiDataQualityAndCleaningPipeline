package dev.farzadski.bronze.rules;

import dev.farzadski.bronze.enums.ColumnName;
import dev.farzadski.core.rules.IRule;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class RenameColumnRule implements IRule<Dataset<Row>> {

  private final Set<ColumnName> columnsToRename;

  public RenameColumnRule() {
    this.columnsToRename = Set.of(ColumnName.values());
  }

  public RenameColumnRule(Set<ColumnName> columnsToRename) {
    this.columnsToRename = columnsToRename;
  }

  @Override
  public Dataset<Row> apply(Dataset<Row> dataset) {
    Dataset<Row> df = dataset;

    Set<String> existingColumns = Arrays.stream(df.columns()).collect(Collectors.toSet());

    for (ColumnName column : columnsToRename) {
      if (existingColumns.contains(column.raw())) {
        df = df.withColumnRenamed(column.raw(), column.canonical());
      }
    }

    return df;
  }
}
