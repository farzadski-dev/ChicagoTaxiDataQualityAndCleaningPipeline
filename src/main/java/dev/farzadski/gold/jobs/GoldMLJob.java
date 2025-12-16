package dev.farzadski.gold.jobs;

import static org.apache.spark.sql.functions.*;

import dev.farzadski.core.jobs.IJob;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

public class GoldMLJob implements IJob {
  private final SparkSession spark;

  public GoldMLJob(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public String name() {
    return "GOLD_ML_JOB";
  }

  @Override
  public void run() {
    spark
        .udf()
        .register(
            "vec_to_array",
            (UDF1<Vector, double[]>) Vector::toArray,
            DataTypes.createArrayType(DataTypes.DoubleType));

    Dataset<Row> df =
        spark
            .read()
            .parquet("data/YellowTaxiTripRecord/gold/aggregates/DAILY_REVENUE_ML_DATASET.parquet");
    // ───────────────────────────────── Add target
    WindowSpec w = Window.orderBy(col("TRIP_DATE"));

    df =
        df // ───── TARGET (next-day drop vs baseline)
            .withColumn("NEXT_DAY_REVENUE", lead(col("DAILY_REVENUE_USD"), 1).over(w))
            .withColumn(
                "TARGET",
                col("NEXT_DAY_REVENUE").lt(col("ROLLING_MEAN_7").multiply(0.9)).cast("int"))
            .drop("NEXT_DAY_REVENUE")
            .na()
            .drop();

    // ───────────── Feature columns
    String[] featureCols = {
      "TRIP_COUNT",
      "IS_WEEKEND",
      "REV_LAG_1",
      "REV_LAG_7",
      "DELTA_1",
      "DELTA_PCT_1",
      "ROLLING_MEAN_7",
      "ROLLING_STD_7",
      "BELOW_ROLLING_MEAN",
      "REVENUE_DROP_10P",
      "HIGH_VOLATILITY"
    };

    VectorAssembler assembler =
        new VectorAssembler().setInputCols(featureCols).setOutputCol("features");

    Dataset<Row> assembled = assembler.transform(df);

    // ───────────── Time-aware split
    Dataset<Row>[] split = assembled.randomSplit(new double[] {0.8, 0.2}, 42);
    Dataset<Row> train = split[0];
    Dataset<Row> test = split[1];

    // ───────────── Class weighting
    Dataset<Row> weightedTrain =
        train.withColumn("class_weight", when(col("TARGET").equalTo(1), 5.0).otherwise(1.0));

    // ───────────── Model
    GBTClassifier classifier =
        new GBTClassifier()
            .setLabelCol("TARGET")
            .setFeaturesCol("features")
            .setWeightCol("class_weight")
            .setMaxIter(60)
            .setMaxDepth(5);

    Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {classifier});

    PipelineModel model = pipeline.fit(weightedTrain);

    Dataset<Row> predictions = model.transform(test);

    // ───────────── Evaluation
    BinaryClassificationEvaluator evaluator =
        new BinaryClassificationEvaluator().setLabelCol("TARGET").setMetricName("areaUnderROC");

    double auc = evaluator.evaluate(predictions);
    System.out.println("📈 Daily Revenue Drop Prediction AUC = " + auc);

    Dataset<Row> predictionsFixed =
        predictions
            .withColumn("prob_array", callUDF("vec_to_array", col("probability")))
            .withColumn("P_DROP", col("prob_array").getItem(1))
            .drop("prob_array");

    double DROP_THRESHOLD = 0.30;

    Dataset<Row> calibrated =
        predictionsFixed.withColumn(
            "prediction_calibrated", col("P_DROP").geq(DROP_THRESHOLD).cast("int"));

    calibrated
        .select(
            col("TRIP_DATE"),
            col("P_DROP"),
            col("prediction"),
            col("prediction_calibrated"),
            col("TARGET"))
        .orderBy(col("TRIP_DATE"))
        .show(30, false);
  }
}
