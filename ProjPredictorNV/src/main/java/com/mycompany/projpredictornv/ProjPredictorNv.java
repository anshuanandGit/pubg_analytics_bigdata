/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.projpredictornv;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.QuantileDiscretizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;

/**
 *
 * @author Anshu Anand
 */
public class ProjPredictorNv {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("ProjPredNv").getOrCreate();
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);

        Dataset<Row> ds1 = sparkSession.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .load("hdfs://spark01.cmua.dom:9000/anshua1/proj/agg_1.csv");

        Dataset<Row> ds = ds1.select(
                //col("party_size").cast(DataTypes.DoubleType),
                //col("player_assists").cast(DataTypes.DoubleType),
                //col("player_dbno").cast(DataTypes.DoubleType),
                //col("player_dist_ride").cast(DataTypes.DoubleType),
                //col("player_dist_walk").cast(DataTypes.DoubleType),
                col("player_dmg").cast(DataTypes.DoubleType),
                //col("player_kills").cast(DataTypes.DoubleType),
                col("player_survive_time").cast(DataTypes.DoubleType),
                col("team_placement").cast(DataTypes.DoubleType));
        //cache the data in memory.
        ds.cache();
        //show the data on screen
        System.out.println("Raw Data -------------------------------");
        ds.show();

        System.out.println("Schema before pre-processing -------------------------------");
        ds.printSchema();

        //preprocessing data  
        //we will will the team ranking in four groups .........
        QuantileDiscretizer qs = new QuantileDiscretizer()
                .setInputCol("team_placement")
                .setOutputCol("team_placement_grp")
                .setNumBuckets(3);
        Dataset<Row> dsnw = qs.fit(ds).transform(ds);
        dsnw.show();
        
        //Use pre-processed data for test and training
        Dataset<Row>[] data = dsnw.randomSplit(new double[]{0.7, 0.3});
        System.out.println("We have training examples count :: " + data[0].count() + " and test examples count ::" + data[1].count());

        //removing 'team_placement' cloumn and then forming str array
        String[] selectedCols = dsnw.columns();
        String[] featuresCols = dsnw.drop("team_placement_grp").columns();
        //print all feature columns
        for (String str : featuresCols) {
            System.out.println(str + " :: ");
        }
        VectorAssembler assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("rawFeatures");

        //Normalize the data.
        VectorIndexer vectorIndexer = new VectorIndexer()
                .setInputCol("rawFeatures")
                .setOutputCol("features")
                .setMaxCategories(4)
                .setHandleInvalid("keep");

        NaiveBayes nb = new NaiveBayes().setLabelCol("team_placement_grp");

        PipelineStage[] staging1 = new PipelineStage[3];

        staging1[0] = assembler;
        staging1[1] = vectorIndexer;
        staging1[2] = nb;

        System.out.println("Total Stages in pipeline -" + staging1.length);

        //ML algorithm pipeline....
        Pipeline pipeline = new Pipeline().setStages(staging1);
        PipelineModel pipelineModel = pipeline.fit(data[0]);
        Dataset<Row> predictions = pipelineModel.transform(data[1]);

        predictions.show();
        predictions.select("team_placement_grp", "prediction").show();

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("team_placement_grp")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        //Evalute the accuracy of model.......
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test set accuracy = " + accuracy);

    }
}
