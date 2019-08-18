/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.projcluster;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;
/**
 *
 * @author Anshu Anand
 */
public class ProjCluster {
    
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("KmeanClusterProjAggData").getOrCreate();
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);

        Dataset<Row> ds1 = sparkSession.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .load("hdfs://spark01.cmua.dom:9000/anshua1/proj/agg_1.csv");
        
        Dataset<Row> ds11 = sparkSession.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .load("hdfs://spark01.cmua.dom:9000/anshua1/proj/agg_2.csv");

        Dataset<Row> ds = ds1.union(ds11).select(
                col("party_size").cast(DataTypes.DoubleType),
                col("player_assists").cast(DataTypes.DoubleType),
                col("player_dbno").cast(DataTypes.DoubleType),
                col("player_dist_ride").cast(DataTypes.DoubleType),
                col("player_dist_walk").cast(DataTypes.DoubleType),
                col("player_dmg").cast(DataTypes.DoubleType),
                col("player_kills").cast(DataTypes.DoubleType),
                col("player_survive_time").cast(DataTypes.DoubleType),
                col("team_placement").cast(DataTypes.DoubleType));

        //cache the data in memory.
        ds.cache();
        //show the data on screen
        System.out.println("Raw Data -------------------------------");
        ds.show();
        //drop missing values...
        //ds.na().drop();
        //print schema...
        System.out.println("Schema before pre-processing -------------------------------");
        ds.printSchema();
        //Feature Engineering ...Create new feature to improve the accuracy of models.

        String[] column = ds.columns();
        //String[] featuresCols = new String[column.length];
        System.out.println("Total No.Of column: " + column.length);

        /*
        //convert string columns to double
        StringIndexer[] strIndexList = new StringIndexer[column.length];
        StringIndexerModel[] strIndModelList = new StringIndexerModel[column.length];

        for (int i = 0; i < column.length; i++) {
            strIndexList[i] = new StringIndexer().setInputCol(column[i]).setOutputCol(column[i] + "-index");
            //System.out.println("StringIndexer created for column: " + column[i]);
            //StringIndexerModel strIndModel1 = strIndexList[i].fit(ds);
            // strIndModelList[i] = strIndModel1;
            //featuresCols[i] =column[i] + "-index";
            strIndModelList[i] = new StringIndexer()
                    .setInputCol(column[i])
                    .setOutputCol(column[i] + "-index")
                    .fit(ds);
            System.out.println("StringIndexer fit created for column: " + column[i] + "-index");

        }
        Pipeline pl = new Pipeline().setStages(new PipelineStage[]{strIndModelList[0], strIndModelList[1], strIndModelList[2], strIndModelList[3], strIndModelList[4], strIndModelList[5], strIndModelList[6], strIndModelList[7], strIndModelList[8], strIndModelList[9], strIndModelList[10]});
        PipelineModel pm = pl.fit(ds);
        Dataset<Row> formatData = pm.transform(ds);
        System.out.println("Schema after pre-processing -------------------------------");
        formatData.printSchema();
        Dataset<Row> preprocesedData = formatData.select(col("Satisfaction-index"),
                col("Airline Status-index"),
                col("Age-index"),
                col("Gender-index"),
                col("Price Sensitivity-index"),
                col("Numbers of Flights pa-index"),
                col("Shopping Amount at Airport-index"),
                col("Class-index"),
                col("Flight cancelled-index"),
                col("Flight Distance-index"),
                col("Flight time in minutes-index"));

        System.out.println("Preprocessed Data -------------------------------");
        preprocesedData.show();
        
        //Dataset<Row>[] data = preprocesedData.randomSplit(new double[]{0.7, 0.3});
        //System.out.println("We have training examples count :: " + data[0].count() + " and test examples count ::" + data[1].count());
 */      
        String[] featuresCols = ds.columns();
        
        VectorAssembler assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("rawFeatures");
        VectorIndexer vectorIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features").setMaxCategories(4);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{assembler, vectorIndexer});
        //apply pipeline
        PipelineModel pipelineModel = pipeline.fit(ds);

        Dataset<Row> readyData = pipelineModel.transform(ds);

       //run k-mean
        KMeans kmeans = new KMeans().setK(4).setSeed(1L);
        KMeansModel model = kmeans.fit(readyData);

        Dataset<Row> predictions = model.transform(readyData);
        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();

        double silhouette = evaluator.evaluate(predictions);
        System.out.println("Silhouette with squared euclidean distance = " + silhouette);

// Shows the result.
        Vector[] centers = (Vector[]) model.clusterCenters();
        System.out.println("Cluster Centers: -----------------------------------");
        
        int cluscnt=1;
        for (Vector center : centers) {
            System.out.println("Cluster "+cluscnt+":- "+center);
            cluscnt =cluscnt+1;
        }
    }
}
