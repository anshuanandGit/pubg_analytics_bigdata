/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.projvecspawnfactor;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;

/**
 *
 * @author Anshu Anand
 */
public class ProjVecSpawnFactor {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("AirlinePredictionDecesionTree").getOrCreate();
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);

        Dataset<Row> ds1 = sparkSession.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .load("hdfs://spark01.cmua.dom:9000/anshua1/proj/agg_1.csv");

        ds1.printSchema();
        Dataset<Row> ds11 = sparkSession.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .load("hdfs://spark01.cmua.dom:9000/anshua1/proj/agg_2.csv");
        ds11.printSchema();

        System.out.println("Joining the two datasets............................");
        //extract column 
        String joinCol = "match_id";
        Dataset<Row> ds = ds1.union(ds11).select(
                col("player_dist_ride"),
                col("player_dist_walk")
        );

        //cache the data in memory.
        ds.cache();
        //show the data on screen
        System.out.println("Raw Data -------------------------------");
        ds.show();

        //print schema...
        System.out.println("Schema before pre-processing -------------------------------");
        ds.printSchema();

        //Feature Engineering ...Create new feature to improve the accuracy of models.
        //We will use a UDF (User Defined Function) to Add the values of val1 and val2 column and append it as a new column called total.
        UDF2 addVSF = new Col1UDF();
        //Register UDF to SparkSession
        sparkSession.udf().register("addVSF", addVSF, DataTypes.StringType);

        Dataset<Row> dsnew1 = ds.withColumn("VehicleSpawnFactor", callUDF("addVSF", col("player_dist_ride"), col("player_dist_walk")));

        System.out.println("Schema With Feature Column Vechicle Spawn factor-------------------------------");
        dsnew1.printSchema();
        System.out.println("Data With Feature Column Vechicle Spawn factor-------------------------------");
        dsnew1.show();

        dsnew1.coalesce(1)
               .write()
               .option("header", "true")
               .csv("hdfs://spark01.cmua.dom:9000/anshua1/output/vsf/");

    }
}
