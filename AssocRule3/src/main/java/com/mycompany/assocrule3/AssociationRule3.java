/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.assocrule3;

import java.sql.Array;
import static java.sql.DriverManager.println;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.types.DataTypes;

/**
 *
 * @author Anshu Anand
 */
public class AssociationRule3 {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("AirlinePredictionDecesionTree").getOrCreate();
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);

        Dataset<Row> ds1 = sparkSession.read()
                .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .load("hdfs://spark01.cmua.dom:9000/anshua1/proj/death1.csv");

        Dataset<Row> ds = ds1.select(
                col("match_id").cast(DataTypes.StringType),
                //col("match_mode").cast(DataTypes.StringType),
                //col("party_size").cast(DataTypes.StringType)
                //col("player_assists").cast(DataTypes.StringType),
                //col("player_dbno").cast(DataTypes.StringType),
                //col("player_dist_ride").cast(DataTypes.StringType),
                //col("player_dist_walk").cast(DataTypes.StringType)
                //col("player_dmg").cast(DataTypes.StringType)
                //col("player_kills").cast(DataTypes.StringType),
                //col("team_placement").cast(DataTypes.StringType)
                //col("player_survive_time").cast(DataTypes.StringType)
                //col("killer_name").cast(DataTypes.StringType),
                //col("victim_name").cast(DataTypes.StringType),
                col("killed_by").cast(DataTypes.StringType)
        );

        //cache the data in memory.
        ds.cache();
        //show the data on screen
        System.out.println("Raw Data -------------------------------");
        ds.show();
        //print schema...
        System.out.println("Schema before pre-processing -------------------------------");
        ds.printSchema();

        JavaRDD<String> data = ds.toJavaRDD().map(row -> row.toString());
        data.count();
        JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(",")));
        System.out.println("converted to java rdd..");
        transactions.cache();

        FPGrowth fpg = new FPGrowth()
                .setMinSupport(0.1)
                .setNumPartitions(10);
        FPGrowthModel<String> model = fpg.run(transactions);
        transactions.cache();
       // System.out.println("Printing frequent Itemset:");
       // for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
       //     System.out.println("Printing frequent Itemset......");
       //     System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
       // }

        double minConfidence = 0.3;
        System.out.println("Printing Association between itemset ********************************");
        for (AssociationRules.Rule<String> rule
                : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
            System.out.println("ANTECEDENT :  "
                    + rule.javaAntecedent() + " => CONSEQUENT:" + rule.javaConsequent() + ", CONFIDENCE " + rule.confidence());
        }
        // $example off$

        //sparkSession.stop();
    }
}
