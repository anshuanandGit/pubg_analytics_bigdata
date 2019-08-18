/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.projvantagemap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author Anshu Anand
 */
public class ProjVantage {
     /**
     * This is void method to execute main method
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(ProjVantage.class);
        job.setJobName("ProjVantage");

        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(com.mycompany.projvantagemap.ProjVantageMapper.class);
        
        job.setCombinerClass(com.mycompany.projvantagemap.ProjVantageReducer.class);
        job.setReducerClass(com.mycompany.projvantagemap.ProjVantageReducer.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        job.waitForCompletion(true);
    }
}
