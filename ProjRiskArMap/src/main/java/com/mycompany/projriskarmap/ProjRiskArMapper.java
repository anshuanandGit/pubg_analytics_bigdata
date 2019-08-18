/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.projriskarmap;

import au.com.bytecode.opencsv.CSVParser;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Anshu Anand
 */
public class ProjRiskArMapper extends
        Mapper<LongWritable, Text, Text, LongWritable> {
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] lines = new
                    CSVParser().parseLine(value.toString());
            String keyout=lines[13];
           
            //second index contains vehicle spawn factor....
            context.write(new Text(keyout), new LongWritable(1)); 
        }
    }
}