/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.vsfmap;

import au.com.bytecode.opencsv.CSVParser;
import java.io.IOException;
import java.time.Month;
import java.util.Calendar;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Anshu Anand
 */
public class VSFMapper  extends
        Mapper<LongWritable, Text, Text, LongWritable> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] lines = new
                    CSVParser().parseLine(value.toString());
            String keyout="NA";
            if(!lines[2].equals("NA")){
              Double d= Double.parseDouble(lines[2]);
              keyout = String.format("%.2f", d);
            }
            else{
                keyout="NA";
            }
            //second index contains vehicle spawn factor....
            context.write(new Text(keyout), new LongWritable(1)); 
        }
    }
}

