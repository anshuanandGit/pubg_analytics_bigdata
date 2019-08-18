/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.vsfmap;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Anshu Anand
 */
public class VSFReducer extends
        Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text token, Iterable<LongWritable> counts,
            Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (LongWritable count : counts) {
            sum += count.get();
        }
        context.write(token, new LongWritable(sum));
    }
}
