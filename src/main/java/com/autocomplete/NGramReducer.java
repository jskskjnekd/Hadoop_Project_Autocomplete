package com.autocomplete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable OutV = new IntWritable();
    private int threshold;

    @Override
    protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Configuration conf2 = context.getConfiguration();
        threshold = conf2.getInt("threshold", 20);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // ("The object",1), ("The object of", 1) , ...

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        OutV.set(sum);
        if (sum >= threshold) context.write(key, OutV);
    }
}
