package com.autocomplete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private int noGram;
    private Text OutK = new Text();
    private IntWritable OutV = new IntWritable(1);

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // read the parameter NGram
        Configuration conf = context.getConfiguration();
        noGram = conf.getInt("noGram", 5);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // "The object of this series of text-books is"
        // ("The object",1), ("The object of", 1) , ...

        // split the line
        String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]", " ");
        String[] fields = line.split("\\s+");

        if (fields.length < 2) return;

        StringBuilder sb;
        for (int i = 0; i < fields.length; i++) {
            sb = new StringBuilder();
            sb.append(fields[i]);
            for (int j = 1; i+j < fields.length && j < noGram; j++) {
                sb.append(" ");
                sb.append(fields[i+j]);
                OutK.set(sb.toString());
                context.write(OutK, OutV);
            }
        }

    }
}
