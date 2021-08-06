package com.autocomplete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NGramDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // get job instance
        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter", ".");
        conf.set("noGram", String.valueOf(6));
        conf.set("threshold", String.valueOf(20));
        Job job1 = Job.getInstance(conf);
        job1.setJobName("NGramBuilder");

        // driver <> jar
        job1.setJarByClass(NGramDriver.class);

        // driver <> mapper/reducer
        job1.setMapperClass(NGramMapper.class);
        job1.setReducerClass(NGramReducer.class);

        // driver <> mapper/reducer output kv
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        //  input/output path/format
        FileInputFormat.setInputPaths(job1, new Path("bookList"));
        FileOutputFormat.setOutputPath(job1, new Path("running_output/out1"));

        // submit job
        job1.waitForCompletion(true);

//        // TODO: set up configuration for job2
//        // get job instance
//        Configuration conf2 = new Configuration();
//        conf2.set("topK", String.valueOf(6));
//        Job job2 = Job.getInstance(conf2);
//        job2.setJobName("LanguageModel");
//
//        // driver <> jar
//        job2.setJarByClass(NGramDriver.class);
//
//        // driver <> mapper/reducer
//        job2.setMapperClass(LanguageModelMapper.class);
//        job2.setReducerClass(LanguageModelReducer.class);
//
//        // driver <> mapper/reducer output kv
//        job1.setMapOutputKeyClass(Text.class);
//        job1.setMapOutputValueClass(Text.class);
//        job1.setOutputKeyClass(DBOutputWritable.class);
//        job1.setOutputValueClass(NullWritable.class);

    }
}
