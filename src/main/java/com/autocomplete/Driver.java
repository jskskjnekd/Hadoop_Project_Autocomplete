package com.autocomplete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // init parameters
        String nGramLib = "running_output/out1";

        // get job instance
        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter", ".");
        conf.set("noGram", String.valueOf(6));
        conf.set("threshold", String.valueOf(20));
        Job job1 = Job.getInstance(conf);
        job1.setJobName("NGramBuilder");

        // driver <> jar
        job1.setJarByClass(Driver.class);

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
        FileOutputFormat.setOutputPath(job1, new Path(nGramLib));

        // submit job
        job1.waitForCompletion(true);

        // job1's output is job2's input
        // get job instance
        Configuration conf2 = new Configuration();
        conf2.set("topK", String.valueOf(6));

        // In the intellij, there is a tool called "Database" setting/testing the mysql (or any other db) connection
        DBConfiguration.configureDB(
                conf2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test",
                "root",
                "password"
        );

        Job job2 = Job.getInstance(conf2);
        job2.setJobName("LanguageModel");

        // driver <> jar
        job2.setJarByClass(Driver.class);

        // besides the maven setup, we should specify the mysql connector (as a JAR file)
        job2.addArchiveToClassPath(new Path("/home/sunyaojin/Downloads/mysql_jdbc_j/mysql-connector-java-8.0.26/mysql-connector-java-8.0.26.jar"));

        // driver <> mapper/reducer
        job2.setMapperClass(LanguageModelMapper.class);
        job2.setReducerClass(LanguageModelReducer.class);

        // driver <> mapper/reducer output kv
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        // the field name below should match the database col name exactly
        DBOutputFormat.setOutput(job2, "output",
                new String[]{"starting_phrase", "following_word", "count"});

        TextInputFormat.setInputPaths(job2, nGramLib);
        FileOutputFormat.setOutputPath(job2, new Path("running_output/out2"));
        boolean result = job2.waitForCompletion(true);

        System.exit(result? 0:1);
    }
}
