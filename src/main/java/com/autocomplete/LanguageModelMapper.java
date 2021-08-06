package com.autocomplete;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LanguageModelMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text OutK = new Text();
    private Text OutV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // "A B C D\t100" -> ("A B C", "D=100")
        String line = value.toString().trim();
        String[] fields = line.split("\t");

        // "A B C D" "100"
        if (fields.length != 2) return;
        int count = Integer.parseInt(fields[1]);

        //
        String[] words = fields[0].toString().trim().split("\\s+");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < words.length-1; i++) {
            sb.append(words[i]);
            sb.append(" ");
        }

        String outkey = sb.toString().trim();
        OutK.set(outkey);
        OutV.set(words[words.length-1] + "=" + count);

        context.write(OutK, OutV);
    }
}
