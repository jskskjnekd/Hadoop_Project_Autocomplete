package com.autocomplete;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.*;

public class LanguageModelReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

    private int topK;
    private DBOutputWritable OutK = new DBOutputWritable();

    @Override
    protected void setup(Reducer<Text, Text, DBOutputWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        topK = conf.getInt("topK", 5);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, DBOutputWritable, NullWritable>.Context context) throws IOException, InterruptedException {

        // (A, B) => (C=10), (D=20), (E=5)...
        TreeMap<Integer, List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());

        for (Text value : values) {
            // "C=10"
            String[] fields = value.toString().trim().split("=");
            String word = fields[0];
            int count = Integer.parseInt(fields[1]);
            if (tm.containsKey(count)){
                tm.get(count).add(word);
            }else{
                List<String> lst = new ArrayList<String>();
                lst.add(word);
                tm.put(count, lst);
            }
        }

        // get topK words
        Iterator<Integer> iter = tm.keySet().iterator();
        for (int j=0; iter.hasNext() && j<topK;){
            int keyCount = iter.next();
            List<String> words = tm.get(keyCount);
            for (int i = 0; i < words.size() && j < topK; i++) {
                OutK.setStartingPhrase(key.toString());
                OutK.setFollowingPhrase(words.get(i));
                OutK.setCount(keyCount);
                context.write(OutK, NullWritable.get());
            }
        }

    }
}
