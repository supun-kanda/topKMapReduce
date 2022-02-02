package com.supunk.mapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class Reduce extends Reducer<Text, IntWritable, Text, Text> {
    private String K_env = System.getenv("K"); // the top count
    private final int K = isNotBlank(K_env) ? Integer.parseInt(K_env) : 3;

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        Set<Integer> sortedValues = new TreeSet<>();

        for (IntWritable val : values) {
            sortedValues.add(Integer.valueOf(val.get()));
        }
        context.write(key, new Text(getSortedList(new ArrayList<>(sortedValues))));
    }

    private String getSortedList(List<Integer> list) {

        int size = list.size();
        StringBuilder topK = new StringBuilder();
        for (int i = size - 1; i >= 0 && i >= size - K; i--) {
            topK.append(list.get(i)).append(",");
        }
        return topK.deleteCharAt(topK.length() - 1).toString();
    }
}
