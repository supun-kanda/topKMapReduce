package com.supunk.mapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNumeric;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (isNotBlank(line)) {
            String[] raw_kv = line.split(",");
            if (raw_kv.length == 2 && isNumeric(raw_kv[1])) {
                context.write(new Text(raw_kv[0]), new IntWritable(Integer.parseInt(raw_kv[1])));
            }
        }
    }
}
