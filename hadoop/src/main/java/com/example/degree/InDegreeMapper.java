package com.example.degree;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Placeholder mapper: emits (dst, 1) for each edge src dst */
public class InDegreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final Text node = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("#"))
            return;
        String[] parts = line.split("\\s+");
        if (parts.length < 2)
            return;
        node.set(parts[1]);
        context.write(node, ONE);
    }
}
