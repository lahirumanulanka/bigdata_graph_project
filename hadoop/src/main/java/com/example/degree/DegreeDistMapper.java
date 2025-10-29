package com.example.degree;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Mapper for degree distribution: input lines are "node\tindegree"; emit (indegree, 1) */
public class DegreeDistMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final IntWritable degree = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("#"))
            return;
        String[] parts = line.split("\t");
        if (parts.length < 2)
            return;
        try {
            int deg = Integer.parseInt(parts[1].trim());
            if (deg < 0) return;
            degree.set(deg);
            context.write(degree, ONE);
        } catch (NumberFormatException e) {
            // skip malformed
        }
    }
}
