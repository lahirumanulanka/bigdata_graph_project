package com.example.degree;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/** Reducer: sums counts per degree value */
public class DegreeDistReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values) sum += v.get();
        context.write(key, new IntWritable(sum));
    }
}
