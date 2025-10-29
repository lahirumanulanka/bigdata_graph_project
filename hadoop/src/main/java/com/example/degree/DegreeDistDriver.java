package com.example.degree;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/** Driver to compute degree distribution from node indegree outputs */
public class DegreeDistDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: DegreeDistDriver <indegree_input> <dist_output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Degree Distribution");
        job.setJarByClass(DegreeDistDriver.class);
        job.setMapperClass(DegreeDistMapper.class);
        job.setReducerClass(DegreeDistReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
