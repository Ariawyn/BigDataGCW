package com.grouph.wikisearch;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;

public class WikiSearchInvertedIndex {

    public static void runInvertedIndexJob(String[] input, String output) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        conf.set("mapreduce.child.java.opts", "-Xmx2048m");

        job.setJarByClass(WikiSearchInvertedIndex.class);

        job.setReducerClass(WikiSearchInvertedIndexReducer.class);

        job.setMapperClass(WikiSearchInvertedIndexMapper.class);

        /*job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);*/

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);

        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        runInvertedIndexJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}
