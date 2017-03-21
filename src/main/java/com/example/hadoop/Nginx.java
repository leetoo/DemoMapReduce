package com.example.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class Nginx {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("mapred.job.queue.name", "regular");
		String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf);
		job.setJarByClass(Nginx.class);
		job.setMapperClass(NginxMap.class);
		job.setCombinerClass(NginxCombiner.class);
		job.setReducerClass(NginxReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
