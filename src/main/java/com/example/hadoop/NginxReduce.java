package com.example.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NginxReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

	protected void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
		long count = 0L;
		for (LongWritable l : value){
			count += l.get();
		}
		context.write(key, new LongWritable(count));
	}

}
