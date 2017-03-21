package com.example.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NginxMap extends Mapper<LongWritable, Text, Text, LongWritable>{

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String s = value.toString();
		s = s.substring(s.indexOf("\"") + 1);
		String url = s.substring(0, s.indexOf("\""));
		url = url.split(" ")[1];
		if(url.indexOf("jsessionid") > -1){
			url = url.substring(0, url.indexOf("jsessionid") - 1);
			url = url.replaceAll("/", "");
		}
		s = s.substring(0, s.lastIndexOf("\""));
		String remoteIp = s.substring(s.lastIndexOf("\"") + 1);
		if (remoteIp.indexOf(", ") > -1){
			remoteIp = remoteIp.substring(remoteIp.indexOf(", ")+2);
		}
		context.write(new Text(url), new LongWritable(1));
		context.write(new Text(remoteIp), new LongWritable(1));
	}
	
}