package com.example.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Temperature {
	
	static class TempMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable>{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			//2015010999
            System.out.print("Before Mapper: " + key + ", " + value);
            String line = value.toString();
            String year = line.substring(0, 4);
            int temperature = Integer.parseInt(line.substring(8));
            output.collect(new Text(year), new IntWritable(temperature));
            // 打印样本: After Mapper:2000, 15
            System.out.println("======After Mapper:" + new Text(year) + ", " + new IntWritable(temperature));
		}
		
	}
	
	/**
     * 四个泛型类型分别代表：<br>
     * KeyIn:        Reducer的输入数据的Key，这里是每行文字中的“年份”<br>
     * ValueIn:      Reducer的输入数据的Value，这里是每行文字中的“气温”<br>
     * KeyOut:       Reducer的输出数据的Key，这里是不重复的“年份”<br>
     * ValueOut:     Reducer的输出数据的Value，这里是这一年中的“最高气温”
     */
    static class TempReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int maxValue = Integer.MIN_VALUE;
            StringBuffer sb = new StringBuffer();
            //取values的最大值
            while(values.hasNext()){
            	IntWritable value = values.next();
            	maxValue = Math.max(maxValue, value.get());
                sb.append(value).append(", ");
            }
            // 打印样本： Before Reduce: 2000, 15, 23, 99, 12, 22, 
            System.out.print("Before Reduce: " + key + ", " + sb.toString());
            output.collect(key, new IntWritable(maxValue));
            // 打印样本： After Reduce: 2000, 99
            System.out.println("======After Reduce: " + key + ", " + maxValue);
		}
    }
    
    public static void main(String[] args) throws Exception {
        //输入路径
        //String dst = "hdfs://localhost:9000/intput.txt";
        //输出路径，必须是不存在的，空文件加也不行。
        //String dstOut = "hdfs://localhost:9000/output1";
        JobConf conf = new JobConf(Temperature.class);
        conf.setJobName("temperature");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        conf.setMapperClass(TempMapper.class);
        conf.setCombinerClass(TempReducer.class);
        conf.setReducerClass(TempReducer.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        JobClient.runJob(conf);
    }
}
