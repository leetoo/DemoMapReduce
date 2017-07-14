package com.example.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.reflect.ClassTag;
import it.nerdammer.spark.hbase.*;

public final class CreeperAnalysis {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("localhost");
		conf.set("spark.hbase.host", "localhost");
		SparkContext sc = new SparkContext(conf);
		
		HBaseSparkContext hsc = new HBaseSparkContext(sc);
		//ClassTag<String> klass = new 
		//hsc.hbaseTable("origin_news", evidence$1, mapper)
	}
	
	public static void test(){
		SparkConf sparkConf = new SparkConf();
		SparkContext sc = new SparkContext(sparkConf);
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		try {
		  List<byte[]> list = new ArrayList<>();
		  list.add(Bytes.toBytes("1"));
		  list.add(Bytes.toBytes("5"));

		  JavaRDD<byte[]> rdd = jsc.parallelize(list);
		  Configuration conf = HBaseConfiguration.create();

		  //JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
		  HBaseSparkContext hhh = new HBaseSparkContext(sc);

//		  hbaseContext.foreachPartition(rdd,
//		      new VoidFunction<Tuple2<Iterator<byte[]>, Connection>>() {
//		   public void call(Tuple2<Iterator<byte[]>, Connection> t)
//		        throws Exception {
//		    Table table = t._2().getTable(TableName.valueOf(tableName));
//		    BufferedMutator mutator = t._2().getBufferedMutator(TableName.valueOf(tableName));
//		    while (t._1().hasNext()) {
//		      byte[] b = t._1().next();
//		      Result r = table.get(new Get(b));
//		      if (r.getExists()) {
//		       mutator.mutate(new Put(b));
//		      }
//		    }
//
//		    mutator.flush();
//		    mutator.close();
//		    table.close();
//		   }
//		  });
		} finally {
		  jsc.stop();
		}
	}
	
}
