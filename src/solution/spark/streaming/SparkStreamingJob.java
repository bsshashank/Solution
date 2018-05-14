package solution.spark.streaming;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONObject;

import scala.Tuple2;
import solution.utils.HBaseWriter;

/**
 * 
 * Java class for streaming data from kafka and saving into {@code HBase}. 
 * @author Shashank
 *
 */
public class SparkStreamingJob {
	
	static String tableName;
	String appName;
	String master;
	
	SparkConf sparkConf;
	JavaSparkContext ctx;
	JavaStreamingContext ssc;
	static HBaseWriter writer;
	Set<String> topics;
	Map<String, String> kafkaParams;
	
	public SparkStreamingJob(String tableName, String appName, String master, Set<String> topics, Duration duration, HBaseWriter writer) {
		SparkStreamingJob.tableName = tableName;
		this.appName = appName;
		this.master = master;
		SparkStreamingJob.writer = writer;
		this.topics = topics;
		
		setupSparkContext();
		setUpKafka();
		createHBaseTable();
	}
	
	/**
	 * Sets up the context for performing Spark streaming.
	 */
	private void setupSparkContext() {
		sparkConf = new SparkConf().setAppName(appName).setMaster(master);
	    ctx = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
	    ssc = new JavaStreamingContext(ctx, new Duration(1000));
	}
	
	/**
	 * Creates a new HBase table for saving device readings.
	 */
	private void createHBaseTable() {
	    List<String> columnDescriptors = new ArrayList<>();
		columnDescriptors.add("deviceid");
		columnDescriptors.add("temperature");
		columnDescriptors.add("location");
		columnDescriptors.add("timestamp");
	    writer.createTable(tableName, columnDescriptors);
	}
	
	private void setUpKafka() {
		kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
	}

	/*public static void main(String[] args) {
		
		HBaseWriter writer = new HBaseWriter("localhost", 2181);
		Set<String> topics = new HashSet<String>();
		topics.add("deviceData");
		SparkStreamingJob sparkTest = new SparkStreamingJob("deviceData", "SparkTest", "local[*]", topics, new Duration(1000), writer);
		sparkTest.startStreaming();
	}*/

	public void startStreaming() {
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		
		JavaDStream<String> putData = directKafkaStream.flatMap(new FlatMapperFunction());
		
		putData.foreachRDD(new HBaseStore());
		
		this.ssc.start();
		this.ssc.awaitTermination();
	}

	/**
	 * Creates a Put HBase record which can be inserted into HBase.
	 * @param jsonData
	 * @return HBase Put record for the corresponding input paramater.
	 */
	private static Put createRecord(JSONObject jsonData) {
		Put deviceReading = new Put(Bytes.toBytes(jsonData.toString()));
		deviceReading.addColumn(Bytes.toBytes("deviceid"), Bytes.toBytes("deviceid"), Bytes.toBytes(jsonData.get("deviceid").toString()));
		deviceReading.addColumn(Bytes.toBytes("temperature"), Bytes.toBytes("temperature"), Bytes.toBytes(jsonData.get("temperature").toString()));
		deviceReading.addColumn(Bytes.toBytes("location"), Bytes.toBytes("latitude"), Bytes.toBytes(jsonData.getJSONObject("location").get("latitude").toString()));
		deviceReading.addColumn(Bytes.toBytes("location"), Bytes.toBytes("longitude"), Bytes.toBytes(jsonData.getJSONObject("location").get("longitude").toString()));
		deviceReading.addColumn(Bytes.toBytes("timestamp"), Bytes.toBytes("timestamp"), Bytes.toBytes(jsonData.get("timestamp").toString()));
		
		return deviceReading;
	}

	/**
	 * Implements a FlatMapFunction for converting timestamp into a human readable format.
	 * @author Shashank
	 *
	 */
	public static class FlatMapperFunction implements FlatMapFunction<Tuple2<String, String>, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<String> call(Tuple2<String, String> data)
				throws Exception {
			JSONObject obj = new JSONObject(data._2).getJSONObject("data");
			Date date = new Date((long) obj.get("timestamp"));
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
			String formattedDate = df.format(date);
			obj.put("timestamp", formattedDate);
			return Arrays.asList(obj.toString());
		}
		
	}
	
	/**
	 * Implements a VoidFunction for inserting data into HBase.
	 * @author Shashank
	 *
	 */
	public static class HBaseStore implements VoidFunction<JavaRDD<String>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void call(JavaRDD<String> putRow) throws Exception {
			List<String> putList = putRow.collect();
			for (String put : putList) {
				writer.insertData(tableName, createRecord(new JSONObject(put)));
			}
		}
	}
}
