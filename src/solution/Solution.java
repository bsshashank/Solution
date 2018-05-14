package solution;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.streaming.Duration;

import solution.simulator.DeviceSimulator;
import solution.spark.streaming.SparkStreamingJob;
import solution.utils.HBaseWriter;

public class Solution {

	public static void main(String[] args) {
		
		System.out.println("---------------------------------Starting device simulators---------------------------------");
		if(args.length < 2) {
			System.out.println("Specify at least one device id to simulate. Please use the following format:");
			System.out.println("solution.Solution Kafak-topic-name device-id1 device-id2...");
			System.exit(0);
		}
		
		String topicName = args[0];
		for(int i=1; i<args.length; i++) {
			DeviceSimulator device = new DeviceSimulator(topicName, args[i]);
			Thread deviceThread = new Thread(device);
			deviceThread.start();
		}
		
		System.out.println("--------------------------------Starting spark streaming job--------------------------------");
		HBaseWriter writer = new HBaseWriter("localhost", 2181);
		Set<String> topics = new HashSet<String>();
		topics.add(topicName);
		SparkStreamingJob sparkTest = new SparkStreamingJob("deviceData", "SparkTest", "local[*]", topics, new Duration(1000), writer);
		sparkTest.startStreaming();
		
	}

}
