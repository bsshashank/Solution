package solution.utils;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * 
 * A Java class for publishing messages to a kafka topic.
 * @author Shashank
 *
 */
public class KafkaMessenger {
	
	private String topic;
	Producer<String, String> producer;
	long messageId = 0;

	public KafkaMessenger(String deviceId) {
		this.topic = deviceId;
		setUpMessenger();
	}
	
	/**
	 * Sends a new message to the registered kafka topic.
	 * @param message Message to be sent.
	 */
	protected void sendMessageToKafka(String message) {
		producer.send(new ProducerRecord<String, String>(topic, Long.toString(messageId++), message));
	}

	/**
	 * Sets up kafka producer.
	 */
	private void setUpMessenger() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<>(props);
	}
}
