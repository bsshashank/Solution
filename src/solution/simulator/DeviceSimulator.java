package solution.simulator;

import java.util.Random;
import java.util.UUID;

import org.json.JSONObject;

import solution.utils.KafkaMessenger;


/**
 * 
 * A Java class for simulating a device which records temperature.
 * @author Shashank
 *
 */
public class DeviceSimulator extends KafkaMessenger implements Runnable {
	
	UUID id;
	
	/**
	 * Starts simulator for a new device.
	 * @param topicName kafka topic to which the generated data has to be published.
	 * @param args 
	 */
	public DeviceSimulator(String topicName, String id) {
		super(topicName);
		this.id = UUID.fromString(id);
	}

	@Override
	public void run() {
		while(true) {
			this.sendMessageToKafka(generateData().toString());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	
	/**
	 * Generates and returns a device reading in {@link JSONObject} format.
	 * @return {@code JSONObject} generated device reading
	 */
	private JSONObject generateData() {
		Random rand = new Random();
		
		JSONObject contents = new JSONObject();
		JSONObject data = new JSONObject();
		
		contents.put("deviceid", id);
		contents.put("temperature", rand.nextInt((50 - 20) + 1) + 20);
		contents.put("location", generateLocation());
		contents.put("timestamp", System.currentTimeMillis());
		
		data.put("data", contents);
		return data;
	}

	private JSONObject generateLocation() {
		JSONObject location = new JSONObject();
		location.put("latitude", (Math.random()*-90.0000)+90.0000);
		location.put("longitude", (Math.random()*-180.0000)+180.0000);
		return location;
	}
	
	/*public static void main(String[] args) {
		DeviceSimulator device = new DeviceSimulator("deviceData");
		DeviceSimulator deviceTwo = new DeviceSimulator("deviceData");
		DeviceSimulator deviceThree = new DeviceSimulator("deviceData");
		
		Thread deviceThread = new Thread(device);
		deviceThread.start();
		
		Thread deviceThreadTwo = new Thread(deviceTwo);
		deviceThreadTwo.start();
		
		Thread deviceThreadThree = new Thread(deviceThree);
		deviceThreadThree.start();
	}*/

}
