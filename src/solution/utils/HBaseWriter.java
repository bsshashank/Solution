package solution.utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;


/**
 * Java class which implements common functionality such as creating a new HBase table and writing data to it.
 * @author Shashank
 *
 */
public class HBaseWriter {

	Configuration config;
	Connection connection = null;

	public HBaseWriter(String address, int port) {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", address);
		config.set("hbase.zookeeper.property.clientPort", Integer.toString(port));

		try {
			connection = ConnectionFactory.createConnection(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * Creates a new HBase table with the provided columnDescriptors. 
	 * @param tableName
	 * @param columnDescriptors
	 */
	public void createTable(String tableName, List<String> columnDescriptors) {
		try {
			Admin admin = connection.getAdmin();
			if(!admin.tableExists(TableName.valueOf(tableName))) {
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
				for(String columnDescriptor: columnDescriptors)
					tableDescriptor.addFamily(new HColumnDescriptor(columnDescriptor));
				admin.createTable(tableDescriptor);
				System.out.println("Table " + tableName + " created");
			}
			else
				System.out.println("Table " + tableName + " already exists");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Inserts a given Put object into the specified table.
	 * @param tableName
	 * @param put
	 */
	public void insertData(String tableName, Put put) {
		
		try {
			Table table = connection.getTable(TableName.valueOf(tableName));
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
