CREATE EXTERNAL TABLE deviceReadings(
key string, 
id string,
latitude double, 
longitude double, 
temperature int, 
time timestamp
) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,deviceid:deviceid,location:latitude,location:longitude,temperature:temperature,timestamp:timestamp") 
TBLPROPERTIES ("hbase.table.name" = "deviceData");
