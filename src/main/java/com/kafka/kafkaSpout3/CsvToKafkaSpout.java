package com.kafka.kafkaSpout3;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import au.com.bytecode.opencsv.CSVReader;

public class CsvToKafkaSpout {
	private static Producer<Integer, String> producer;
    private final Properties properties = new Properties();

    public CsvToKafkaSpout() {
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");
        producer = new Producer<>(new ProducerConfig(properties));
    }

	public static void main(String[] args) throws Exception {//test,f://1.csv
		// TODO Auto-generated method stub
		if(args != null && args.length > 1){
			new CsvToKafkaSpout();
			CSVReader reader = null;
			String[] nextLine;
	        String topic = args[0];//test
	        KeyedMessage<Integer, String> data = null;
			
			if(args != null && args.length > 0){
				//reader = new CSVReader(new FileReader(args[1])); //f://1.csv
				DataInputStream in = new DataInputStream(new FileInputStream(new File(args[1])));  
				reader = new CSVReader(new InputStreamReader(in,"big5"));
				if(reader.readNext() != null){
					while ((nextLine = reader.readNext()) != null) { 
						//System.out.println(nextLine[0]); 
				        data = new KeyedMessage<>(topic, nextLine[0]);
				        producer.send(data);
				        data = null;
				       
					}
					 producer.close();
				}	
				reader = null;
				in = null;
			}
			
		} 
	}

}
