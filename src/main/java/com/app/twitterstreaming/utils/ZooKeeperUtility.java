package com.app.twitterstreaming.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

public class ZooKeeperUtility {

	private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperUtility.class);
	
	public static void createTopic(String topic) throws FileNotFoundException, IOException {
		Properties properties = new Properties();
		properties.load(new FileReader(new File("src/main/resources/kafka.properties")));

		AdminClient adminClient = AdminClient.create(properties);
		
		// new NewTopic(topicName, numPartitions,replicationFactor)
		NewTopic newTopic = new NewTopic(topic, 1, (short) 1); 
		

		List<NewTopic> newTopics = new ArrayList<NewTopic>();
		newTopics.add(newTopic);
		
	    ListTopicsResult listTopics = adminClient.listTopics();
	    Set<String> names;
	    boolean contains = false;
		try {
			names = listTopics.names().get();
			contains = names.contains(topic);
		} catch (InterruptedException | ExecutionException e) {
			LOGGER.error(e.getMessage());
		}
	    
		
		if(!contains) {	
			adminClient.createTopics(newTopics);
			LOGGER.info("Topic created successfully.");
		}
		else {
			LOGGER.info("Topic exists");
		}
		adminClient.close();
	}

}
