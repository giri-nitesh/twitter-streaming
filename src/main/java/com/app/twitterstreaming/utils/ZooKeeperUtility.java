package com.app.twitterstreaming.utils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

public class ZooKeeperUtility {
	
	private ZooKeeperUtility() {}

	private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperUtility.class);
	
	/**
	 * 
	 * @param topic Creates a topic to the kafka cluster with
	 * specified @param topic
	 * @throws {@IOException}
	 * @throws {@link InterruptedException}	
	 */
	public static void createTopic(String topic) throws IOException {
		topic = getValidString(topic);
		LOGGER.info("Creating topic with name: {}", topic);
		Properties properties = new Properties();
		properties.load(new FileReader(new File("src/main/resources/kafka.properties")));

		AdminClient adminClient = AdminClient.create(properties);
		
		// new NewTopic(topicName, numPartitions,replicationFactor)
		NewTopic newTopic = new NewTopic(topic, 1, (short) 1); 
		

		List<NewTopic> newTopics = new ArrayList<>();
		newTopics.add(newTopic);
		
	    ListTopicsResult listTopics = adminClient.listTopics();
	    Set<String> names;
	    boolean contains = false;
		try {
			names = listTopics.names().get();
			contains = names.contains(topic);
		} catch (InterruptedException | ExecutionException e) {
			LOGGER.error(e.getMessage());
			//Logging is not enough
			Thread.currentThread().interrupt();
		}
	    
		
		if(!contains) {	
			adminClient.createTopics(newTopics);
			LOGGER.info("Topic with name {} created successfully.", topic);
		}
		else {
			LOGGER.info("Topic exists");
		}
		adminClient.close();
	}
	
	/**
	 * Lists down all the topic registered with kafka cluster
	 * @return
	 * @throws IOException
	 */
	public static List<String> getAllTopics() throws IOException {
		Properties properties = new Properties();
		properties.load(new FileReader(new File("src/main/resources/kafka.properties")));

		AdminClient adminClient = AdminClient.create(properties);
	    ListTopicsResult listTopics = adminClient.listTopics();
	    Set<String> names = null;
		try {
			names = listTopics.names().get();
		} catch (InterruptedException | ExecutionException e) {
			LOGGER.error(e.getMessage());
			//Logging is not enough
			Thread.currentThread().interrupt();
		}
		adminClient.close();
		if(names != null)
			return names.stream().collect(Collectors.toList());
		else return new ArrayList<>();
	}
	/**
	 * 
	 * @param hashtag
	 * @return A valid string after removing '@' or '#'
	 * so that we can do valid operations with string
	 */
	public static String getValidString(String hashtag) {
		if(hashtag.length() < 2)
			return null;
		if(hashtag.substring(0, 1).equals("#")) {
			hashtag = hashtag.substring(1);
		}
		if(hashtag.substring(0, 1).equals("@")) {
			hashtag = hashtag.substring(1);
		}
		return hashtag;
	}

}
