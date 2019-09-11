package com.app.twitterstreaming.consumer;

import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.jackson.JsonObjectSerializer;

import com.app.twitterstreaming.configuration.KafkaConfiguration;
import com.app.twitterstreaming.model.Tweet;
import com.app.twitterstreaming.producer.TwitterProducer;
import com.app.twitterstreaming.utils.Utility;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class TwitterConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterConsumer.class);

	private final static String TOPIC = KafkaConfiguration.TOPIC;

	/*
    @Override
	@KafkaListener(topics = "TestNewMicroservicesOrderEvent_Demo")
	public void consume(@Payload String message) {
		logger.info(String.format("#### -> Consumed message -> %s", message));
		List<String> list = Arrays.asList(message.split(","));
		sspCartService.getSspCart(list.get(0), list.get(1));
	}
	 */

	public List<Tweet> getTweets(int offset, int size, 
			String kafkaTopicName) {
		KafkaConsumer<String, String> kafkaConsumer = null;
		boolean flag = true;
		List<Tweet> messagesFromKafka = new ArrayList<>();
		int recordCount = 0;
		int i = 0;
		KafkaTweetConsumer tweetConsumer = new KafkaTweetConsumer();
		kafkaConsumer = tweetConsumer.createConsumer();
		kafkaConsumer.subscribe(Arrays.asList(kafkaTopicName));
		TopicPartition topicPartition = new TopicPartition(kafkaTopicName, 0);
		LOGGER.info("Subscribed to topic " + kafkaConsumer.listTopics());
		while (flag) {
			// will consume all the messages and store in records
			ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);

			//kafkaConsumer.seekToBeginning(topicPartition);

			// getting total records count
			recordCount = records.count();
			Gson gson = new Gson();
			LOGGER.info("recordCount " + recordCount);
			for (ConsumerRecord<String, String> record : records) {
				if(record.value() != null) {
					if (i >= recordCount - size) {
						// adding last 20 messages to messagesFromKafka	
						Tweet tweet = gson.fromJson(record.value(), Tweet.class);
						LOGGER.info("Tweet with id: "+tweet.getId()+" processed");
						messagesFromKafka.add(tweet);
					}
					i++;
				}
			}
			if (recordCount > 0) {
				flag = false;
			}
		}
		kafkaConsumer.close();
		return messagesFromKafka;
	}

}
