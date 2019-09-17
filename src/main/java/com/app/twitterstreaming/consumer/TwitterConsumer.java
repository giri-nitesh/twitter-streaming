package com.app.twitterstreaming.consumer;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.app.twitterstreaming.model.Tweet;
import com.app.twitterstreaming.service.KafkaConsumerService;
import com.google.gson.Gson;

/**
 * 
 * @author Nitesh
 * Represents Twitter Consumer that fetches tweets from
 * Kafka queue
 *
 */
public class TwitterConsumer implements KafkaConsumerService {

	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterConsumer.class);

	/**
	 * @param offset Represents the starting point of 
	 * message in the queue
	 * @param size Represents the size of the response to be 
	 * returned to the caller
	 * @param kafkaTopicName Represents the topic name for which
	 * the tweets needs to be fetched
	 */
	public List<Tweet> getTweets(int offset, int size, 
			String kafkaTopicName) {
		KafkaConsumer<String, String> kafkaConsumer = null;
		boolean flag = true;
		List<Tweet> messagesFromKafka = new ArrayList<>();
		int recordCount = 0;
		int i = 0;
		KafkaTweetConsumer tweetConsumer = new KafkaTweetConsumer(kafkaTopicName);
		kafkaConsumer = tweetConsumer.createConsumer();
		LOGGER.info("Creaetd a consumer successfully which has been subscribed to : {} ", 
				tweetConsumer.getTopic());
		while (flag) {
			// will consume all the messages and store in records
			ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
			// getting total records count
			recordCount = records.count();
			Gson gson = new Gson();
			for (ConsumerRecord<String, String> record : records) {
				if(record.value() != null) {
					if (i >= recordCount - size) {
						// adding last 'size' messages to messagesFromKafka	
						Tweet tweet = gson.fromJson(record.value(), Tweet.class);
						LOGGER.info("Tweet with id: {} processed", tweet.getId());
						messagesFromKafka.add(tweet);
					}
					i++;
				}
			}
			if (recordCount > 0) {
				flag = false;
			}
		}
		LOGGER.info("Fetched {} messages from Kafka", size);
		kafkaConsumer.close();
		return messagesFromKafka;
	}

}
