package com.app.twitterstreaming.service;

import java.util.List;

import com.app.twitterstreaming.model.Tweet;

public interface KafkaConsumerService {
	
	public List<Tweet> getTweets(int offset, int size, 
			String kafkaTopicName);
}
