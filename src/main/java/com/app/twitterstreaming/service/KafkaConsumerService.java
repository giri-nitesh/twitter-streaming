package com.app.twitterstreaming.service;

import java.util.List;

import org.springframework.stereotype.Component;

import com.app.twitterstreaming.model.Tweet;

@Component
public interface KafkaConsumerService {
	
	public List<Tweet> getTweets(int offset, int size, 
			String kafkaTopicName);
}
