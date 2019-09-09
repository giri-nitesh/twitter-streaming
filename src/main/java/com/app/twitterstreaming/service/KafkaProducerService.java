package com.app.twitterstreaming.service;

import org.springframework.stereotype.Component;

@Component
public interface KafkaProducerService {
	
	public void activateTweetListening();

}
