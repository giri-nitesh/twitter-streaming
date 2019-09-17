package com.app.twitterstreaming.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.app.twitterstreaming.constant.AppConstants.QueryConstants;
import com.app.twitterstreaming.consumer.TwitterConsumer;
import com.app.twitterstreaming.model.Tweet;
import com.app.twitterstreaming.producer.TwitterProducer;
import com.app.twitterstreaming.utils.ZooKeeperUtility;

@RestController
@RequestMapping("/app")
public class TwitterController {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterController.class);


	@PostMapping(value ="/create-topic/")
	@ResponseBody
	public ResponseEntity<String> getKafkaTopic(@RequestParam final String topic) 
			throws IOException {
		ZooKeeperUtility.createTopic(topic);
		return new ResponseEntity<>(HttpStatus.CREATED);
	}
	
	@GetMapping(path="/get-topics")
	public ResponseEntity<List<String>> getAllTopics() 
			throws IOException {
		List<String> list = ZooKeeperUtility.getAllTopics();
		HttpHeaders headers = new HttpHeaders();
		headers.add("Responded", "TwitterController");
		return ResponseEntity.accepted().headers(headers).body(list);
	}

	@GetMapping(value="/tweets/")
	public ResponseEntity<List<Tweet>> getTweetsForHashtag(@RequestParam final String source){
		
		QueryConstants queryType = QueryConstants.HASHTAG;
		if(source.contains("@")) {
			queryType = QueryConstants.ACCOUNT;
		}
		String hashtag = ZooKeeperUtility.getValidString(source).trim();
		List<String> hashtags = new ArrayList<>();
		hashtags.add(hashtag);
		TwitterProducer producer = new TwitterProducer(hashtags, queryType);
		
		Thread t1 = new Thread() {
			@Override
			public synchronized void run(){
				try{
					producer.activateTweetListening();
				}catch(Exception e){
					LOGGER.error(e.getMessage());
				}
			}
		};
		t1.setPriority(1);
		t1.start();
		TwitterConsumer consumer = new TwitterConsumer();
		//Will fetch first 50 tweets, can be modified to 
		//suppport pagination request
		List<Tweet> list = consumer.getTweets(0, 50, hashtag);	
		HttpHeaders headers = new HttpHeaders();
		headers.add("Responded", "TwitterController");
		return ResponseEntity.accepted().headers(headers).body(list);	

	}

}
