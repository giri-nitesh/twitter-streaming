package com.app.twitterstreaming.controller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.app.twitterstreaming.constant.AppConstants.QueryConstants;
import com.app.twitterstreaming.consumer.TwitterConsumer;
import com.app.twitterstreaming.model.Tweet;
import com.app.twitterstreaming.producer.TwitterProducer;
import com.app.twitterstreaming.utils.ZooKeeperUtility;

@RestController
@RequestMapping("/tweet")
public class TwitterController {

	@RequestMapping(value="/create-topic/{topic}" , method = RequestMethod.POST)
	public ResponseEntity<String> getKafkaTopic(@RequestParam("topic") String topic) 
			throws FileNotFoundException, IOException {
		ZooKeeperUtility.createTopic(topic);
		return new ResponseEntity<String>(HttpStatus.OK);
	}

	@RequestMapping(value="/{id}" , method = RequestMethod.GET)
	public ResponseEntity<List<Tweet>> getTweetsForTwitterAccount(@RequestParam("id") Long id) {
		Thread t1 = new Thread() {
			public  synchronized void  run(){
				try{
					List<String> accounts = new ArrayList<>();
					accounts.add(id.toString());
					TwitterProducer producer = new TwitterProducer(accounts, QueryConstants.ACCOUNT);
					producer.activateTweetListening();
				}catch(Exception e){
					System.out.println("Exception"+e);
				}
			}
		};
		t1.start();
		TwitterConsumer consumer = new TwitterConsumer();

		List<Tweet> list = consumer.getTweets(0, 5, id.toString());	
		HttpHeaders headers = new HttpHeaders();
		headers.add("Responded", "TwitterController");
		return ResponseEntity.accepted().headers(headers).body(list);	
	}

	@RequestMapping(value="/{hashtag}" , method = RequestMethod.GET)
	public ResponseEntity<List<Tweet>> getTweetsForHashtag(@RequestParam("hashtag") String hashtag) {
		Thread t1 = new Thread() {
			public  synchronized void  run(){
				try{
					List<String> hashtags = new ArrayList<>();
					hashtags.add(hashtag);
					TwitterProducer producer = new TwitterProducer(hashtags, QueryConstants.ACCOUNT);
					producer.activateTweetListening();
				}catch(Exception e){
					System.out.println("Exception"+e);
				}
			}
		};
		t1.start();
		TwitterConsumer consumer = new TwitterConsumer();
		List<Tweet> list = consumer.getTweets(0, 5, hashtag);	
		HttpHeaders headers = new HttpHeaders();
		headers.add("Responded", "TwitterController");
		return ResponseEntity.accepted().headers(headers).body(list);	

	}

}
