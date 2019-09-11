package com.app.twitterstreaming.controller;

import java.util.List;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.app.twitterstreaming.configuration.KafkaConfiguration;
import com.app.twitterstreaming.consumer.TwitterConsumer;
import com.app.twitterstreaming.model.Tweet;
import com.app.twitterstreaming.producer.TwitterClient;
import com.app.twitterstreaming.producer.TwitterProducer;


@RestController
public class TestController {

	@RequestMapping("/getTweets")

	public List<Tweet> home() {
		//TwitterClient client = new TwitterClient("mangal-mission");
		Thread t1 = new Thread() {
			public  synchronized void  run(){
	            try{
	            	TwitterProducer producer = new TwitterProducer("TheSkyIsPink");
	        		producer.activateTweetListening();
	            }catch(Exception e){
	                System.out.println("Exception"+e);
	            }
	        }
		};
		t1.start();
		TwitterConsumer consumer = new TwitterConsumer();
		
		return consumer.getTweets(0, 5, KafkaConfiguration.TOPIC);
		
	}
}
