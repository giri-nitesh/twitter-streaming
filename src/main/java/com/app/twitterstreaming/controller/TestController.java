package com.app.twitterstreaming.controller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.app.twitterstreaming.configuration.KafkaConfiguration;
import com.app.twitterstreaming.consumer.TwitterConsumer;
import com.app.twitterstreaming.model.Tweet;
import com.app.twitterstreaming.producer.TwitterProducer;
import com.app.twitterstreaming.utils.ZooKeeperUtility;



@RestController
public class TestController {

	@RequestMapping("/getTweets/5")

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
	@GetMapping(value = "/create-topic")
	public void createKafkaTopic(@RequestParam("topic") String topic) throws FileNotFoundException, IOException {
		ZooKeeperUtility.createTopic(topic);
		

	}
}
