package com.app.twitterstreaming.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.app.twitterstreaming.producer.TwitterProducer;

@RestController
public class TestController {

	@RequestMapping("/")

	public String home() {
		TwitterProducer producer = new TwitterProducer();
		producer.run();
		return null;
	}
}
