package com.app.twitterstreaming.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class TestController {

	@RequestMapping("/")

	public String home() {
		
		return null;
	}
}
