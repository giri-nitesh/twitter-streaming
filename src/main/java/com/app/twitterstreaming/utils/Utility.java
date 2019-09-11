package com.app.twitterstreaming.utils;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Utility {

	private static final Logger LOGGER = LoggerFactory.getLogger(Utility.class);
	
	public static Map<String, String> kafkaMessageToMap(String message){
		Map<String, String> map = new HashMap<>();
		message = message.substring(6);
		String pairs[] = message.split("{*.}");
		for(String entry : pairs) {
			String pair[] = entry.split("=");
			String key = pair[0].trim();
			String value = pair[1].trim();
			try {
				map.put(key, value);
				System.out.println(key + " "+ value);
			}
			catch(Exception e) {
				LOGGER.error(e.toString());
			}
		}
		return map;
	}

}
