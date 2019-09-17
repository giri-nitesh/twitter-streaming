package com.app.twitterstreaming.configuration;

public class KafkaConfiguration {
	
	private KafkaConfiguration() {}
	
    public static final String SERVERS = "localhost:9092";
    public static final String TOPIC = "TheSkyIsPink";
    public static final long SLEEP_TIMER = 1000;
}
