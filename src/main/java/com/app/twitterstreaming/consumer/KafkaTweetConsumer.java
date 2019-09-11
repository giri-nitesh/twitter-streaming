package com.app.twitterstreaming.consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.app.twitterstreaming.configuration.KafkaConfiguration;

/**
 * 
 * @author Nitesh
 * Represents a Kafka Consumer.
 *
 */
public class KafkaTweetConsumer {
	
    private final static String TOPIC = KafkaConfiguration.TOPIC;
    
    public KafkaTweetConsumer() {
		
	}
	
    public KafkaConsumer<String, String> createConsumer() {

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        		KafkaConfiguration.SERVERS);

        props.put(ConsumerConfig.GROUP_ID_CONFIG,
        		"KafkaExampleConsumer");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        		LongDeserializer.class.getName());

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        		StringDeserializer.class.getName());

        // Create the consumer using props.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));

        return consumer;

    }

}
