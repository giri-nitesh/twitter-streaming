package com.app.twitterstreaming.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicCallback implements Callback {
    
	private static final Logger LOGGER = LoggerFactory.getLogger(BasicCallback.class);
	@Override
    /**
     * Callback method to check if message sent by producer was 
     * put into kafka queue successfully.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            LOGGER.info("Message with offset %s acknowledged by partition %s\n",
                    metadata.offset(), metadata.partition());
        } else {
            LOGGER.error(exception.getMessage());
        }
    }
}
