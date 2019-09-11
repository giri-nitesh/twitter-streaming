package com.app.twitterstreaming.utils;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJsonSerializer implements Serializer<Object> {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonSerializer.class);

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsBytes(o);
        } catch (Exception e) {
        	LOGGER.error(e.getMessage());
        }
        return retVal;
    }

    @Override
    public void close() {

    }

	@Override
	public void configure(Map configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}
}