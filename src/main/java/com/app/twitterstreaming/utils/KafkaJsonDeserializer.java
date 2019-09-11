package com.app.twitterstreaming.utils;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJsonDeserializer<T> implements Deserializer<Object> {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonDeserializer.class);

    private Class <T> type;

    public KafkaJsonDeserializer(Class type) {
        this.type = type;
    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        T obj = null;
        try {
            obj = mapper.readValue(bytes, type);
        } catch (Exception e) {

        	LOGGER.error(e.getMessage());
        }
        return obj;
    }

    @Override
    public void close() {

    }

	@Override
	public void configure(Map configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}
}
