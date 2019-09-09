package com.app.twitterstreaming.producer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.app.twitterstreaming.configuration.KafkaConfiguration;
import com.app.twitterstreaming.model.Tweet;
import com.app.twitterstreaming.service.KafkaProducerService;
import com.google.gson.Gson;
import com.twitter.hbc.core.Client;

/** Represents a TwitterProducer
 * @author Nitesh
*/
public class TwitterProducer implements KafkaProducerService{
    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;
    private Callback callback;
    
    /** Creates an Twitter Producer with the given twitter client.
	 * @param client Twitter Client to get the tweets.
	*/
    public TwitterProducer(Client client) {
    	this.client = client;
    }

    private Producer<Long, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    /** Activates the producer by fetching tweets from Twitter
     * and putting tweets into kafka queue.
	*/
    @Override
    public void activateTweetListening() {
        client.connect();
        try (Producer<Long, String> producer = getProducer()) {
            while (true) {
            	// Process the messages in the queue 
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                System.out.printf("Fetched tweet id %d\n", tweet.getId());
                long key = tweet.getId();
                String msg = tweet.toString();
                ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaConfiguration.TOPIC, key, msg);
                producer.send(record, callback);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }
}
