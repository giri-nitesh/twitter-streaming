package com.app.twitterstreaming.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.app.twitterstreaming.configuration.KafkaConfiguration;
import com.app.twitterstreaming.constant.AppConstants.QueryConstants;
import com.app.twitterstreaming.model.Tweet;
import com.app.twitterstreaming.service.KafkaProducerService;
import com.google.gson.Gson;
import com.twitter.hbc.core.Client;

/** Represents a TwitterProducer
 * @author Nitesh
*/
public class TwitterProducer implements KafkaProducerService{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);
	
    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;
    private Callback callback;
    
    /** Creates a Twitter Producer with the given term to be tracked.
	 * @param term Term(or hashtag) to be tracked.
	*/
    public TwitterProducer(String term, QueryConstants queryType) {
    	LOGGER.info("Twitter Client created for Hashtag: #",term);
    	
    	gson = new Gson();
        callback = new BasicCallback();
        queue = new LinkedBlockingQueue<>(10000);
        TwitterClient twitterClient= new TwitterClient(term, queue);
        this.client = twitterClient.getClient(queryType);
        LOGGER.info("Client created is:" +client);
    }
    
    /** Creates a Twitter Producer with the list of terms to be tracked.
	 * @param terms Terms(or hashtags) to be tracked.
	*/
    public TwitterProducer(List<String> terms, QueryConstants queryType) {
    	LOGGER.info("Twitter Client created for Hashtags:",terms.toString());
    	
    	gson = new Gson();
        callback = new BasicCallback();
        queue = new LinkedBlockingQueue<>(10000);
        this.client = new TwitterClient(terms, queue).getClient(queryType);
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
        
    	LOGGER.info("Creating connection to Twitter..");
    	client.connect();
    	LOGGER.info("Connected to Twitter stream");
        
        try (Producer<Long, String> producer = getProducer()) {
            while (true) {
            	// Process the messages in the queue 
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                LOGGER.info("Fetched tweet id ", tweet.getId());
                long key = tweet.getId();
                String msg = tweet.toString();
                //LOGGER.info("Putting msg into queue "+ msg);
                ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaConfiguration.TOPIC, key, msg);
                producer.send(record, callback);
            }
        } 
        catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
        } finally {
        	LOGGER.error("Twitter Client stopped due to some exception.");
            client.stop();
        }
    }
}
