package com.app.twitterstreaming.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.app.twitterstreaming.configuration.KafkaConfiguration;
import com.app.twitterstreaming.constant.AppConstants.QueryConstants;
import com.app.twitterstreaming.model.Tweet;
import com.app.twitterstreaming.service.KafkaProducerService;
import com.google.gson.Gson;
import com.twitter.hbc.httpclient.BasicClient;

/** Represents a TwitterProducer
 * @author Nitesh
 */
public class TwitterProducer implements KafkaProducerService{

	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class);

	private BasicClient client;
	private BlockingQueue<String> queue;
	private Gson gson;
	private List<String> list;
	@Autowired
	private Callback callback;

	/** Creates a Twitter Producer with the given term to be tracked.
	 * @param term Term(or hashtag) to be tracked.
	 */
	public TwitterProducer(String term, QueryConstants queryType) {
		LOGGER.info("Twitter Client created for Hashtag: #{}",term);
		list = new ArrayList<>();
		list.add(term);
		gson = new Gson();
		queue = new LinkedBlockingQueue<>(10000);
		TwitterClient twitterClient= new TwitterClient(term, queue);
		this.client = twitterClient.getClient(queryType);
		LOGGER.info("Client created is: {}", client);
	}

	/** Creates a Twitter Producer with the list of terms to be tracked.
	 * @param terms Terms(or hashtags) to be tracked.
	 */
	public TwitterProducer(List<String> terms, QueryConstants queryType) {
		LOGGER.info("Twitter Client created for Hashtags: {}",terms);
		list = terms;
		gson = new Gson();
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
	public void activateTweetListening() throws Exception{

		LOGGER.info("Creating connection to Twitter..");
		client.connect();
		LOGGER.info("Connected to Twitter stream");
		Producer<Long, String> producer = getProducer();
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			if (client.isDone()) {
				LOGGER.info(client.getExitEvent().getMessage());
				break;
			}

			String msg="";
			try{
				msg = queue.poll(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			Tweet tweet = gson.fromJson(msg, Tweet.class);
			LOGGER.info("Fetched tweet id {}", tweet.getId());
			long key = tweet.getId();
			LOGGER.info("Putting msg into queue {}", tweet.getText());
			ProducerRecord<Long, String> record = new ProducerRecord<>(list.get(0), key, msg);
			producer.send(record, callback);
		}
		client.stop();
	}
}
