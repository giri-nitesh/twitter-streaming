package com.app.twitterstreaming.producer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import com.app.twitterstreaming.configuration.TwitterConfiguration;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/** Represents a TwitterClient
 * @author Nitesh
*/
public class TwitterClient {
	
	private StatusesFilterEndpoint endpoint;
	
	private BlockingQueue<String> queue;
	
	private Authentication authentication;

	/** Creates an Twitter Client with the given list of string(or hashtags) to track.
	 * @param list List of terms(or Hashtags).
	 * To get a good twitter client, a list of terms or a string should be passed as parameter
	*/
	public TwitterClient(List<String> list) {
		// Configure Authentication
		// Authentication auth = new BasicAuth(username, password);
		authentication = new OAuth1(
				TwitterConfiguration.CONSUMER_KEY,
				TwitterConfiguration.CONSUMER_SECRET,
				TwitterConfiguration.ACCESS_TOKEN,
				TwitterConfiguration.TOKEN_SECRET);

		// Define our endpoint: By default, delimited=length is set (we need this for our processor)
		// and stall warnings are on.

		endpoint = new StatusesFilterEndpoint();

		// Track the items with hashtag or some terms
		// endpoint.trackTerms(Lists.newArrayList("twitterapi", "#yolo"));

		endpoint.trackTerms(list);

		// Create an appropriately sized blocking queue

		queue = new LinkedBlockingQueue<>(10000);

	}
	
	/** Creates an Twitter Client with the given string(or hashtag).
	 * @param term A string(or Hashtag) to track.
	 * To get a good twitter client, a list of string(or hashtags) or a string 
	 * should be passed as parameter.
	*/
	public TwitterClient(String term) {
		// Configure Authentication
		// Authentication auth = new BasicAuth(username, password);
		authentication = new OAuth1(
				TwitterConfiguration.CONSUMER_KEY,
				TwitterConfiguration.CONSUMER_SECRET,
				TwitterConfiguration.ACCESS_TOKEN,
				TwitterConfiguration.TOKEN_SECRET);

		// Define our endpoint: By default, delimited=length is set (we need this for our processor)
		// and stall warnings are on.

		endpoint = new StatusesFilterEndpoint();

		// Track the items with hashtag or some terms
		// endpoint.trackTerms(Lists.newArrayList("twitterapi", "#yolo"));

		endpoint.trackTerms(Collections.singletonList(term));

		// Create an appropriately sized blocking queue

		queue = new LinkedBlockingQueue<>(10000);

	}
	
	/** Gets the Twitter Client
	 * @return A Twitter Client with the given list of strings(or Hashtags) or 
	 * string(or hashtag).
	*/
	public Client getClient(){
		return new ClientBuilder()
				.hosts(Constants.STREAM_HOST)
				.authentication(authentication)
				.endpoint(endpoint)
				.processor(new StringDelimitedProcessor(queue))
				.build();
	}
}
