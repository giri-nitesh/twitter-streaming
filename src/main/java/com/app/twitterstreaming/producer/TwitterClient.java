package com.app.twitterstreaming.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.app.twitterstreaming.configuration.TwitterConfiguration;
import com.app.twitterstreaming.constant.AppConstants.QueryConstants;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.BasicAuth;
import com.twitter.hbc.httpclient.auth.OAuth1;

/** Represents a TwitterClient
 * @author Nitesh
 */
public class TwitterClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterClient.class);
	private StatusesFilterEndpoint endpoint;
	private BlockingQueue<String> queue;
	private Authentication authentication;
	private List<String> list;

	/** Creates an Twitter Client with the given list of string(or hashtags) to track.
	 * @param list List of terms(or Hashtags).
	 * To get a good twitter client, a list of terms or a string should be passed as parameter
	 */
	public TwitterClient(List<String> list, BlockingQueue<String> queue) {
		this.queue = queue;
		this.list = list;
	}

	/** Creates a Twitter Client with the given string(or hashtag).
	 * @param term A string(or Hashtag) to track.
	 * To get a good twitter client, a list of string(or hashtags) or a string 
	 * should be passed as parameter.
	 */
	public TwitterClient(String term, BlockingQueue<String> queue) {
		//new TwitterClient(Collections.singletonList(term), queue); 
		List<String> list = new ArrayList<String>();
		list.add(term);
		this.list = list;
		this.queue = queue;
	}

	/** Gets the Twitter Client
	 * @return A Twitter Client with the given list of strings(or Hashtags) or 
	 * string(or hashtag).
	 */
	public Client getClient(QueryConstants queryType){
		// Configure Authentication
		//Authentication auth = new BasicAuth("k_g_nitesh", "");
		authentication = new OAuth1(
				TwitterConfiguration.CONSUMER_KEY,
				TwitterConfiguration.CONSUMER_SECRET,
				TwitterConfiguration.ACCESS_TOKEN,
				TwitterConfiguration.TOKEN_SECRET);
		
		// Define our endpoint: By default, delimited=length is set (we need this for our processor)
		// and stall warnings are on.

		endpoint = new StatusesFilterEndpoint();

		// Track the tweets with hashtag or some terms
		// endpoint.trackTerms(Lists.newArrayList("twitterapi", "#yolo"));
		if(queryType == QueryConstants.HASHTAG) {
			endpoint.trackTerms(list);
			LOGGER.info("Hashtag registered with listener service");
			}
		
		// Track the tweets with set of account ids
		// Convert the list of string params to list of ids 
		if(queryType == QueryConstants.ACCOUNT) {
			List<Long> accounts = new ArrayList<Long>();
			for(String account : list) {
				Long item = Long.getLong(account);
				accounts.add(item);
			}
			endpoint.followings(accounts);
			LOGGER.info("User ids registered with listener service");
		}
		
		
		System.out.println("Endpoint: "+ getEndpoint());
		System.out.println("Authentication: "+ getAuthentication());
		Client client = new ClientBuilder()
				.hosts(Constants.STREAM_HOST)
				.authentication(authentication)
				.endpoint(endpoint)
				.processor(new StringDelimitedProcessor(queue))
				.build();
		LOGGER.info("Twitter client created successfully with following configuration:", 
				this.toString());
		return client;
	}

	public StatusesFilterEndpoint getEndpoint() {
		return this.endpoint;
	}

	public BlockingQueue<String> getQueue() {
		return queue;
	}

	public Authentication getAuthentication() {
		return authentication;
	}

	@Override
	public String toString() {
		return "TwitterClient [queue=" + queue + ", authentication=" + authentication + ", "
				+ "endpoint=" + endpoint + "]";
	}	


}
