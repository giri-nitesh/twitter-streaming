package com.app.twitterstreaming.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.app.twitterstreaming.configuration.TwitterConfiguration;
import com.app.twitterstreaming.constant.AppConstants.QueryConstants;
import com.app.twitterstreaming.utils.Utility;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
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
		list = new ArrayList<>();
		list.add(term);
		this.queue = queue;
	}

	/** Gets the Twitter Client
	 * @return A Twitter Client with the given list of strings(or Hashtags) or 
	 * string(or hashtag).
	 */
	public BasicClient getClient(QueryConstants queryType){
		// Configure Authentication
		authentication = new OAuth1(
				TwitterConfiguration.CONSUMER_KEY,
				TwitterConfiguration.CONSUMER_SECRET,
				TwitterConfiguration.ACCESS_TOKEN,
				TwitterConfiguration.TOKEN_SECRET);
		
		// Define our endpoint: By default, delimited=length is set (we need this for our processor)
		// and stall warnings are on.

		endpoint = new StatusesFilterEndpoint();

		// Track the tweets with hashtag or some terms

		if(queryType == QueryConstants.HASHTAG) {
			endpoint.trackTerms(list);
			LOGGER.info("Hashtag registered with listener service");
			}
		
		// Track the tweets with set of account ids
		// Convert the list of string params to list of ids 
		if(queryType == QueryConstants.ACCOUNT) {
			List<Long> accounts = new ArrayList<>();
			for(String account : list) {
				Long item = new Long(Utility.TWEET_ID.get(account));
				accounts.add(item);
			}
			endpoint.followings(accounts);
			LOGGER.info("User ids registered with listener service");
		}
		
		
		LOGGER.info("Endpoint: {} and Authenticaion: {}", getEndpoint(), getAuthentication());
		BasicClient client = new ClientBuilder()
				.name("TwitterClient")
				.hosts(Constants.STREAM_HOST)
				.authentication(authentication)
				.endpoint(endpoint)
				.processor(new StringDelimitedProcessor(queue))
				.build();
		LOGGER.info("Twitter client created successfully with following configuration: {}", 
				this);
		return client;
	}

	public StatusesFilterEndpoint getEndpoint() {
		return this.endpoint;
	}

	public BlockingQueue<String> getQueue() {
		return this.queue;
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
