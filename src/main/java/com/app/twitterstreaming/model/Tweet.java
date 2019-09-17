package com.app.twitterstreaming.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.annotations.SerializedName;

public class Tweet {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Tweet.class);	
    private long id;
    private String text;
    private String lang;
    private User user;

    @SerializedName("retweet_count")
    private int retweetCount;

    @SerializedName("favorite_count")
    private int favoriteCount;

    public Tweet(long id, String text, String lang, User user, int retweetCount, int favoriteCount) {
        this.id = id;
        this.text = text;
        this.lang = lang;
        this.user = user;
        this.retweetCount = retweetCount;
        this.favoriteCount = favoriteCount;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public int getRetweetCount() {
        return retweetCount;
    }

    public void setRetweetCount(int retweetCount) {
        this.retweetCount = retweetCount;
    }

    public int getFavoriteCount() {
        return favoriteCount;
    }

    public void setFavoriteCount(int favoriteCount) {
        this.favoriteCount = favoriteCount;
    }
    
    /*Following toString message puts the tweets into kafka queue
     * before converting to json object, so that it can be 
     * consumed to tweet object easily.
     * */
    @Override
    public String toString() {
    	ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    	String json = "";
		try {
			json = ow.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			LOGGER.error(e.toString());
		}
    	return json;
    	
    }
}
