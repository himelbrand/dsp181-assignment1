package com.dsp181.manager.manger;


public class Review {
	private String id;
	private String review;
	private String url;
	private int sentiment = -1;
	private String entities = null;
	private int rating = 0;
	public Review(String id,String review,String url,int sentiment,int rating)
	{
		this.url = url;
		this.rating = rating;
		this.id = id;
		this.review = review;
		this.sentiment = sentiment;
		this.entities = null;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public void setEntities(String entities) {
		this.entities = entities;
	}
	public void setReview(String review) {
		this.review = review;
	}
	public void setId(String id) {
		this.id = id;
	}
	public void setSentiment(int sentiment) {
		this.sentiment = sentiment;
	}
	
	public void setEntitiesAndSentiment(String entities,int sentiment){
		setEntities(entities);
		setSentiment(sentiment);
	}
	public String getEntities() {
		return entities;
	}
	public String getId() {
		return id;
	}
	public String getReview() {
		return review;
	}
	public int getSentiment() {
		return sentiment;
	}
	public int getRating() {
		return rating;
	}
	public void setRating(int rating) {
		this.rating = rating;
	}
	
}
