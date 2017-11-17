package com.dsp181.manager.manger;


public class Review {
	private String id;
	private String review;
	private int sentiment;
	private String entities;
	public Review(String id,String review,int sentiment,String entities)
	{
		this.id = id;
		this.review = review;
		this.sentiment = sentiment;
		this.entities = entities;
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
	
}
