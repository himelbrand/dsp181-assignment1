package com.dsp181.manager.manger;

import java.util.HashMap;
import java.util.Map;

public class InputFile {
	int numberOfAnalyzedReviews;
	Map<String, Review> reviewsHashMap;
	String uuid;
	int numberOfFilesPerWorker =0;
	
	
	public InputFile(int numberOfAnalyzedReviews, String uuid,
			int numberOfFilesPerWorker) {
		super();
		this.numberOfAnalyzedReviews = numberOfAnalyzedReviews;
		this.reviewsHashMap = new HashMap<String, Review>();
		this.uuid = uuid;
		this.numberOfFilesPerWorker = numberOfFilesPerWorker;
	}
	
	public int getNumberOfAnalyzedReviews() {
		return numberOfAnalyzedReviews;
	}
	public void setNumberOfAnalyzedReviews(int numberOfAnalyzedReviews) {
		this.numberOfAnalyzedReviews = numberOfAnalyzedReviews;
	}
	public void incNumberOfAnalyzedReviews() {
		this.numberOfAnalyzedReviews++;
	}
	public Map<String, Review> getReviewsHashMap() {
		return reviewsHashMap;
	}
	public void setReviewsHashMap(Map<String, Review> reviewsHashMap) {
		this.reviewsHashMap = reviewsHashMap;
	}
	public String getUuid() {
		return uuid;
	}
	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	public int getNumberOfFilesPerWorker() {
		return numberOfFilesPerWorker;
	}
	public void setNumberOfFilesPerWorker(int numberOfFilesPerWorker) {
		this.numberOfFilesPerWorker = numberOfFilesPerWorker;
	}


		
		
	
}
