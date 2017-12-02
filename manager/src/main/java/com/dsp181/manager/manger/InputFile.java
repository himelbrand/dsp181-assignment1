package com.dsp181.manager.manger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InputFile {
	AtomicInteger numberOfAnalyzedReviews= new AtomicInteger(0);
	ConcurrentHashMap<String, Review> reviewsHashMap = null;
	String uuid ="";
	int numberOfFilesPerWorker =0;
	boolean doneSending = false;
	
	public InputFile(int numberOfAnalyzedReviews, String uuid,
			int numberOfFilesPerWorker) {
		super();
		this.numberOfAnalyzedReviews = new AtomicInteger(numberOfAnalyzedReviews);
		this.reviewsHashMap = new ConcurrentHashMap<String, Review>();
		this.uuid = uuid;
		this.numberOfFilesPerWorker = numberOfFilesPerWorker;
	}
	public void setDoneSending(boolean doneSending) {
		this.doneSending = doneSending;
	}
	public boolean isDoneSending() {
		return doneSending;
	}
	public int getNumberOfAnalyzedReviews() {
		return numberOfAnalyzedReviews.get();
	}
	public void setNumberOfAnalyzedReviews(int numberOfAnalyzedReviews) {
		this.numberOfAnalyzedReviews.set(numberOfAnalyzedReviews);
	}
	public void incNumberOfAnalyzedReviews() {
		this.numberOfAnalyzedReviews.incrementAndGet();
	}
	public Map<String, Review> getReviewsHashMap() {
		return reviewsHashMap;
	}
	public void setReviewsHashMap(ConcurrentHashMap<String, Review> reviewsHashMap) {
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
