package com.dsp181.manager.manger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
//import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

//import org.omg.CORBA.PUBLIC_MEMBER;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
//import com.jayway.jsonpath.internal.function.numeric.Min;

import edu.stanford.nlp.util.Pair;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.*;
//import com.amazonaws.services.mturk.model.CreateWorkerBlockRequest;
//import com.amazonaws.services.mturk.model.transform.ReviewActionDetailJsonUnmarshaller;

/**
 * Hello world!
 *
 */
public class App {

	static SQS sqs;
	static S3 s3;
	static 	AmazonEC2 ec2;
	static double numberOfReviewsPerWorker;
	static Map<String, Pair<Integer,Map<String, Review>>> reviewsHashmap;
	static 	List<Instance> workersIntances;
	static boolean terminate;
	public static void main( String[] args ){
		sqs = new SQS();
		sqs.createQueue("workersQueue");

		terminate = false;

		s3 = new S3();

		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
		ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();

		//contain all review in the form of hashMap<key : "file key, value : pair< numberOfResloveRevies , hashMap<key : reviewId, value : reviewObj>>>		
		reviewsHashmap = new HashMap<String, Pair<Integer,Map<String, Review >>>();

		numberOfReviewsPerWorker=0;

		workersIntances = null;


		while(!terminate){
			HashMap<String, String> keysAndBucketsHashMap = retriveMessageFromLocalAppQueue();

			ArrayList<S3Object> localAppDownloadedInputFiles = retriveFilesFromS3(keysAndBucketsHashMap);

			int numberOfreviews = sendMessagesToWorkersQueue(localAppDownloadedInputFiles);

			//create workers
			int numberOfWorkersToCreate = (int)Math.ceil(numberOfreviews / numberOfReviewsPerWorker) - workersIntances.size();
			if(numberOfWorkersToCreate > 0){
				createWorkers(numberOfWorkersToCreate);
			}

			//update reviewsHashmap and write summary file if necessary
			if(terminate)
				while(reviewsHashmap.size() != 0)
					retriveMessageFromWorkersQueue();
			else
				retriveMessageFromWorkersQueue();
		}
	}


	////////////////////////////////////////////////////////////////////////////
	public static HashMap<String, String> retriveMessageFromLocalAppQueue(){
		HashMap<String, String> keysAndBucketsHashMap = new HashMap<String, String>();
		List<Message> messages = sqs.reciveMessages("localAppQueue");
		for(Message message:messages){
			if(message.getBody().split("###")[0].equals("fileMessage")) {
				keysAndBucketsHashMap.put(message.getBody().split("###")[1], message.getBody().split("###")[2]);
				numberOfReviewsPerWorker = Math.min(numberOfReviewsPerWorker, Integer.parseInt(message.getBody().split("###")[3]));
				// delete the message from queue
				sqs.deleteMessages(Collections.singletonList(message),"localAppQueue");
			}else if(message.getBody().equals("terminate")) {
				terminate = true;
				break;
			}

		}
		return keysAndBucketsHashMap;
	}

	public static ArrayList<S3Object> retriveFilesFromS3(HashMap<String, String> keysAndBucketsHashMap){
		try {
			return s3.downloadFiles(keysAndBucketsHashMap);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}


	public static int sendMessagesToWorkersQueue(ArrayList<S3Object> localAppDownloadedInputFiles){
		int numberOfreviews =0;
		BufferedReader reader = null;
		Gson gson = new Gson();
		JsonObject jsonObjLine;
		JsonArray jsonReviews;
		String sCurrentLine,movieTitle,reviewId,reviewText,reviewUrl;
		for(S3Object fileInputObject: localAppDownloadedInputFiles){
			reader = new BufferedReader(new InputStreamReader(fileInputObject.getObjectContent()));
			try {
				while ((sCurrentLine = reader.readLine()) != null) {

					jsonObjLine = gson.fromJson(sCurrentLine,JsonElement.class).getAsJsonObject();
					jsonReviews =  jsonObjLine.get("reviews").getAsJsonArray();

					movieTitle = fileInputObject.getBucketName()+"@@@" + fileInputObject.getKey();

					for(JsonElement review:  jsonReviews) {
						reviewId = ((JsonObject) review).get("id").toString();
						reviewText = ((JsonObject) review).get("text").toString();
						reviewUrl = ((JsonObject) review).get("link").toString();
						numberOfreviews++;

						// first movie review
						if(reviewsHashmap.get(movieTitle) == null){ 
							Map<String, Review> tempMap = new HashMap<String, Review>();
							tempMap.put(reviewId, new Review(reviewId,reviewText,reviewUrl,0,null));
							reviewsHashmap.put(movieTitle,new Pair<Integer,Map<String,Review>>(0,tempMap));
						}
						// movie already ready exist in the hashMap
						else{
							reviewsHashmap.get(movieTitle).second().put(reviewId,new Review(reviewId,reviewText,reviewUrl,0,null));
						}
						sqs.sendMessage("reviewMessage###" + movieTitle  + "###" + reviewId + "###" + reviewText,"workersQueue");
					}
				}

			} catch (JsonSyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				if(reader != null)
					try {
						reader.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
		}
		return numberOfreviews;
	}

	public static void createWorkers(int numberOfWorkersToCreate){
		try {
			RunInstancesRequest request = new RunInstancesRequest("ami-6057e21a", numberOfWorkersToCreate, numberOfWorkersToCreate);

			request.setInstanceType(InstanceType.T2Micro.toString());
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();

			for (Instance instance : instances) {
				workersIntances.add(instance);
				ArrayList<Tag> tags = new ArrayList<Tag>();
				tags.add(new Tag("worker" + workersIntances.size()));
				instance.setTags(tags);
			}
			System.out.println("Launch instances: " + instances);

		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}
	}

	public static void retriveMessageFromWorkersQueue(){
		List<Message>  messages = sqs.reciveMessages("workersQueue");
		String movieTitle,reviewId,reviewSentiment,reviewEntities;

		for(Message message:messages){
			if(message.getBody().split("###")[0].equals("reviewAnalyzeComplete")) {
				movieTitle = message.getBody().split("###")[1];
				reviewId = message.getBody().split("###")[2];
				reviewSentiment = message.getBody().split("###")[3];
				reviewEntities = message.getBody().split("###")[4];
				//update reviewsHashmap
				reviewsHashmap.get(movieTitle).second().get(reviewId).setEntitiesAndSentiment(reviewEntities,Integer.parseInt(reviewSentiment));
				reviewsHashmap.get(movieTitle).setFirst(reviewsHashmap.get(movieTitle).first() + 1 );
				// delete the message from queue
				sqs.deleteMessages(Collections.singletonList(message),"workersQueue");

				//check if all this movie reviews are analyzed and create file if so
				writeSummaryFileIfNeeded(movieTitle);

			}
		}

	}

	public static void writeSummaryFileIfNeeded(String movieTitle){
		try {
			if(reviewsHashmap.get(movieTitle).first() == reviewsHashmap.get(movieTitle).second().size()){
				JsonObject jsonPerReview = new JsonObject();
				PrintWriter writer = new PrintWriter(movieTitle + "@@@" + "complete.txt", "UTF-8");
				Iterator<Entry<String, Review>> it = reviewsHashmap.get(movieTitle).second.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<String, Review> pair = (Map.Entry<String, Review>)it.next();
					jsonPerReview = new JsonObject();
					jsonPerReview.addProperty("review",((Review)pair.getValue()).getReview());
					jsonPerReview.addProperty("entities",((Review)pair.getValue()).getEntities());
					jsonPerReview.addProperty("sentiment",((Review)pair.getValue()).getSentiment());
					jsonPerReview.addProperty("url",((Review)pair.getValue()).getUrl());
					writer.println(jsonPerReview);
				}
				writer.close();

				//upload summary file to S3
				ArrayList<String> fileKey = s3.uploadFiles(new String[] {movieTitle + "@@@" + "complete.txt"}, movieTitle.split("@@@")[0]);
				//send message to localApp containing the key of the summary file
				sqs.sendMessage("completeFileMessage###" + fileKey.get(0), "localAppQueue");
				reviewsHashmap.remove(movieTitle);

			}
		}    catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
