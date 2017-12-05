package com.dsp181.manager.manger;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
//import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
//import org.omg.CORBA.PUBLIC_MEMBER;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.cloudfront.model.StreamingDistribution;
import com.amazonaws.services.directconnect.model.NewBGPPeer;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.kinesisanalytics.model.Input;

/**
 * Hello world!
 *
 */
class ReciverLocalAppQueue extends  Thread {
	@Override
	public void run() {
		while(!App.terminateLocalAppReciver){
			HashMap<String, String> keysAndBucketsHashMap = App.retriveMessageFromLocalAppQueue();
			App.localAppDownloadedInputFiles.addAll(App.retriveFilesFromS3Local(keysAndBucketsHashMap));
		}
		App.terminateWorkersSender = true;
		App.latch.countDown();

	}
}

class SenderWorkerQueue extends  Thread {
	@Override
	public void run() {
		while(!App.terminateWorkersSender || App.localAppDownloadedInputFiles.size() > 0){
			App.sendMessagesToWorkersQueue();
		}
		App.latch.countDown();
	}
}

class ReceiverWorkerQueue extends  Thread {

	ExecutorService executor = Executors.newFixedThreadPool(5);
	@Override
	public void run() {
		while(!App.terminateWorkersSender || App.inputFileHashmap.size() > 0){
			if(App.inputFileHashmap.size() > 0 && App.receiverCount.get() < 6){
				executor.execute(new ReceiverWorkerQueueSolo());
				App.receiverCount.incrementAndGet();
			}
		}
		App.executor.shutdown();
		while (!App.executor.isTerminated()) {
		}
		App.latch.countDown();

	}
}

class ReceiverWorkerQueueSolo extends  Thread {
	@Override
	public void run() {
		App.retriveMessageFromWorkersQueue();
		App.receiverCount.decrementAndGet();
	}
}

class SenderLocalAppQueue extends  Thread {
	private String inputFileKey;
	public SenderLocalAppQueue(String inputFileKey) {
		super();
		this.inputFileKey = inputFileKey;
	}
	@Override
	public void run() {
		App.writeSummaryFileIfNeeded(this.inputFileKey);
	}
}

public class App {

	static AtomicInteger receiverCount = new AtomicInteger(0);
	static SQS sqs;
	static S3 s3;
	static 	AmazonEC2 ec2;
	static AtomicInteger NumWorkersInstancesIds = new AtomicInteger(0);
	static ConcurrentHashMap<String, InputFile> inputFileHashmap = new ConcurrentHashMap<String, InputFile>();

	static HashMap<Integer,   HashMap<String, AtomicInteger>> workersLogs = new HashMap<Integer , HashMap<String, AtomicInteger>>();
	static boolean terminateLocalAppReciver=false,terminateWorkersSender=false,terminateFinal=false;
	static Object lock = new Object();
	static ConcurrentLinkedQueue<String> inputFileKeyArrayList = new ConcurrentLinkedQueue<String>(); 
	static ConcurrentLinkedQueue<String> localAppDownloadedInputFiles = new ConcurrentLinkedQueue<String>(); 
	static ArrayList<Thread> threadArrayList = new ArrayList<Thread>();;
	static ExecutorService executor = Executors.newFixedThreadPool(1);
	static CountDownLatch latch = new CountDownLatch(3);
	static String managerToWorkersQueue = "managerToWorkersQueue",workersToManagerQueue="workersToManagerQueue",localAppToManagerQueue="localAppToManagerQueue.fifo",managerTolocalAppQueue="managerTolocalAppQueue-";
	static Runtime runtime = Runtime.getRuntime();
	static int mb = 1024*1024;
	public static void main( String[] args ){

		BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAIPQVA435AAQCCUIQ", "M3OyJZdbJjb6DRL5pHCglZk2mFYh7DLcQ46JJaik");
		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(awsCreds);

		sqs = new SQS();
		sqs.launch(credentialsProvider);
		sqs.createQueue(managerToWorkersQueue);
		sqs.createQueue(workersToManagerQueue);

		s3 = new S3();
		s3.launch(credentialsProvider);

		ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();

		NumWorkersInstancesIds = new AtomicInteger(getWorkersInstancesIds().size());
		threadArrayList.add(new ReciverLocalAppQueue());
		threadArrayList.get(0).setPriority(Thread.MAX_PRIORITY);
		threadArrayList.get(0).start();
		threadArrayList.add(new SenderWorkerQueue());
		threadArrayList.get(1).start();
		threadArrayList.add(new ReceiverWorkerQueue());
		threadArrayList.get(2).start();

		try {
			System.out.println("\nlatch enter await\n");
			latch.await();
			System.out.println("\nlatch exit await\n");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();}

		ArrayList<String> workersInstancesIds = getWorkersInstancesIds();
		TerminateInstancesRequest terminateInstancesRequest;
		TerminateInstancesResult terminateInstancesResult;
		if(!workersInstancesIds.isEmpty()){
			terminateInstancesRequest = new TerminateInstancesRequest();
			terminateInstancesRequest.setInstanceIds(workersInstancesIds);
			terminateInstancesResult = ec2.terminateInstances(terminateInstancesRequest);


			//Wait for instances to terminate
			DescribeInstancesResult terminatedInstacesIds = ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(workersInstancesIds));
			for(InstanceStateChange instance :terminateInstancesResult.getTerminatingInstances())
			{
				Instance myInstance  = ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(instance.getInstanceId())).getReservations().get(0).getInstances().get(0);
				while (!myInstance.getState().getName().equals("terminated")) {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println(instance.getCurrentState().getName()  + " " + instance.getInstanceId());
					myInstance  = ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(instance.getInstanceId())).getReservations().get(0).getInstances().get(0);
				}
			}
		}
		ArrayList<String> managerIds = getManagerInstancesIds();
		if(!managerIds.isEmpty()){
			terminateInstancesRequest = new TerminateInstancesRequest();
			terminateInstancesRequest.setInstanceIds(getManagerInstancesIds());
			terminateInstancesResult = ec2.terminateInstances(terminateInstancesRequest);
		}

		System.out.println("\n!!!manager terminated !!!\n");
	}

	////////////////////////////////////////////////////////////////////////////

	private static ArrayList<String>  getWorkersInstancesIds(){
		ArrayList<String> instancesIds = new ArrayList<String>();

		DescribeInstancesRequest request = new DescribeInstancesRequest();
		List<String> filters = new ArrayList<String>();
		filters.add("worker");
		Filter filter = new Filter("tag-value", filters);

		filters = new ArrayList<String>();
		filters.add("running");
		Filter filter2 = new Filter("instance-state-name", filters);
		DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter,filter2));
		List<Reservation> reservations = result.getReservations();
		for (Reservation reservation : reservations) {
			List<Instance> instances = reservation.getInstances();
			for (Instance instance : instances) {
				instancesIds.add(instance.getInstanceId());
			}
		}
		return instancesIds;
	}

	private static ArrayList<String>  getManagerInstancesIds(){
		ArrayList<String> instancesIds = new ArrayList<String>();

		DescribeInstancesRequest request = new DescribeInstancesRequest();
		List<String> filters = new ArrayList<String>();
		filters.add("manager");
		Filter filter = new Filter("tag-value", filters);
		DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter));
		List<Reservation> reservations = result.getReservations();
		for (Reservation reservation : reservations) {
			List<Instance> instances = reservation.getInstances();
			for (Instance instance : instances) {
				instancesIds.add(instance.getInstanceId());
			}
		}
		return instancesIds;
	}

	public static HashMap<String, String> retriveMessageFromLocalAppQueue(){
		HashMap<String, String> keysAndBucketsHashMap = new HashMap<String, String>();
		List<Message> messages = sqs.reciveMessagesFifoQueue(localAppToManagerQueue);
		Map<String,MessageAttributeValue> messageAttributes = null;
		String inputFileKey,fileKey,bucketName,numberOfFilesPerWorker,UUID;
		for(Message message:messages){
			if(!message.getBody().equals("terminate")) {
				messageAttributes = message.getMessageAttributes();
				fileKey = messageAttributes.get("fileKey").getStringValue();
				bucketName = messageAttributes.get("bucketName").getStringValue();
				numberOfFilesPerWorker = messageAttributes.get("numberOfFilesPerWorker").getStringValue();
				UUID = messageAttributes.get("UUID").getStringValue();
				inputFileKey = bucketName +"@@@" + fileKey;

				System.out.println();
				System.out.println("retrive messages from localappqueue , inputFileKey:" + inputFileKey);
				inputFileHashmap.put(inputFileKey,new InputFile(0, UUID, Integer.parseInt(numberOfFilesPerWorker)));

				keysAndBucketsHashMap.put(fileKey,bucketName);
				//numberOfReviewsPerWorker = Math.min(numberOfReviewsPerWorker, Integer.parseInt(message.getBody().split("###")[3]));
				// delete the message from queue
				sqs.deleteMessages(Collections.singletonList(message),localAppToManagerQueue);
			}else{
				System.out.println("retrive messages from localappqueue -----------------------  T E R M I N A T E -----------------------");
				terminateLocalAppReciver = true;
				sqs.deleteMessages(Collections.singletonList(message),localAppToManagerQueue);
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

	public static ArrayList<String> retriveFilesFromS3Local(HashMap<String, String> keysAndBucketsHashMap){
		try {
			return s3.downloadFilesLocal(keysAndBucketsHashMap);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}


	public static void sendMessagesToWorkersQueue(){
		int numberOfreviews =0;
		int numberOfWorkersToCreate=0;
		Gson gson = new Gson();
		JsonObject jsonObjLine;
		JsonArray jsonReviews;
		String inputFileKey,reviewId,reviewText,reviewUrl;
		int reviewRating = 0;
		SendMessageRequest sendMessageRequest = null;
		HashMap<String,MessageAttributeValue> messageAttributes = null;
		SendMessageBatchRequestEntry entry = null;
		ArrayList<SendMessageBatchRequestEntry> entries = null;
		while(!localAppDownloadedInputFiles.isEmpty()){
			String fileInputString = localAppDownloadedInputFiles.poll();
			numberOfWorkersToCreate=0;	
			int nnnnn = 0;
			numberOfreviews = 0;
			String[] fileInputStringSplit = fileInputString.split("###");
			inputFileKey = fileInputStringSplit[0];

			String filekeytemp = inputFileKey.split("@@@")[1];
			System.out.println("sending reviews from new input file - " + inputFileKey);
			for (String sCurrentLine :fileInputStringSplit[1].split("\n")) {

				jsonObjLine = gson.fromJson(sCurrentLine,JsonElement.class).getAsJsonObject();
				jsonReviews =  jsonObjLine.get("reviews").getAsJsonArray();

				entries = new ArrayList<SendMessageBatchRequestEntry>();

				for(JsonElement review:  jsonReviews) {
					reviewId = UUID.randomUUID().toString();
					reviewText = ((JsonObject) review).get("text").getAsString();
					reviewRating = ((JsonObject) review).get("rating").getAsInt();
					if(reviewText.split(" ").length < 100)
					{
						reviewUrl = ((JsonObject) review).get("link").getAsString();
						numberOfreviews++;
						nnnnn++;
						System.out.println("send message number - " + nnnnn + " | " + reviewId +  " --- " + filekeytemp);
						inputFileHashmap.get(inputFileKey).getReviewsHashMap().put(reviewId,new Review(reviewId,reviewText,reviewUrl,-1,reviewRating));
						messageAttributes = new HashMap<String, MessageAttributeValue>();
						messageAttributes.put("inputFileKey", new MessageAttributeValue().withDataType("String").withStringValue(inputFileKey));
						messageAttributes.put("reviewId", new MessageAttributeValue().withDataType("String").withStringValue(reviewId));
						messageAttributes.put("reviewText", new MessageAttributeValue().withDataType("String").withStringValue(reviewText));

						entry = new  SendMessageBatchRequestEntry();
						entry.withMessageAttributes(messageAttributes);
						entry.withMessageBody("reviewMessage");
						entry.withId(UUID.randomUUID().toString());
						entries.add(entry);

						if(numberOfreviews == inputFileHashmap.get(inputFileKey).getNumberOfFilesPerWorker()){
							numberOfreviews = 0;
							numberOfWorkersToCreate++;
							synchronized (NumWorkersInstancesIds) {
								if(numberOfWorkersToCreate > NumWorkersInstancesIds.get() &&  NumWorkersInstancesIds.get() < 19){
									System.out.println("create worker - " + numberOfWorkersToCreate);
									createWorkers(1);
								}
							}

						}
					}
				}
				SendMessageBatchRequest sendMessageBatchRequest =new  SendMessageBatchRequest(managerToWorkersQueue, entries);
				sqs.sendMessageBatch(sendMessageBatchRequest);
			}
			if(numberOfreviews != 0){
				numberOfreviews = 0;
				numberOfWorkersToCreate++;
				synchronized (NumWorkersInstancesIds) {
					if(numberOfWorkersToCreate > NumWorkersInstancesIds.get() &&  NumWorkersInstancesIds.get() < 19){
						System.out.println("create worker - " + numberOfWorkersToCreate);
						createWorkers(1);
					}
				}
			}
			inputFileHashmap.get(inputFileKey).setDoneSending(true);
		}
	}

	public static void createWorkers(int numberOfWorkersToCreate){
		try {
			RunInstancesRequest request = new RunInstancesRequest("ami-0a00ce72", numberOfWorkersToCreate, numberOfWorkersToCreate);
			request.setInstanceType(InstanceType.T2Medium.toString());
			request.setUserData(getUserDataScript(NumWorkersInstancesIds.get()));
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
			for(Instance instance : instances){
				workersLogs.put(NumWorkersInstancesIds.get(),new HashMap<String, AtomicInteger>());
				workersLogs.get(NumWorkersInstancesIds.get()).put(instance.getInstanceId(), new AtomicInteger(0));
			}

			ArrayList<Tag> tags = new ArrayList<Tag>();
			Tag t = new Tag();
			t.setKey("role");
			t.setValue("worker");
			tags.add(t);
			CreateTagsRequest ctr = new CreateTagsRequest();
			ctr.setTags(tags);
			for (Instance instance : instances) {
				ctr.withResources(instance.getInstanceId());
				ec2.createTags(ctr);
			}
			System.out.println("Run new worker - " + NumWorkersInstancesIds.get());
		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}
		NumWorkersInstancesIds.incrementAndGet();
	}

	public static void retriveMessageFromWorkersQueue(){
		List<Message>  messages = sqs.reciveMessages(workersToManagerQueue);
		String inputFileKey,reviewId,reviewSentiment,reviewEntities;
		//String[] messageSplitArray;
		InputFile inputFile;
		Map<String,MessageAttributeValue> messageAttributes = null;
		for(Message message:messages){
			messageAttributes = message.getMessageAttributes();
			//		messageSplitArray = message.getBody().split("###");
			inputFileKey =messageAttributes.get("inputFileKey").getStringValue();
			reviewId = messageAttributes.get("reviewId").getStringValue();
			reviewSentiment = messageAttributes.get("sentiment").getStringValue();
			reviewEntities = messageAttributes.get("entities").getStringValue();
			inputFile = inputFileHashmap.get(inputFileKey);

			//update reviewsHashmap
			if(inputFile !=null && inputFile.getReviewsHashMap().get(reviewId).getEntities() == null){
				inputFile.getReviewsHashMap().get(reviewId).setEntitiesAndSentiment(reviewEntities,Integer.parseInt(reviewSentiment));
				inputFile.incNumberOfAnalyzedReviews();
				System.out.println("receive message number - " + inputFile.getReviewsHashMap().size() + "|" + inputFile.getNumberOfAnalyzedReviews() + " | " + reviewId );
				Map.Entry<String, AtomicInteger> entry = workersLogs.get(Integer.parseInt(messageAttributes.get("WorkerId").getStringValue())).entrySet().iterator().next();
				entry.getValue().incrementAndGet();
				System.out.println(workersLogs);
				if(inputFile.isDoneSending() && inputFile.getNumberOfAnalyzedReviews() == inputFile.getReviewsHashMap().size()){
					System.out.println("finish analyze file size : " + inputFile.getReviewsHashMap().size());
					Thread worker = new SenderLocalAppQueue(inputFileKey);
					executor.execute(worker);
				}
			}
			// delete the message from queue
			sqs.deleteMessages(Collections.singletonList(message),workersToManagerQueue);
			//check if all this movie reviews are analyzed and create file if so
		}

	}

	public static void writeSummaryFileIfNeeded(String inputFileKey){
		try {
			System.out.println("write summary file for - " + inputFileKey);
			JsonObject jsonPerReview = new JsonObject();
			PrintWriter writer = new PrintWriter(inputFileKey + "@@@" + "complete.txt", "UTF-8");
			Iterator<Entry<String, Review>> it = inputFileHashmap.get(inputFileKey).getReviewsHashMap().entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Review> pair = (Map.Entry<String, Review>)it.next();
				jsonPerReview = new JsonObject();
				jsonPerReview.addProperty("review",((Review)pair.getValue()).getReview());
				jsonPerReview.addProperty("entities",((Review)pair.getValue()).getEntities());
				jsonPerReview.addProperty("sentiment",((Review)pair.getValue()).getSentiment());
				jsonPerReview.addProperty("url",((Review)pair.getValue()).getUrl());
				jsonPerReview.addProperty("rating",((Review)pair.getValue()).getRating());
				writer.println(jsonPerReview);
			}
			writer.close();

			//upload summary file to S3
			ArrayList<String> fileKey = s3.uploadFiles(new String[] {inputFileKey + "@@@" + "complete.txt"}, inputFileKey.split("@@@")[0]);
			//send message to localApp containing the key of the summary file
			HashMap<String,MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
			messageAttributes.put("fileKey", new MessageAttributeValue().withDataType("String").withStringValue(fileKey.get(0)));

			SendMessageRequest sendMessageRequest = new SendMessageRequest();
			sendMessageRequest.withMessageBody("completeFileMessage");
			sendMessageRequest.withQueueUrl(managerTolocalAppQueue + inputFileHashmap.get(inputFileKey).getUuid());
			sendMessageRequest.withMessageAttributes(messageAttributes);

			sqs.sendMessage(sendMessageRequest);
			inputFileHashmap.remove(inputFileKey);
		}    catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static String getUserDataScript(int workerId){
		ArrayList<String> lines = new ArrayList<String>();
		lines.add("#! /bin/bash");
		lines.add("sudo apt-get update");
		lines.add("sudo apt-get install openjdk-8-jre-headless -y");
		lines.add("sudo apt-get install wget -y");
		lines.add("sudo apt-get install unzip -y");
		lines.add("echo export WorkerId=" + workerId +" >> ~/.bash_profile");
		lines.add("source ~/.bash_profile");
		lines.add("sudo wget http://repo1.maven.org/maven2/com/googlecode/efficient-java-matrix-library/ejml/0.23/ejml-0.23.jar");
		lines.add("sudo wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.3.0/stanford-corenlp-3.3.0.jar");
		lines.add("sudo wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.3.0/stanford-corenlp-3.3.0-models.jar");
		lines.add("sudo wget http://central.maven.org/maven2/de/jollyday/jollyday/0.4.7/jollyday-0.4.7.jar");
		lines.add("sudo wget https://s3.amazonaws.com/ass1jars203822300/worker.zip");
		lines.add("sudo unzip -P 123456 worker.zip");
		lines.add("java -cp .:worker-0.0.1-SNAPSHOT.jar:stanford-corenlp-3.3.0.jar:stanford-corenlp-3.3.0-models.jar:ejml-0.23.jar:jollyday-0.4.7.jar -jar worker-0.0.1-SNAPSHOT.jar ");
		String str = new String(Base64.getEncoder().encode(join(lines, "\n").getBytes()));
		return str;
	}

	private static String join(Collection<String> s, String delimiter) {
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = s.iterator();
		while (iter.hasNext()) {
			builder.append(iter.next());
			if (!iter.hasNext()) {
				break;
			}
			builder.append(delimiter);
		}
		return builder.toString();
	}
}
