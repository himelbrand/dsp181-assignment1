package com.dsp181.local.local;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
//import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
//import java.io.Writer;
import java.util.*;

public class App {

	private static AWSCredentialsProvider credentialsProvider;
	private static AmazonEC2 ec2;
	private static UUID uuid;
	private static SQS sqs;
	private static S3 s3;
	private static int numberOfFilesPerWorker =0;
	private static boolean terminate = false; 
	public static void main(String[] args) throws IOException {

		BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAIPQVA435AAQCCUIQ", "M3OyJZdbJjb6DRL5pHCglZk2mFYh7DLcQ46JJaik");

		credentialsProvider = new AWSStaticCredentialsProvider(awsCreds);
		ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();

		uuid = UUID.randomUUID();

		s3 = new S3();
		s3.launch(credentialsProvider,uuid);

		sqs = new SQS();
		sqs.launch(credentialsProvider,uuid);


		// get details from args array
		ArrayList<String> filesPathArray = getArgsDetails(args);

		// check if the manager node is active and run it otherwise
		launchManagerNode();

		// set S3 storage and upload files
		ArrayList<String> filesKeys = s3.uploadFiles(filesPathArray);
		int remainingFiles = filesKeys.size();
		sendInputFilesLocation(filesKeys,numberOfFilesPerWorker);

		// send termination message
		if(terminate){
			SendMessageRequest sendMessageRequest = new SendMessageRequest(sqs.getMyQueueUrlSend(), "terminate");
			 sendMessageRequest.setMessageGroupId("groupid-" + uuid.toString());
			sqs.sendMessageRequest(sendMessageRequest);
		}

		// check for message "completeFileMessage"
		while(remainingFiles > 0){ 
			ArrayList<String> responseCompleteFilesKeys = checkForCompleteFileMessage();
			//
			remainingFiles -= responseCompleteFilesKeys.size();
			// download files from S3 and create html file
			if(responseCompleteFilesKeys.size() > 0)
				downloadResulstAndCreateHtml(responseCompleteFilesKeys);
		}


	}

	private static void launchManagerNode(){
		boolean done = false;
		boolean found = false;
		while(!done) {
			DescribeInstancesRequest request = new DescribeInstancesRequest();
			DescribeInstancesResult response = ec2.describeInstances(request);

			for(Reservation reservation : response.getReservations()) {
				for(Instance instance : reservation.getInstances()) {
					for(Tag tag :instance.getTags()){
						if(tag.getValue().equals("manager"))
							found = true;
					}
				}
			}
			request.setNextToken(response.getNextToken());

			if(response.getNextToken() == null) {
				done = true;
			}
		}
		if(!found) {
			System.out.println("Create manager node!");
			try {
				// Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
				RunInstancesRequest request = new RunInstancesRequest("ami-0a00ce72", 1, 1);

				request.setInstanceType(InstanceType.T2Large.toString());
				request.setUserData(getUserDataScript());

				Instance instance = ec2.runInstances(request).getReservation().getInstances().get(0);
				ArrayList<Tag> tags = new ArrayList<Tag>();
				Tag t = new Tag();
				t.setKey("role");
				t.setValue("manager");
				tags.add(t);
				CreateTagsRequest ctr = new CreateTagsRequest();
				ctr.setTags(tags);
				ctr.withResources(instance.getInstanceId());
				ec2.createTags(ctr);

				//System.out.println("Launch instances: " + instances);

			} catch (AmazonServiceException ase) {
				System.out.println("Caught Exception: " + ase.getMessage());
				System.out.println("Reponse Status Code: " + ase.getStatusCode());
				System.out.println("Error Code: " + ase.getErrorCode());
				System.out.println("Request ID: " + ase.getRequestId());
			}
		}else{
			System.out.println("Manager node already running ###");
		}
	}

	private static 	void sendInputFilesLocation(ArrayList<String> filesKeys,int numberOfFilesPerWorker){
		for(String fileKey:filesKeys){
			SendMessageRequest sendMessageRequest = new SendMessageRequest(sqs.getMyQueueUrlSend(), "fileMessage###" + fileKey + "###" + s3.getBucketName() + "###" + numberOfFilesPerWorker + "###" + uuid);
			 sendMessageRequest.setMessageGroupId("groupid-" + uuid.toString());
			
			sqs.sendMessageRequest(sendMessageRequest);
			//sqs.sendMessage("fileMessage###" + fileKey + "###" + s3.getBucketName() + "###" + numberOfFilesPerWorker + "###" + uuid);
		}
		System.out.println();
	}

	private static ArrayList<String> checkForCompleteFileMessage(){
		List<Message> messages;
		ArrayList<String> responseKeys = new ArrayList<String>();
		messages = sqs.reciveMessages();
		for(Message message:messages){
			//if(message.getBody().split("###")[0].equals("completeFileMessage") && (message.getBody().split("###")[2].equals(uuid.toString()))) {
			responseKeys.add(message.getBody().split("###")[1]);
			// delete the "processComplete" message
			sqs.deleteMessages(Collections.singletonList(message));
			//}
			System.out.println("waiting for process complete message, another 20 sec");
		}
		return responseKeys;
	}

	private static void downloadResulstAndCreateHtml(ArrayList<String> responseCompleteFilesKeys){

		try {
			ArrayList<S3Object> responseObjects;
			responseObjects = s3.downloadFiles(responseCompleteFilesKeys.toArray(new String[responseCompleteFilesKeys.size()]));
			for(S3Object responseObject:responseObjects)
			{
				StringBuilder htmlBuilder =new StringBuilder();
				htmlBuilder.append("<html>");
				htmlBuilder.append("<head><title> file " + responseObject.getObjectMetadata() + "</title></head>");
				htmlBuilder.append("<body>");
				BufferedReader reader = new BufferedReader(new InputStreamReader(responseObject.getObjectContent()));
				String line;
				String color;
				Gson gson = new Gson(); 
				JsonElement jelem;
				JsonObject jobj; 
				while((line = reader.readLine()) != null) {
					jelem = gson.fromJson(line, JsonElement.class);
					jobj = jelem.getAsJsonObject();
					switch(Integer.parseInt(jobj.get("sentiment").toString())){
					case 0: color = "DarkRed";
					break;
					case 1: color = "red";
					break;
					case 2: color = "black";
					break;
					case 3: color = "LightGreen";
					break;
					case 4: color = "DarkGreen";
					break;
					default: color = "yellow";
					break;
					}
					htmlBuilder.append("</br>");
					htmlBuilder.append("<a href=\"" +  jobj.get("url").getAsString() + "\"> <div style=\"color:"+color +";\">"  + jobj.get("review").getAsString() + " " + jobj.get("entities").getAsString() + " </div></a>");

				}
				htmlBuilder.append("</body></html>");
				PrintWriter writer = new PrintWriter(responseObject.getKey()+ ".html" , "UTF-8");
				writer.print(htmlBuilder);
				ArrayList<String> tempArrayList = new ArrayList<String>();
				tempArrayList.add(responseObject.getKey()+ ".html");
				writer.close();
				s3.uploadFiles(tempArrayList);
				System.out.println("APP RESULT" + responseObject.getObjectContent()); //TODO convert object content to html

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	private static String getUserDataScript(){
		ArrayList<String> lines = new ArrayList<String>();
		lines.add("#! /bin/bash");
		lines.add("sudo apt-get update");
		lines.add("sudo apt-get install openjdk-8-jre-headless -y");
		lines.add("sudo apt-get install wget -y");
		lines.add("sudo wget https://s3.amazonaws.com/ass1jars203822300/manager.zip");
		lines.add("sudo uzip -P 123456 manager.zip");
		lines.add("java -jar manager.jar");
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

	private static ArrayList<String> getArgsDetails(String[] args){
		ArrayList<String> filesPathArray = new ArrayList<String>();
		int numberOfFiles=args.length;
		if(args[args.length - 1].equals("terminate")){
			terminate = true;
			numberOfFilesPerWorker = Integer.parseInt(args[args.length - 2]);
			numberOfFiles -= 2;
		}
		else{
			numberOfFiles -= 1;
			numberOfFilesPerWorker = Integer.parseInt(args[args.length - 1]);
		}
		for(int i=0;i<numberOfFiles;i++){
			filesPathArray.add(args[i]);
		}
		return filesPathArray;
	}
}
