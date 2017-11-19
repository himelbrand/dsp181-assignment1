package com.dsp181.local.local;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
//import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
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
	public static void main(String[] args) throws IOException {


		credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

		uuid = UUID.randomUUID();

		s3 = new S3();
		s3.launch(credentialsProvider,uuid);

		sqs = new SQS();
		sqs.launch(credentialsProvider);


		// get details from args array
		ArrayList<String> filesPathArray = new ArrayList<String>();
		boolean terminate = false;
		int numberOfFilesPerWorker = 0,numberOfFiles=args.length;
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

		// check if the manager node is active and run it otherwise
		launchManagerNode();
		
		// set S3 storage and upload files
		ArrayList<String> filesKeys = s3.uploadFiles(filesPathArray);
		int remainingFiles = filesKeys.size();
		sendInputFilesLocation(filesKeys,numberOfFilesPerWorker);
		
		// send termination message
		if(terminate)
			sqs.sendMessage("terminate");
		
		// check for message "completeFileMessage"
		while(remainingFiles > 0){ 
			ArrayList<String> responseCompleteFilesKeys = checkForCompleteFileMessage();
			remainingFiles -= responseCompleteFilesKeys.size();
			// download files from S3 and create html file
			downloadResulstAndCreateHtml(responseCompleteFilesKeys);
		}

		//send a termination message to the manager mode
		// TODO check how its need to be supplied as one of the input argument
		sqs.sendMessage("termination###"+ uuid);










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
						if(tag.getKey().equals("manager"))
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
			try {
				// Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
				RunInstancesRequest request = new RunInstancesRequest("ami-6057e21a", 1, 1);

				request.setInstanceType(InstanceType.T2Micro.toString());
				List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();

				for (Instance instance : instances) {
					ArrayList<Tag> tags = new ArrayList<Tag>();
					tags.add(new Tag("manager"));
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
	}

	private static 	void sendInputFilesLocation(ArrayList<String> filesKeys,int numberOfFilesPerWorker){
		for(String fileKey:filesKeys){
			sqs.sendMessage("fileMessage###" + fileKey + "###" + s3.getBucketName() + "###" + numberOfFilesPerWorker);
		}
		System.out.println();
	}

	private static ArrayList<String> checkForCompleteFileMessage(){
		List<Message> messages;
		ArrayList<String> responseKeys = new ArrayList<String>();
		messages = sqs.reciveMessages();
		for(Message message:messages){
			if(message.getBody().split("###")[0].equals("completeFileMessage")) {
				responseKeys.add(message.getBody().split("###")[1]);
				// delete the "processComplete" message
				sqs.deleteMessages(Collections.singletonList(message));
				break;
			}
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

					switch(Integer.parseInt(jobj.get("sentiment").getAsString())){
					case 0: color = "DarkRed";
					case 1: color = "red";
					case 2: color = "black";
					case 3: color = "LightGreen";
					case 4: color = "DarkGreen";
					default: color = "yellow";
					}
					htmlBuilder.append("<a href=\"" +  jobj.get("url").getAsString() + "\"> <div style=\"color:"+color +";\">"  + jobj.get("review").getAsString() + " " + jobj.get("entities").getAsString() + " </div></a>");
				}
				htmlBuilder.append("</body></html>");
				PrintWriter writer = new PrintWriter(responseObject.getKey()+ ".html" , "UTF-8");
				writer.print(htmlBuilder);
				ArrayList<String> tempArrayList = new ArrayList<String>();
				tempArrayList.add(responseObject.getKey()+ ".html");
				s3.uploadFiles(tempArrayList);
				writer.close();
				System.out.println("APP RESULT" + responseObject.getObjectContent()); //TODO convert object content to html

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
