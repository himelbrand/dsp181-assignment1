package com.dsp181.local.local;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
//import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class App {

	private static AWSCredentialsProvider credentialsProvider;
	private static AmazonEC2 ec2;
	private static UUID uuid;
	private static SQS sqs;
	private static S3 s3; 
	private static int numberOfFilesPerWorker =0;
	private static boolean terminate = false; 
	private static ArrayList<String> filesPathArray = new ArrayList<String>(),outputFilesPathArray = new ArrayList<String>();
	public static void main(String[] args) throws IOException {

		// get details from args array
		if(!getArgsDetails(args)){
			System.out.println("Argumant format isn't correct");
			return;
		}
		
		long startTime = System.nanoTime();
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




		// check if the manager node is active and run it otherwise
		launchManagerNode();

		// set S3 storage and upload files
		ArrayList<String> filesKeys = s3.uploadFiles(filesPathArray);
		int remainingFiles = filesKeys.size();
		sendInputFilesLocation(filesKeys,numberOfFilesPerWorker);

		// send termination message
		if(terminate){
			SendMessageRequest sendMessageRequest = new SendMessageRequest(sqs.getMyQueueUrlSend(), "terminate");
			sendMessageRequest.setMessageGroupId("localApp-" + uuid.toString());
			SendMessageResult messageResult =  sqs.sendMessageRequest(sendMessageRequest);
			while(messageResult.getMessageId() == null){
				System.out.println("termiante is being send " + sqs.sendMessageRequest(sendMessageRequest));
			}
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

		long endTime = System.nanoTime();
		System.out.println("Took "+(double)(endTime - startTime)/ 1000000000.0 + " seconds"); 


	}

	private static void launchManagerNode(){
		boolean done = false;
		boolean found = false;
		while(!done) {
			ArrayList<String> filters = new ArrayList<String>();
			filters.add("running");
			Filter filter = new Filter("instance-state-name", filters);
			DescribeInstancesRequest request = new DescribeInstancesRequest();
			DescribeInstancesResult response = ec2.describeInstances(request.withFilters(filter));

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

				request.setInstanceType(InstanceType.T2Medium.toString());
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
		Map<String, MessageAttributeValue> messageAttributes;
		for(String fileKey:filesKeys){
			messageAttributes = new HashMap<String, MessageAttributeValue>();
			messageAttributes.put("fileKey", new MessageAttributeValue().withDataType("String").withStringValue(fileKey));
			messageAttributes.put("bucketName", new MessageAttributeValue().withDataType("String").withStringValue(s3.getBucketName()));
			messageAttributes.put("numberOfFilesPerWorker", new MessageAttributeValue().withDataType("String").withStringValue(String.valueOf(numberOfFilesPerWorker)));
			messageAttributes.put("UUID", new MessageAttributeValue().withDataType("String").withStringValue(uuid.toString()));

			SendMessageRequest sendMessageRequest = new SendMessageRequest();
			sendMessageRequest.withMessageBody("fileMessage" + UUID.randomUUID().toString());
			sendMessageRequest.withQueueUrl(sqs.getMyQueueUrlSend());
			sendMessageRequest.withMessageAttributes(messageAttributes);
			sendMessageRequest.setMessageGroupId("groupid-" + uuid.toString());

			SendMessageResult result = sqs.sendMessageRequest(sendMessageRequest);
		}
		System.out.println();
		System.out.println("Done sending files");
		System.out.println();
	}

	private static ArrayList<String> checkForCompleteFileMessage(){
		List<Message> messages;
		ArrayList<String> responseKeys = new ArrayList<String>();
		messages = sqs.reciveMessages();
		for(Message message:messages){
			responseKeys.add(message.getMessageAttributes().get("fileKey").getStringValue());
			sqs.deleteMessages(Collections.singletonList(message));
		}
		return responseKeys;
	}

	private static void downloadResulstAndCreateHtml(ArrayList<String> responseCompleteFilesKeys){

		try {
			ArrayList<String> responseFile;
			responseFile = s3.downloadFilesLocal(responseCompleteFilesKeys);
			for(String file:responseFile)
			{
				String[] fileSplit = file.split("\n");
				String fileName = fileSplit[0];
				StringBuilder htmlBuilder =new StringBuilder();
				htmlBuilder.append("<html>");
				htmlBuilder.append("<head><title> file " + fileName + "</title></head>");
				htmlBuilder.append("<body>");
				String color, sarcasm;
				Gson gson = new Gson(); 
				JsonElement jelem;
				JsonObject jobj; 
				for(String line : Arrays.copyOfRange(fileSplit, 1, fileSplit.length)) {
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
					sarcasm = "sarcasm detected: "+ (jobj.get("sentiment").getAsInt() < (jobj.get("rating").getAsInt() - 2) ? "True" : "False");
					htmlBuilder.append("</br>");
					htmlBuilder.append("<div style=\"border-top:3px solid grey;border-bottom:3px solid grey;padding:10px;\">"+  
							"<b>Review: </b> <span style=\"color:"+color +";\">"+ jobj.get("review").getAsString() + "</span>"+
							"<br><b>Link: </b><a href=\"" +  jobj.get("url").getAsString() + "\">"+jobj.get("url").getAsString()+"</a>"
							+"<br><b>"+sarcasm+"</b>"
							+"<br><b>Sentiments: </b>"+jobj.get("sentiment").toString()+"<b>, Entities: </b>"+ jobj.get("entities").getAsString() + " </div>");

				}
				htmlBuilder.append("</body></html>");
				System.out.println(fileName);
				String fileOutputName =  outputFilesPathArray.get(filesPathArray.indexOf(fileName));
				PrintWriter writer = new PrintWriter(fileOutputName +  ".html" , "UTF-8");
				writer.print(htmlBuilder);
				ArrayList<String> tempArrayList = new ArrayList<String>();
				tempArrayList.add(fileOutputName+ ".html");
				writer.close();
				s3.uploadFiles(tempArrayList);

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
		lines.add("sudo apt-get install unzip -y");
		lines.add("sudo wget https://s3.amazonaws.com/ass1jars203822300/manager.zip");
		lines.add("sudo unzip -P 123456 manager.zip");
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

	private static boolean getArgsDetails(String[] args){
		int numberOfFiles=args.length;
		if(args[args.length - 1].equals("terminate")){
			terminate = true;
			try {
				numberOfFilesPerWorker = Integer.parseInt(args[args.length - 2]);
				numberOfFiles -= 2;
			}  catch(NumberFormatException nfe){
				return false;
			}


		}
		else{
			numberOfFiles -= 1;
			try {
			numberOfFilesPerWorker = Integer.parseInt(args[args.length - 1]);
			}  catch(NumberFormatException nfe){
				return false;
			}
		}
		if(numberOfFiles % 2 != 0 )
			return false;

		System.out.println(numberOfFiles);
		for(int i=0;i<numberOfFiles;i++){
			if(i<numberOfFiles/2)
				filesPathArray.add(args[i]);
			else
				outputFilesPathArray.add(args[i]);			
		}
		return true;
	}
}
