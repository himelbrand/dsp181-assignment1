package com.dsp181.local.local;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

import java.io.IOException;
import java.util.*;

public class App {
    public static void main(String[] args) throws IOException {

        // set AWSCredentialsProvider
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

        // set the ec2
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        // set UUID

        UUID uuid = UUID.randomUUID();

        // check if the manager node is active and run it otherwise
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

        // set S3 storage and upload files
        String[] filesPath = {"./input/B001DZTJRQ.txt"};
        S3 s3 = new S3();
        s3.launch(credentialsProvider,uuid);
        ArrayList<String> filesKeys = s3.uploadFiles(filesPath);

        // set sqs and sends files location in the format "FileLocation###fileKey##..."
        SQS sqs = new SQS();
        sqs.launch(credentialsProvider);
        
        for(String fileKey:filesKeys){
        sqs.sendMessage("fileMessage###" + fileKey + "###" + s3.getBucketName());
        }
        System.out.println();

        // check for message "processComplete"
        List<Message> messages;
        String responseKey = null;
        boolean processComplete = false;
        while(!processComplete){
            messages = sqs.reciveMessages();
            for(Message message:messages){
                if(message.getBody().split("###")[0].equals("processComplete")) {
                    processComplete = true;
                    responseKey = message.getBody().split("###")[1];

                    // delete the "processComplete" message
                    sqs.deleteMessages(Collections.singletonList(message));
                    break;
                }
                System.out.println("waiting for process complete message, another 20 sec");
            }
        }

        // download file from S3
        S3Object responseObject = s3.downloadFiles(new String[]{responseKey}).get(0);
        System.out.println("APP RESULT" + responseObject.getObjectContent()); //TODO convert object content to html

        
        //send a termination message to the manager mode
        // TODO check how its need to be supplied as one of the input argument
        sqs.sendMessage("termination###"+ uuid);










    }
}
