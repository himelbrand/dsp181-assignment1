package com.dsp181.local.local;

import java.util.HashMap;
/*
 * Copyright 2010-2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.util.List;
import java.util.Map;
import java.util.UUID;
//import java.util.UUID;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web
 * Services developer account, and be signed up to use Amazon SQS. For more
 * information on Amazon SQS, see http://aws.amazon.com/sqs.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in the
 *                   AwsCredentials.properties file before you try to run this
 *                   sample.
 * http://aws.amazon.com/security-credentials
 */
public class SQS {
    private AmazonSQS sqs;
    private String myQueueUrlSend;
    private String myQueueUrlReceive;
    public String getMyQueueUrlReceive() {
		return myQueueUrlReceive;
	}
    public String getMyQueueUrlSend() {
		return myQueueUrlSend;
	}
    public  void launch(AWSCredentialsProvider credentialsProvider,UUID uuid) {
        /*
         * Important: Be sure to fill in your AWS access credentials in the
         *            AwsCredentials.properties file before you try to run this
         *            sample.
         * http://aws.amazon.com/security-credentials
         */

        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SQS LocalApp");
        System.out.println("===========================================\n");

        try {
            // Create a new "localAppQueueSend" "localAppQueueRecive" if it does not exist
            System.out.println("Listing all queues in your account.\n");
            boolean foundLocalAppQueueSend=false,foundLocalAppQueueReceive = false;
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
            	System.out.println("  QueueUrl: " + queueUrl);
            	if(queueUrl.equals("localAppToManagerQueue")){
            		foundLocalAppQueueSend = true;
            		myQueueUrlSend = queueUrl;
            	}
            	if(queueUrl.equals("managerTolocalAppQueue-"+uuid)){
            		foundLocalAppQueueReceive = true;
            		myQueueUrlReceive = queueUrl;
            	}
            }
            Map<String, String> attributes = new HashMap<String, String>();
            // A FIFO queue must have the FifoQueue attribute set to True
            attributes.put("FifoQueue", "true");
            // Generate a MessageDeduplicationId based on the content, if the user doesn't provide a MessageDeduplicationId
            attributes.put("ContentBasedDeduplication", "true");
            
            if(!foundLocalAppQueueSend){
            System.out.println();
            System.out.println("Creating a new SQS queue called localAppToManagerQueue.\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("localAppToManagerQueue.fifo").withAttributes(attributes);
            myQueueUrlSend = sqs.createQueue(createQueueRequest).getQueueUrl();
            }
            if(!foundLocalAppQueueReceive){
            System.out.println();
            System.out.println("Creating a new SQS queue called " + "managerTolocalAppQueue-"+uuid + ".\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("managerTolocalAppQueue-"+uuid);
            myQueueUrlReceive = sqs.createQueue(createQueueRequest).getQueueUrl();
            }
            
            

        }catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    public void sendMessage(String message) {
        // Send a message
        System.out.println("Sending a message to " + myQueueUrlSend + ".\n");
        sqs.sendMessage(new SendMessageRequest(myQueueUrlSend, message));
    }
    public SendMessageResult sendMessageRequest(SendMessageRequest sendMessageRequest) {
        // Send a message
        System.out.println("Sending a message request to " + myQueueUrlSend + ".\n");
        return sqs.sendMessage(sendMessageRequest);
    }
    public List<Message> reciveMessages() {
        // Receive messages
        System.out.println("Receiving messages from " + myQueueUrlReceive + " .\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrlReceive).withQueueUrl(myQueueUrlReceive)
        		.withWaitTimeSeconds(20).withMaxNumberOfMessages(1).withAttributeNames("fileKey");
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        return messages;
    }
    public void printMessages(List<Message> messages){
        for (Message message : messages) {
            System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue());
            }
        }
        System.out.println();
    }

    public void deleteMessages(List<Message> messages) {
        // Delete a message
        System.out.println("Deleting a message.\n");
        String messageRecieptHandle = messages.get(0).getReceiptHandle();
        sqs.deleteMessage(new DeleteMessageRequest(myQueueUrlReceive, messageRecieptHandle));
    }

    public void deleteQueue(String queueName) {
        // Delete a queue
        System.out.println("Deleting the test queue.\n");
        sqs.deleteQueue(new DeleteQueueRequest(queueName));
    }
}