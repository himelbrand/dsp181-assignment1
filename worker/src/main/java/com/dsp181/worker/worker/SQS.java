package com.dsp181.worker.worker;

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

import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
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

    public  void launch() {
    	BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAIPQVA435AAQCCUIQ", "M3OyJZdbJjb6DRL5pHCglZk2mFYh7DLcQ46JJaik");
        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .withRegion("us-west-2")
                .build();
    }
    
    public void createQueue(String queueUrl){
    	
    	   try {
    		   
               System.out.println();
               System.out.println("Creating a new SQS queue called " + queueUrl + ".\n");
               new CreateQueueRequest(queueUrl);
               
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
    public SendMessageResult sendMessageRequest(SendMessageRequest sendMessageRequest) {
        // Send a message
        System.out.println("Sending a message request to workersToManaer.\n");
        return sqs.sendMessage(sendMessageRequest);
    }
    
    public void sendMessage(String message,String queueUrl) {
        // Send a message
        System.out.println("Sending a message to " + queueUrl + ".\n");
        sqs.sendMessage(new SendMessageRequest(queueUrl, message));
    }
    
    public List<Message> reciveMessages(String queueUrl) {
        // Receive messages
        System.out.println("Receiving messages from " + queueUrl + " .\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl).withQueueUrl(queueUrl)
        		.withMaxNumberOfMessages(10).withWaitTimeSeconds(20)
        		.withMessageAttributeNames("inputFileKey","reviewId","reviewText");
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        return messages;
    }
    
    public void sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest) {
        // Send a message
        //System.out.println("Sending a message to " + queueUrl + ".\n");
        sqs.sendMessageBatch(sendMessageBatchRequest);
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

    public void deleteMessages(List<Message> messages,String queueUrl) {
        // Delete a message
        System.out.println("Deleting a message.\n");
        String messageRecieptHandle = messages.get(0).getReceiptHandle();
        sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));
    }

    public void deleteQueue(String queueUrl) {
        // Delete a queue
        System.out.println("Deleting the test queue.\n");
        sqs.deleteQueue(new DeleteQueueRequest(queueUrl));
    }

     /*   } catch (AmazonServiceException ase) {
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
   */
}