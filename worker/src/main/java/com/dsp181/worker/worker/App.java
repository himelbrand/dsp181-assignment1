package com.dsp181.worker.worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.ec2.model.AttachClassicLinkVpcRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;



/**
 * Hello world!
 *
 */
public class App 
{
	static SQS sqs;
	static NLPClass nlp = new NLPClass();

	public static void main( String[] args )
	{
		
		sqs = new SQS();
		sqs.launch();
		int sentiment  = -1;
		ArrayList<String> entities = null;
		NLPClass nlp = new NLPClass();
		List<Message> messages = null;
		Map<String, MessageAttributeValue> messageAttributes = null;
		SendMessageRequest sendMessageRequest = null;
		Map<String,String> receiveMessageAttributes = null;

		while(true){
			 messages = sqs.reciveMessages("managerToWorkersQueue");
			for(Message message:messages){
				//if(message.getBody().split("###")[0].equals("reviewMessage")) {
					//sentiment = nlp.findSentiment(message.getBody().split("###")[3]);
					//entities = nlp.findEntities(message.getBody().split("###")[3]);
				receiveMessageAttributes = message.getAttributes();
					sentiment = nlp.findSentiment(receiveMessageAttributes.get("reviewText"));
					entities = nlp.findEntities(receiveMessageAttributes.get("reviewText"));
					
					
					messageAttributes = new HashMap<String, MessageAttributeValue>();
					messageAttributes.put("inputFileKey", new MessageAttributeValue().withDataType("String").withStringValue(receiveMessageAttributes.get("inputFileKey")));
					messageAttributes.put("reviewId", new MessageAttributeValue().withDataType("String").withStringValue(receiveMessageAttributes.get("reviewId")));
					messageAttributes.put("sentiment", new MessageAttributeValue().withDataType("String").withStringValue(String.valueOf(sentiment)));
					messageAttributes.put("entities", new MessageAttributeValue().withDataType("String").withStringValue(entities.toString()));
					
					sendMessageRequest = new SendMessageRequest();
					sendMessageRequest.withMessageBody("reviewAnalyzeComplete");
					sendMessageRequest.withQueueUrl("workersToManagerQueue");
					sendMessageRequest.withMessageAttributes(messageAttributes);
					
					sqs.sendMessageRequest(sendMessageRequest);
				//	sqs.sendMessage("reviewMessage###" + movieTitle  + "###" + reviewId + "###" + reviewText,managerToWorkersQueue);
					
				//	sqs.sendMessage("reviewAnalyzeComplete###"
				//			+ message.getBody().split("###")[1] + "###" // movieTitle inputFileKey
				//			+ message.getBody().split("###")[2] + "###" //reviewId  
				//			+ sentiment + "###"
					//		+ entities, "workersToManagerQueue");
					// delete the message from queue
					sqs.deleteMessages(Collections.singletonList(message),"managerToWorkersQueue");

			//	}
			}
		}        
	}
}
