package com.dsp181.worker.worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.directconnect.model.NewPrivateVirtualInterfaceAllocation;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AttachClassicLinkVpcRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import edu.stanford.nlp.util.Pair;



/**
 * Hello world!
 *
 */

class entitiesThread extends Thread{
	private String text;
	private int index;
	entitiesThread(String text,int index){
		this.text = text;
		this.index= index;
	}
	@Override 
	public void run(){
		ArrayList<String> tempEntities = App.nlp.findEntities(text);
		tempEntities.removeIf(Objects::isNull);
		App.entitiesArray.set(index,tempEntities);
		App.latch.countDown();
	}


}

class sentimentThread extends Thread{
	private String text;
	private int index;
	sentimentThread(String text,int index){
		this.text = text;
		this.index= index;
	}
	@Override 
	public void run(){
		App.sentimentArray[index] = App.nlp.findSentiment(text);
		App.latch.countDown();
		System.out.println("entities down " + index);
	}

}

public class App 
{
	static int[] sentimentArray;// = new int[10];
	static ArrayList<ArrayList<String> > entitiesArray;// = new ArrayList<ArrayList<String>>(10);

	static CountDownLatch latch ;//= new CountDownLatch(3);
	static SQS sqs;
	static NLPClass nlp = new NLPClass();

	public static void main( String[] args )
	{

		sqs = new SQS();
		sqs.launch();
		List<Message> messages = null;
		Map<String, MessageAttributeValue> messageAttributes = null;
		Map<String,MessageAttributeValue> receiveMessageAttributes = null;
		ExecutorService executor = Executors.newFixedThreadPool(5);
		int i=0;
		SendMessageBatchRequestEntry entry = null;
		ArrayList<SendMessageBatchRequestEntry> entries = null;
		ArrayList<Pair<MessageAttributeValue, MessageAttributeValue> > ids = null;
		while(true){
			messages = sqs.reciveMessages("managerToWorkersQueue");
			if(messages.size() > 0)
			{
				latch = new CountDownLatch(messages.size() * 2);
				i=0;
				entries = new ArrayList<SendMessageBatchRequestEntry>();	
				sentimentArray = new int[messages.size()];
				entitiesArray = new ArrayList<ArrayList<String>>(messages.size());
				for(int k=0;k<messages.size();k++){
					entitiesArray.add(k,null);
				}
				ids = new ArrayList<Pair<MessageAttributeValue,MessageAttributeValue>>(messages.size());
				for(Message message:messages){
					receiveMessageAttributes = message.getMessageAttributes();
					executor.execute(new entitiesThread(receiveMessageAttributes.get("reviewText").getStringValue(), i));
					executor.execute(new sentimentThread(receiveMessageAttributes.get("reviewText").getStringValue(), i));
					MessageAttributeValue inputFileKey = new MessageAttributeValue().withDataType("String").withStringValue(receiveMessageAttributes.get("inputFileKey").getStringValue());
					MessageAttributeValue reviewId = new MessageAttributeValue().withDataType("String").withStringValue(receiveMessageAttributes.get("reviewId").getStringValue());
					
					ids.add(i, new Pair<MessageAttributeValue, MessageAttributeValue>(inputFileKey,reviewId));
					i++;
				}
				System.out.println("recieve " + messages.size() + " messages !");
				try {
					latch.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				for(int j =0;j<messages.size();j++){

					messageAttributes = new HashMap<String, MessageAttributeValue>();
					messageAttributes.put("inputFileKey", ids.get(j).first);
					messageAttributes.put("reviewId", ids.get(j).second);
					messageAttributes.put("sentiment", new MessageAttributeValue().withDataType("String").withStringValue(String.valueOf(sentimentArray[j])));
					messageAttributes.put("entities", new MessageAttributeValue().withDataType("String").withStringValue(entitiesArray.get(j).toString()));
					messageAttributes.put("workerId", new MessageAttributeValue().withDataType("String").withStringValue(entitiesArray.get(j).toString()));
					entry = new  SendMessageBatchRequestEntry();
					entry.withMessageBody("reviewAnalyzeComplete");
					entry.withMessageAttributes(messageAttributes);
					entry.withId(UUID.randomUUID().toString());
					entries.add(entry);
				}
				SendMessageBatchRequest sendMessageBatchRequest =new  SendMessageBatchRequest("workersToManagerQueue", entries);
				sqs.sendMessageBatch(sendMessageBatchRequest);
				System.out.println("sent " + entries.size() + " messages !");
				for(Message message:messages){
					sqs.deleteMessages(Collections.singletonList(message),"managerToWorkersQueue");
				}
			}
		}        
	}
}
