package com.dsp181.worker.worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.sqs.model.Message;

import edu.stanford.nlp.util.Pair;

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
		int sentiment;
		ArrayList<String> entities;
		NLPClass nlp = new NLPClass();
		//get messages from SQS queue
		while(true){
			List<Message> messages = sqs.reciveMessages("workersQueue");
			for(Message message:messages){
				if(message.getBody().split("###")[0].equals("reviewMessage")) {
					sentiment = nlp.findSentiment(message.getBody().split("###")[3]);
					entities = nlp.findEntities(message.getBody().split("###")[3]);
					sqs.sendMessage("reviewAnalyzeComplete###"
							+ message.getBody().split("###")[1] + "###"
							+ message.getBody().split("###")[2] + "###"
							+ sentiment + "###"
							+ entities, "workersQueue");
					// delete the message from queue
					sqs.deleteMessages(Collections.singletonList(message),"localAppQueue");

				}
			}
		}        
	}
}
