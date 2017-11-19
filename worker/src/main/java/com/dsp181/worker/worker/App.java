package com.dsp181.worker.worker;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.Instance;
import com.dsp181.manager.manger.Review;
import com.dsp181.manager.manger.S3;
import com.dsp181.manager.manger.SQS;

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
		
		//get messages from SQS queue
    	 System.out.println("im a worker");
         int n = nlp.findSentiment("I don't like this stuff!!!");
         System.out.println("this is the answer: "+n);
         
         
         
    }
}
