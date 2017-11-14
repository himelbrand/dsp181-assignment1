package com.dsp181.worker.worker;

/**
 * Hello world!
 *
 */
public class App 
{
	static NLPClass nlp = new NLPClass();
    public static void main( String[] args )
    {
    	 System.out.println("im a worker");
         int n = nlp.findSentiment("I don't like this stuff!!!");
         System.out.println("this is the answer: "+n);
    }
}
