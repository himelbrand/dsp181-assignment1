package com.dsp181.worker.worker;

import java.util.List;
import java.util.Properties;

//import com.google.gson.JsonArray;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class NLPClass {

    Properties props1 = new Properties();
    StanfordCoreNLP  sentimentPipeline;
    Properties props2 = new Properties();
    StanfordCoreNLP NERPipeline;

    public NLPClass(){
        props1.put("annotators", "tokenize, ssplit, parse, sentiment");
        sentimentPipeline =  new StanfordCoreNLP(props1);
        props2.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        NERPipeline =  new StanfordCoreNLP(props2);
    }
    public int findSentiment(String review) {

        int mainSentiment = 0;
        if (review!= null && review.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(review);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }

   /* public JsonArray printEntities(String review){

        // return jsonArray containing the result
        JsonArray resultEntitiesJsonArray = new JsonArray();

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                resultEntitiesJsonArray.add(word + ":" + ne);
            }
        }
        return resultEntitiesJsonArray;

    }*/
}