package com.dsp181.local.local;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class temp {
	public static void main(String[] args) {
        BufferedReader br = null;
        FileReader fr = null;
        PrintWriter writer = null;
        JsonObject jsonOutputLine;
        JsonObject jsonPerReview;
        JsonArray resultJsonArrayForAllReviews;

        StringBuilder htmlBuilder =new StringBuilder();
        htmlBuilder.append("<html>");
        htmlBuilder.append("<head><title> file #1 </title></head>");
        htmlBuilder.append("<body>");
        try {

            writer = new PrintWriter("fileName.txt", "UTF-8");
           // NLPClass nlp = new NLPClass();

            fr = new FileReader("./input/B01LYRCIPG.txt");
            br = new BufferedReader(fr);

            String sCurrentLine;

            Gson gson = new Gson();
            JsonElement element;
            JsonObject jsonRev,jsonObj;
            JsonArray jsonReviews;

            while ((sCurrentLine = br.readLine()) != null) {

                jsonOutputLine = new JsonObject();
                resultJsonArrayForAllReviews = new JsonArray();

                element = gson.fromJson(sCurrentLine,JsonElement.class);
                jsonObj = element.getAsJsonObject();


                jsonOutputLine.add("title",jsonObj.get("title"));
                jsonOutputLine.add("reviews",jsonObj.get("reviews"));

                jsonReviews =  jsonObj.get("reviews").getAsJsonArray();

                for(JsonElement review:  jsonReviews) {

                    jsonPerReview = new JsonObject();
                    jsonPerReview.addProperty("sentiment",nlp.findSentiment(((JsonObject) review).get("text").toString()));
                    jsonPerReview.add("enitities",nlp.printEntities(((JsonObject) review).get("text").toString()));
                    resultJsonArrayForAllReviews.add(jsonPerReview);
                }
                jsonOutputLine.add("result",resultJsonArrayForAllReviews);
                writer.println(jsonOutputLine);
            }


        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            try {
                writer.close();
                if (br != null)
                    br.close();

                if (fr != null)
                    fr.close();

            } catch (IOException ex) {

                ex.printStackTrace();

            }


        }
        htmlBuilder.append("</body>");
        htmlBuilder.append("</html>");
    }
}
