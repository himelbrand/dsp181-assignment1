package com.dsp181.local.local;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {

        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

        String[] filesPath = {"./input/B001DZTJRQ.txt"};
        S3 s3 = new S3();
        s3.launch(credentialsProvider);
        s3.uploadFiles(filesPath);

        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();



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

        // run node manager if it doesn't run already
        if(!found){
            try {
                // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
                RunInstancesRequest request = new RunInstancesRequest("ami-6057e21a", 1, 1);

                request.setInstanceType(InstanceType.T2Micro.toString());
                List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();

                for( Instance instance : instances){
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

    }
}
