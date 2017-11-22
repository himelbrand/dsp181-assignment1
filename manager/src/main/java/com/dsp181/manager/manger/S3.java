package com.dsp181.manager.manger;


import java.io.BufferedReader;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

//import com.amazonaws.AmazonClientException;
//import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
//import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using
 * the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on
 * Amazon S3, see http://aws.amazon.com/s3.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in the
 * AwsCredentials.properties file before you try to run this
 * sample.
 * http://aws.amazon.com/security-credentials
 */
public class S3 {
	private AWSCredentialsProvider credentialsProvider;
	private AmazonS3 s3;
	public void launch(AWSCredentialsProvider credentialsProvider) {
		/*
		 * Important: Be sure to fill in your AWS access credentials in the
		 *            AwsCredentials.properties file before you try to run this
		 *            sample.
		 * http://aws.amazon.com/security-credentials
		 */
		s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();
		this.credentialsProvider = credentialsProvider;
	}

	public ArrayList<String> uploadFiles(String[] files,String bucketName) {

		ArrayList<String> keys = new ArrayList<String>();

		String key = null;
		File file;
		for (String filePath : files) {
			file = new File(filePath);
			key = file.getName().replace('\\', '_').replace('/', '_').replace(':', '_');
			keys.add(key);
			
			System.out.println("Uploading a new object to S3 from a file , bucketName: " + bucketName +" key:" + key + "\n");
			
			PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
			s3.putObject(req);
		}
		return keys;

	}

	public ArrayList<S3Object> downloadFiles(HashMap<String, String> keysAndBuckets) throws IOException {
		/*
		 * Download an object - When you download an object, you get all of
		 * the object's metadata and a stream from which to read the contents.
		 * It's important to read the contents of the stream as quickly as
		 * possibly since the data is streamed directly from Amazon S3 and your
		 * network connection will remain open until you read all the data or
		 * close the input stream.
		 *
		 * GetObjectRequest also supports several other options, including
		 * conditional downloading of objects based on modification times,
		 * ETags, and selectively downloading a range of an object.
		 */


		ArrayList<S3Object> s3ObjectList = new ArrayList<S3Object>();
		for (Map.Entry<String,String> entry : keysAndBuckets.entrySet()) {
			System.out.println("Download object file key : " + entry.getKey() + " | bucketName : " + entry.getValue());
			S3Object object = s3.getObject(new GetObjectRequest(entry.getValue().toString(), entry.getKey().toString()));
			s3ObjectList.add(object);
			//System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
			//displayTextInputStream(object.getObjectContent());
		}
		return s3ObjectList;
	}
	public ArrayList<String> downloadFilesLocal(HashMap<String, String> keysAndBuckets) throws IOException {

		String sCurrentLine;
		String file;
		S3Object object ;
		BufferedReader reader = null;
		ArrayList<String> downloadedFilesArrayString = new ArrayList<String>();
		for (Map.Entry<String,String> entry : keysAndBuckets.entrySet()) {
			System.out.println("Download loca object file key : " + entry.getKey() + " | bucketName : " + entry.getValue());
			object = s3.getObject(new GetObjectRequest(entry.getValue().toString(), entry.getKey().toString()));
			file = entry.getValue()+"@@@" + entry.getKey() +"###";
			reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
			try {
				while ((sCurrentLine = reader.readLine()) != null) {
					file += sCurrentLine;
					file += "\n";
				}
				downloadedFilesArrayString.add(file);
			}finally{

			}
		}if(reader != null)
			reader.close();
		return downloadedFilesArrayString;
	}
	public void bucketObjectList(String bucketName) throws IOException {
		/*
		 * List objects in your bucket by prefix - There are many options for
		 * listing the objects in your bucket.  Keep in mind that buckets with
		 * many objects might truncate their results when listing their objects,
		 * so be sure to check if the returned object listing is truncated, and
		 * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
		 * additional results.

		 */

		System.out.println("Listing objects");
		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
				.withBucketName(bucketName)
				.withPrefix("My"));
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			System.out.println(" - " + objectSummary.getKey() + "  " +
					"(size = " + objectSummary.getSize() + ")");
		}
		System.out.println();
	}

	public void deleteFiles(String[] keys,String bucketName) throws IOException {
		/*
		 * Delete an object - Unless versioning has been turned on for your bucket,
		 * there is no way to undelete an object, so use caution when deleting objects.
		 */
		System.out.println("Deleting an object\n");
		for (String key : keys) {
			s3.deleteObject(bucketName, key);
		}
	}

	public void deleteBucket(String bucketName) throws IOException {
		/*
		 * Delete a bucket - A bucket must be completely empty before it can be
		 * deleted, so remember to delete any objects from your buckets before
		 * you try to delete them.
		 */
		System.out.println("Deleting bucket " + bucketName + "\n");
		s3.deleteBucket(bucketName);
	}


	/**
	 * Displays the contents of the specified input stream as text.
	 *
	 * @param input The input stream to display as text.
	 * @throws IOException
	 */
	private static void displayTextInputStream(InputStream input) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			if (line == null) break;

			System.out.println("    " + line);
		}
		System.out.println();
	}

}