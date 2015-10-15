
/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.util.Tables;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class NYSELoad {

	/*
	 * Before running the code: Fill in your AWS access credentials in the
	 * provided credentials file template, and be sure to move the file to the
	 * default location (/Users/usdgadiraj/.aws/credentials) where the sample
	 * code will load the credentials from.
	 * https://console.aws.amazon.com/iam/home?#security_credential
	 *
	 * WARNING: To avoid accidental leakage of your credentials, DO NOT keep the
	 * credentials file in your source directory.
	 */

	static AmazonDynamoDBClient dynamoDB;
	static NyseParser nyseParser = new NyseParser();

	/**
	 * The only information needed to create a client are security credentials
	 * consisting of the AWS Access Key ID and Secret Access Key. All other
	 * configuration, such as the service endpoints, are performed
	 * automatically. Client parameters, such as proxies, can be specified in an
	 * optional ClientConfiguration object when constructing a client.
	 *
	 * @see com.amazonaws.auth.BasicAWSCredentials
	 * @see com.amazonaws.auth.ProfilesConfigFile
	 * @see com.amazonaws.ClientConfiguration
	 */
	private static void init() throws Exception {
		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (/Users/usdgadiraj/.aws/credentials).
		 */
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default").getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (/Users/usdgadiraj/.aws/credentials), and is in valid format.", e);
		}
		dynamoDB = new AmazonDynamoDBClient(credentials);
		Region region = Region.getRegion(Regions.US_EAST_1);
		dynamoDB.setRegion(region);
		dynamoDB.setEndpoint("http://dynamo.itversity.com:8000");
	}

	public static void main(String[] args) throws Exception {
		init();
		String tableName = "stock_eod";
		readAndLoad(tableName, args[0]);
	}

	private static void readAndLoad(String tableName, String nysePath) {
		try {
			// Create table if it does not exist yet
			if (!Tables.doesTableExist(dynamoDB, tableName)) {
				System.out.println("Table " + tableName + " is does not exist");
			}

			File localInputFolder = new File(nysePath);
			File[] listOfDirectories = localInputFolder.listFiles();
			for (File dir : listOfDirectories) {
				if (dir.isDirectory()) {
					File[] files = dir.listFiles();
					for (File file : files) {
						BufferedReader br = null;
						if (file.getName().endsWith("csv")) {
							// Add an item
							String sCurrentLine;

							try {
								br = new BufferedReader(new FileReader(file));
								while ((sCurrentLine = br.readLine()) != null) {
									nyseParser.parse(sCurrentLine);
									Map<String, AttributeValue> item = newItem(nyseParser);
									putItem(tableName, item);
								}
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to AWS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with AWS, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static void putItem(String tableName, Map<String, AttributeValue> item) {
		PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
		dynamoDB.putItem(putItemRequest);
	}

	private static Map<String, AttributeValue> newItem(NyseParser nyseParser) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("stockTicker", new AttributeValue(nyseParser.getStockTicker()));
		item.put("tradeDate", new AttributeValue(nyseParser.getTransactionDate()));
		item.put("lp", new AttributeValue().withN(Float.toString(nyseParser.getLowPrice())));
		item.put("op", new AttributeValue().withN(Float.toString(nyseParser.getOpenPrice())));
		item.put("cp", new AttributeValue().withN(Float.toString(nyseParser.getClosePrice())));
		item.put("hp", new AttributeValue().withN(Float.toString(nyseParser.getHighPrice())));
		item.put("v", new AttributeValue().withN(Float.toString(nyseParser.getVolume())));

		return item;
	}

}