
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
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.util.Tables;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class NYSEQuery {

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
	static DynamoDB dynamo;
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
		dynamo = new DynamoDB(dynamoDB);
	}

	public static void main(String[] args) throws Exception {
		init();
		String tableName = "stock_eod";
		query(tableName);
	}

	private static void query(String tableName) {
		Table table = null;
		try {
			// Create table if it does not exist yet
			if (!Tables.doesTableExist(dynamoDB, tableName)) {
				System.out.println("Table " + tableName + " is does not exist");
			} else {
				table = dynamo.getTable(tableName);
			}

			// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html
			// select stockTicker, tradeDate, v from stock_eod 
			// where stockTicker = 'LEA' and tradeDate between '02-Oct-2013' and '03-Oct-2013'
			QuerySpec spec = new QuerySpec().withProjectionExpression("stockTicker,tradeDate,v")
					.withKeyConditionExpression("stockTicker = :v_st and tradeDate between :v_sd and :v_ed")
//					.withFilterExpression("contains(tradeDate, :v_t)")
					.withValueMap(new ValueMap()
										.withString(":v_st", "LEA")
										.withString(":v_sd", "02-Oct-2013")
										.withString(":v_ed", "03-Oct-2013")
							// .withString(":v_td", "01-Oct-2013")
//							.withString(":v_t", "Oct")
							)
					.withConsistentRead(true);

			ItemCollection<QueryOutcome> items = table.query(spec);

			Iterator<Item> iterator = items.iterator();
			while (iterator.hasNext()) {
				System.out.println(iterator.next().toJSONPretty());
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