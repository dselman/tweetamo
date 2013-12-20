package org.selman.tweetamo;

/*
 * Copyright 2013 Daniel Selman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import twitter4j.Status;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.Select;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

/**
 * Stores Twitter status updates in a DynamoDB table. Provides some
 * simple query APIs to retrieve status updates.
 */
public class PersistentStore {
	private static AmazonDynamoDBClient dynamoDB;
	private static final Log LOG = LogFactory.getLog(PersistentStore.class);

	private static final String TABLE_NAME = "tweetamo_status";
	public static final String COL_ID = "id";
	public static final String COL_CREATEDAT = "createdAt";
	public static final String COL_LAT = "lat";
	public static final String COL_LONG = "long";
	public static final String COL_SCREENNAME = "screenName";
	public static final String COL_TEXT = "text";

	public static final String INDEX_SCREENNAME = "INDEX_screenName";

	private static PersistentStore INSTANCE = null;

	/**
	 * The only information needed to create a client are security credentials
	 * consisting of the AWS Access Key ID and Secret Access Key. All other
	 * configuration, such as the service endpoints, are performed
	 * automatically. Client parameters, such as proxies, can be specified in an
	 * optional ClientConfiguration object when constructing a client.
	 * 
	 * @see com.amazonaws.auth.BasicAWSCredentials
	 * @see com.amazonaws.auth.PropertiesCredentials
	 * @see com.amazonaws.ClientConfiguration
	 */
	private PersistentStore(Region region, long readCapacity, long writeCapacity)
			throws Exception {
		/*
		 * This credentials provider implementation loads your AWS credentials
		 * from a properties file at the root of your classpath.
		 */
		dynamoDB = new AmazonDynamoDBClient(
				new ClasspathPropertiesFileCredentialsProvider());
		dynamoDB.setRegion(region);

		try {
			if (!tablesExist()) {
				createTables(readCapacity, writeCapacity);
			}
			waitForTableToBecomeAvailable(TABLE_NAME);
		} catch (Exception e) {
			handleException(e);
		}
	}

	public static PersistentStore getInstance() {
		synchronized (PersistentStore.class) {
			if (INSTANCE == null) {
				try {
					INSTANCE = new PersistentStore(
							Region.getRegion(Regions.US_EAST_1), 1L, 50L);
				} catch (Exception e) {
					LOG.error("Failed to create PersistentStore", e);
				}
			}

			return INSTANCE;
		}
	}

	private void createTables(long readCapacity, long writeCapacity)
			throws Exception {
		// ID | createdAt | lat | long | screen name | text |
		// Primary index is by ID
		// Global Secondary index is by screen name + createdAt

		try {
			CreateTableRequest createTableRequest = new CreateTableRequest()
					.withTableName(TABLE_NAME)
					.withKeySchema(
							new KeySchemaElement().withAttributeName(COL_ID)
									.withKeyType(KeyType.HASH))
					.withAttributeDefinitions(
							new AttributeDefinition().withAttributeName(COL_ID)
									.withAttributeType(ScalarAttributeType.N),
							new AttributeDefinition().withAttributeName(
									COL_CREATEDAT).withAttributeType(
									ScalarAttributeType.N),
							new AttributeDefinition().withAttributeName(
									COL_SCREENNAME).withAttributeType(
									ScalarAttributeType.S))
					.withProvisionedThroughput(
							new ProvisionedThroughput().withReadCapacityUnits(
									readCapacity).withWriteCapacityUnits(
									writeCapacity))
					.withGlobalSecondaryIndexes(
							new GlobalSecondaryIndex()
									.withIndexName(INDEX_SCREENNAME)
									.withProvisionedThroughput(
											new ProvisionedThroughput()
													.withReadCapacityUnits(
															(long) 10)
													.withWriteCapacityUnits(
															(long) 1))
									.withProjection(
											new Projection()
													.withProjectionType("ALL"))
									.withKeySchema(
											new KeySchemaElement()
													.withAttributeName(
															COL_SCREENNAME)
													.withKeyType(KeyType.HASH),
											new KeySchemaElement()
													.withAttributeName(
															COL_CREATEDAT)
													.withKeyType(KeyType.RANGE)));

			TableDescription createdTableDescription = dynamoDB.createTable(
					createTableRequest).getTableDescription();
			LOG.info("Created Table: " + createdTableDescription);
		} catch (Exception e) {
			handleException(e);
		}
	}

	private boolean tablesExist() {
		try {
			DescribeTableRequest describeTableRequest = new DescribeTableRequest()
					.withTableName(TABLE_NAME);
			dynamoDB.describeTable(describeTableRequest).getTable();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public void add(Status status) throws Exception {
		try {
			Map<String, AttributeValue> item = newItem(status);
			PutItemRequest putItemRequest = new PutItemRequest(TABLE_NAME, item);
			dynamoDB.putItem(putItemRequest);
			LOG.info("Stored status in Dynamo: " + status.getId());
		} catch (Exception e) {
			handleException(e);
		}
	}

	public QueryResult getLatestTweetsForScreenName(String screenName,
			long timestamp) throws Exception {
		try {
			long startDateMilli = System.currentTimeMillis();

			Map<String, Condition> keyConditions = new HashMap<String, Condition>();

			keyConditions.put(
					COL_SCREENNAME,
					new Condition().withComparisonOperator(
							ComparisonOperator.EQ).withAttributeValueList(
							new AttributeValue().withS(screenName)));

			keyConditions.put(
					COL_CREATEDAT,
					new Condition().withComparisonOperator(
							ComparisonOperator.BETWEEN)
							.withAttributeValueList(
									new AttributeValue().withN(Long
											.toString(timestamp)),
									new AttributeValue().withN(Long
											.toString(startDateMilli))));

			QueryRequest queryRequest = new QueryRequest()
					.withTableName(TABLE_NAME).withIndexName(INDEX_SCREENNAME)
					.withKeyConditions(keyConditions)
					.withSelect(Select.ALL_ATTRIBUTES)
					.withScanIndexForward(true);

			QueryResult result = dynamoDB.query(queryRequest);
			return result;
		} catch (Exception e) {
			handleException(e);
		}

		return null;
	}

	public ScanResult getSince(long timestamp, int limit) throws Exception {
		try {
			Condition scanFilterCondition = new Condition()
					.withComparisonOperator(ComparisonOperator.GT.toString())
					.withAttributeValueList(
							new AttributeValue().withN(Long.toString(timestamp)));
			Map<String, Condition> conditions = new HashMap<String, Condition>();
			conditions.put(COL_CREATEDAT, scanFilterCondition);

			ScanRequest scanRequest = new ScanRequest()
					.withTableName(TABLE_NAME)
					.withScanFilter(conditions)
					.withLimit(limit)
					.withAttributesToGet(
							Arrays.asList(COL_ID, COL_CREATEDAT, COL_LAT,
									COL_LONG, COL_SCREENNAME, COL_TEXT));

			return dynamoDB.scan(scanRequest);
		} catch (Exception e) {
			handleException(e);
		}

		return null;
	}

	private void handleException(Exception e) throws Exception {
		if (e instanceof AmazonServiceException) {
			AmazonServiceException ase = (AmazonServiceException) e;
			LOG.error(
					"Caught an AmazonServiceException, which means your request made it "
							+ "to AWS, but was rejected with an error response for some reason.",
					e);
			LOG.error("Error Message:    " + ase.getMessage());
			LOG.error("HTTP Status Code: " + ase.getStatusCode());
			LOG.error("AWS Error Code:   " + ase.getErrorCode());
			LOG.error("Error Type:       " + ase.getErrorType());
			LOG.error("Request ID:       " + ase.getRequestId());
		} else if (e instanceof AmazonClientException) {
			AmazonClientException ace = (AmazonClientException) e;
			LOG.error(
					"Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with AWS, "
							+ "such as not being able to access the network.",
					e);
			LOG.error("Error Message: " + ace.getMessage());
		}

		throw e;
	}

	private static Map<String, AttributeValue> newItem(Status status) {
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put(COL_ID,
				new AttributeValue().withN(Long.toString(status.getId())));
		item.put(COL_CREATEDAT, new AttributeValue().withN(Long.toString(status
				.getCreatedAt().getTime())));
		if (status.getGeoLocation() != null) {
			item.put(COL_LAT, new AttributeValue().withN(Double.toString(status
					.getGeoLocation().getLatitude())));
			item.put(COL_LONG, new AttributeValue().withN(Double
					.toString(status.getGeoLocation().getLongitude())));
		}
		item.put(COL_SCREENNAME,
				new AttributeValue().withS(status.getUser().getScreenName()));
		item.put(COL_TEXT, new AttributeValue().withS(status.getText()));
		return item;
	}

	private static void waitForTableToBecomeAvailable(String tableName) {
		LOG.info("Waiting for " + tableName + " to become ACTIVE...");

		long startTime = System.currentTimeMillis();
		long endTime = startTime + (10 * 60 * 1000);
		while (System.currentTimeMillis() < endTime) {
			try {
				Thread.sleep(1000 * 20);
			} catch (Exception e) {
			}
			try {
				DescribeTableRequest request = new DescribeTableRequest()
						.withTableName(tableName);
				TableDescription tableDescription = dynamoDB.describeTable(
						request).getTable();
				String tableStatus = tableDescription.getTableStatus();
				System.out.println("  - current state: " + tableStatus);
				if (tableStatus.equals(TableStatus.ACTIVE.toString()))
					return;
			} catch (AmazonServiceException ase) {
				if (ase.getErrorCode().equalsIgnoreCase(
						"ResourceNotFoundException") == false)
					throw ase;
			}
		}

		throw new RuntimeException("Table " + tableName + " never went active");
	}

}
