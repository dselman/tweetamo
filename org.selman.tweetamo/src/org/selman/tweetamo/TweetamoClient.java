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
package org.selman.tweetamo;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

/**
 * <p>
 * This is a basic client application that connects to the Twitter
 * Streaming API and publishes status updates to a AWS Kinesis stream.
 * </p>
 * <p>
 * The OAuth credentials for connecting to Twitter are loaded from the
 * TwitterCredentials.properties file.
 * </p>
 * <p>
 * The client publishes tweets to the AWS Kinesis Stream with
 * the name tweetamo. The stream must exist.
 * </p>
 * 
 * @author dselman
 *
 */
public class TweetamoClient {

	/**
	 * The name of the Kinesis stream that we publish to
	 */
	public static final String STREAM_NAME = "tweetamo";

	static AmazonKinesisClient kinesisClient;
	private static final Log LOG = LogFactory.getLog(TweetamoClient.class);

	public static void main(String[] args) throws Exception {
		
		if( args.length != 2 ) {
			System.out.println( "Usage: [language] [search topic]");
		}
		
		kinesisClient = new AmazonKinesisClient(
				new ClasspathPropertiesFileCredentialsProvider());
		waitForStreamToBecomeAvailable(STREAM_NAME);

		LOG.info("Publishing tweets to stream : " + STREAM_NAME);
	    StatusListener listener = new StatusListener(){
	        public void onStatus(Status status) {	            
				try {
					PutRecordRequest putRecordRequest = new PutRecordRequest();
					putRecordRequest.setStreamName(STREAM_NAME);
					putRecordRequest.setData( TweetSerializer.toBytes(status) );
					putRecordRequest.setPartitionKey( status.getUser().getScreenName() );
					PutRecordResult putRecordResult = kinesisClient
							.putRecord(putRecordRequest);
					LOG.info("Successfully putrecord, partition key : "
							+ putRecordRequest.getPartitionKey() + ", ShardID : "
							+ putRecordResult.getShardId());

				} catch (Exception e) {
					LOG.error( "Failed to putrecord", e );
				}	            
	        }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	        public void onException(Exception ex) {
	            ex.printStackTrace();
	        }
			@Override
			public void onScrubGeo(long arg0, long arg1) {}
			@Override
			public void onStallWarning(StallWarning arg0) {}
	    };
	    
	    ClasspathTwitterCredentialsProvider provider = new ClasspathTwitterCredentialsProvider();
	    TwitterCredentials credentials = provider.getTwitterCredentials();
	    
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey( credentials.getConsumerKey() )
		  .setOAuthConsumerSecret(credentials.getConsumerSecret() )
		  .setOAuthAccessToken( credentials.getAccessToken() )
		  .setOAuthAccessTokenSecret( credentials.getAccessTokenSecret() );
	    TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
	    twitterStream.addListener(listener);
	    FilterQuery filterQuery = new FilterQuery();
	    filterQuery.language( new String[] {args[0]});
	    filterQuery.track( new String[] {args[1]});
	    twitterStream.filter(filterQuery);
	}

	private static void waitForStreamToBecomeAvailable(String myStreamName) throws InterruptedException {
		LOG.info("Waiting for " + myStreamName + " to become ACTIVE...");

		long startTime = System.currentTimeMillis();
		long endTime = startTime + TimeUnit.MINUTES.toMillis(5);
		while (System.currentTimeMillis() < endTime) {
			Thread.sleep( TimeUnit.SECONDS.toMillis(5));
			DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
			describeStreamRequest.setStreamName(myStreamName);
			describeStreamRequest.setLimit(10);
			DescribeStreamResult describeStreamResponse = kinesisClient
					.describeStream(describeStreamRequest);

			String streamStatus = describeStreamResponse
					.getStreamDescription().getStreamStatus();
			if ("ACTIVE".equals(streamStatus)) {
				return;
			}
		}
	}	
}