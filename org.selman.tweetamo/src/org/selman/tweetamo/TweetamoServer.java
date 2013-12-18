package org.selman.tweetamo;

/*
Copyright 2013 Daniel Selman

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;


/**
 * Sample Kinesis Application.
 */
public final class TweetamoServer {
    
    private static final String DEFAULT_APP_NAME = "TweetamoServer";
    private static final String DEFAULT_STREAM_NAME = "tweetamo";
    private static final String DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com";
    
    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream DEFAULT_INITIAL_POSITION = InitialPositionInStream.TRIM_HORIZON;
    
    private static String applicationName = DEFAULT_APP_NAME;
    private static String streamName = DEFAULT_STREAM_NAME;
    private static String kinesisEndpoint = DEFAULT_KINESIS_ENDPOINT;
    private static InitialPositionInStream initialPositionInStream = DEFAULT_INITIAL_POSITION;
    
    private static KinesisClientLibConfiguration kinesisClientLibConfiguration;
    
    private static final Log LOG = LogFactory.getLog(TweetamoServer.class);

    /**
     * 
     */
    private TweetamoServer() {
        super();
    }
    
    /**
     * @param args Property file with config overrides (e.g. application name, stream name)
     * @throws IOException Thrown if we can't read properties from the specified properties file
     */
    public static void main(String[] args) throws IOException {
        String propertiesFile = null;
        
        if (args.length > 1) {
            System.err.println("Usage: java " + TweetamoServer.class.getName() + " <propertiesFile>");
            System.exit(1);
        } else if (args.length == 1) {
            propertiesFile = args[0];
        }

        configure(propertiesFile);
        
        System.out.println("Starting " + applicationName);
        LOG.info("Running " + applicationName + " to process stream " + streamName);
        
        
        IRecordProcessorFactory recordProcessorFactory = new TweetamoRecordProcessorFactory();
         Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);
    }
    
    private static void configure(String propertiesFile) throws IOException {
        
        
        if (propertiesFile != null) {
            loadProperties(propertiesFile);
        }
        
        // ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl" , "60");
        
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        LOG.info("Using workerId: " + workerId);
       
        // Get credentials from IMDS. If unsuccessful, get them from the classpath. 
       AWSCredentialsProvider credentialsProvider = null;
        try {
            credentialsProvider = new InstanceProfileCredentialsProvider();
            // Verify we can fetch credentials from the provider
            credentialsProvider.getCredentials();
            LOG.info("Obtained credentials from the IMDS.");
        } catch (AmazonClientException e) {
            LOG.info("Unable to obtain credentials from the IMDS, trying classpath properties", e);
            credentialsProvider = new ClasspathPropertiesFileCredentialsProvider();
            // Verify we can fetch credentials from the provider
            credentialsProvider.getCredentials();
            LOG.info("Obtained credentials from the properties file.");
        }
        
        LOG.info("Using credentials with access key id: " + credentialsProvider.getCredentials().getAWSAccessKeyId());
        
        kinesisClientLibConfiguration = new KinesisClientLibConfiguration(applicationName, streamName, kinesisEndpoint, 
        		initialPositionInStream, credentialsProvider, workerId); 
    }

    /**
     * @param propertiesFile
     * @throws IOException Thrown when we run into issues reading properties
     */
    private static void loadProperties(String propertiesFile) throws IOException {
        FileInputStream inputStream = new FileInputStream(propertiesFile);
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } finally {
            inputStream.close();
        }
        
        String appNameOverride = properties.getProperty(ConfigKeys.APPLICATION_NAME_KEY);
        if (appNameOverride != null) {
            applicationName = appNameOverride;
        }
        LOG.info("Using application name " + applicationName);
        
        String streamNameOverride = properties.getProperty(ConfigKeys.STREAM_NAME_KEY);
        if (streamNameOverride != null) {
            streamName = streamNameOverride;
        }
        LOG.info("Using stream name " + streamName);
        
        String kinesisEndpointOverride = properties.getProperty(ConfigKeys.KINESIS_ENDPOINT_KEY);
        if (kinesisEndpointOverride != null) {
            kinesisEndpoint = kinesisEndpointOverride;
        }
        String initialPositionOverride = properties.getProperty(ConfigKeys.INITIAL_POSITION_IN_STREAM_KEY);
        if (initialPositionOverride != null) {
             initialPositionInStream = InitialPositionInStream.valueOf(initialPositionOverride);
        }
         LOG.info("Using initial position " + initialPositionInStream.toString() + " (if a checkpoint is not found).");
    }

}
