/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package org.selman.tweetamo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Simple implementation TwitterCredentialsProvider that reads in Twitter access keys from a
 * properties file.
 */
public class PropertiesCredentials implements TwitterCredentialsProvider {

	private final TwitterCredentials twitterCredentials;

	/**
	 * Reads the specified file as a Java properties file and extracts the
	 * access keys. If the specified file doesn't contain the
	 * access keys an IOException will be thrown.
	 * 
	 * @param file
	 *            The file from which to read the credentials properties.
	 * 
	 * @throws FileNotFoundException
	 *             If the specified file isn't found.
	 * @throws IOException
	 *             If any problems are encountered reading the AWS access keys
	 *             from the specified file.
	 * @throws IllegalArgumentException
	 *             If the specified properties file does not contain the
	 *             required keys.
	 */
	public PropertiesCredentials(File file) throws FileNotFoundException,
			IOException, IllegalArgumentException {
		this(new FileInputStream(file));
	}

	/**
	 * Reads the specified input stream as a stream of Java properties file
	 * content and extracts the AWS access key ID and secret access key from the
	 * properties.
	 * 
	 * @param inputStream
	 *            The input stream containing the AWS credential properties.
	 * 
	 * @throws IOException
	 *             If any problems occur while reading from the input stream.
	 */
	public PropertiesCredentials(InputStream inputStream) throws IOException {
		Properties accountProperties = new Properties();
		try {
			accountProperties.load(inputStream);
		} finally {
			try {
				inputStream.close();
			} catch (Exception e) {
			}
		}

		if (accountProperties.getProperty("consumerKey") == null
				|| accountProperties.getProperty("consumerSecret") == null
				|| accountProperties.getProperty("accessToken") == null
				|| accountProperties.getProperty("accessTokenSecret") == null) {
			throw new IllegalArgumentException(
					"The specified file or stream doesn't contain the expected properties: 'consumerKey' "
							+ "'consumerSecret' 'accessToken' and 'accessTokenSecret'.");
		}

		twitterCredentials = new TwitterCredentials(
				accountProperties.getProperty("consumerKey"),
				accountProperties.getProperty("consumerSecret"),
				accountProperties.getProperty("accessToken"),
				accountProperties.getProperty("accessTokenSecret"));
	}

	@Override
	public TwitterCredentials getTwitterCredentials() {
		return twitterCredentials;
	}
}
