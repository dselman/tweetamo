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

/**
 *  Keys for configuration overrides (via properties file).
 */
public class ConfigKeys {
    
    /**
     * Name of the application.
     */
    public static final String APPLICATION_NAME_KEY = "applicationName";

    /**
     * Name of the Kinesis stream.
     */
    public static final String STREAM_NAME_KEY = "streamName";

    /**
     * Kinesis endpoint.
     */
    public static final String KINESIS_ENDPOINT_KEY = "kinesisEndpoint";
    
    /**
     * Initial position in the stream when an application starts up for the first time.
     * Value is one of LATEST (most recent data) or TRIM_HORIZON (oldest available data).
     */
    public static final String INITIAL_POSITION_IN_STREAM_KEY = "initialPositionInStream";
    
    private ConfigKeys() {        
    }

}
