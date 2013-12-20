tweetamo
========

An experimental integration between AWS Kinesis, Dynamo and Twitter. The "client" code subscribes to tweets using Twitter's streaming API and pushes them to a Kinesis stream. The "server" code subscribes to the stream and writes the Twitter status updates to a Dynamo table for offline query and analysis (TBD). The code is still very much evolving.

To run this code you will need to create 2 authentication files: one for AWS and one for Twitter. Both files need to be in the classpath when you run.

AwsCredentials.properties:
--------------------------
    # Fill in your AWS Access Key ID and Secret Access Key
    # http://aws.amazon.com/security-credentials
    accessKey=***
    secretKey=***

TwitterCredentials.properties:
------------------------------
    # Fill in your Twitter OAuth security credentials
    # https://dev.twitter.com/docs/auth/oauth
    consumerKey=***
    consumerSecret=***
    accessToken=***
    accessTokenSecret=***

Use the AWS console to create a Kinesis stream named tweetamo.

Launch the Tweetamo client Java application. The first argument is the language of the tweets to monitor, the second argument a search string. E.g 

    org.selman.tweetamo.TweetamoClient en rugby. 
    
Once launched the client will connect to the tweetamo Kinesis steam and publish the tweets that match your search string to the stream.

Launch the org.selman.tweetamo.TweetamoServer Java application. This application connects to your tweetamo stream and simply logs the tweets to the console.

Building
--------

Download the code, open a shell at the org.selman.tweetamo directory and type:

    mvn clean install
