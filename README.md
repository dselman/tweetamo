tweetamo
========

An experimental integration between AWS Kinesis and Twitter. The code is still very much evolving and I have not yet Mavenized it or committed the required JARs.

To run this code you will need to create 2 authentication files: one for AWS and one for Twitter. Both files need to be in the classpath.

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

You then need to use the AWS console to create a Kinesis stream named tweetamo.

You then launch the Tweetamo client Java application. The first argument is the language of the tweets to monitor, the second argument a search string. E.g 

    org.selman.tweetamo.TweetamoClient en rugby. 
    
Once launched the client will connect to the tweetamo Kinesis steam and publish the tweets that match your search string to the stream.

You can then launch the org.selman.tweetamo.TweetamoServer Java application. This application connects to your tweetamo stream and simply logs the tweets to the console.
