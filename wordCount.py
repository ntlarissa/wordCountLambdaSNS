import boto3
import os
import json

def lambda_handler(event, context):


    if event:
    
        #open the file, read the content and count words
        print("Event : ", event)
        
        #Create a s3 client to retrieve data
        s3 = boto3.client('s3')
    
        #get name file
        filename = str(event["Records"][0]["s3"]["object"]["key"])
        print(f'Filename: {filename}')
        #read the content
        fileObj = s3.get_object(Bucket = os.environ['bucket'], Key = filename)
        data = fileObj["Body"].read()
        #count words
        words = data.split()
        message = "The word count in the file "+filename+" is "+str(len(words))
        print(message)
    
        # Retrieve the topic ARN and the region where the lambda function is running from the environment variables.
    
        TOPIC_ARN = os.environ['topicARN']
        FUNCTION_REGION = os.environ['AWS_REGION']
    
        # Extract the topic region from the topic ARN.
    
        arnParts = TOPIC_ARN.split(':')
        TOPIC_REGION = arnParts[3]

    

    # Create an SNS client, and format and publish a message containing the sales analysis report based on the extracted report data.

    snsClient = boto3.client('sns', region_name=TOPIC_REGION)

    
    # Publish the message to the topic.

    response = snsClient.publish(
        TopicArn = TOPIC_ARN,
        Subject = 'Word count',
        Message = message
    )

    # Return a successful function execution message.

    return {
        'statusCode': 200,
        'body': json.dumps('Word count sent.')
    }
