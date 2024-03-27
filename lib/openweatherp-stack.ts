import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';

export class OpenweatherpStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);


    const bucketName = 'open-weather-upload-bucket'
    // Check if bucket already exists
    const existingBucket = s3.Bucket.fromBucketName(this, 'ExistingBucket', bucketName);

    // Create an S3 bucket only if it doesn't exist
    const bucket = existingBucket.bucketName ?
      existingBucket :
      new s3.Bucket(this, 'OpenWeatherUpload', {
        bucketName: bucketName
      });

    const lambdaFunction = new lambda.DockerImageFunction(this, 'InvokeFunction', {
      functionName: 'OpenWeatherPLambdaExtract',
      code: lambda.DockerImageCode.fromImageAsset('./lambda-image-extract'),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
      environment: {
        // Specify your environment variables here
        s3_upload_bucket: bucket.bucketName,
      },
    });

    const functionUrl = lambdaFunction.addFunctionUrl({
      authType: lambda.FunctionUrlAuthType.NONE,
      cors: {
        allowedMethods: [lambda.HttpMethod.ALL],
        allowedHeaders: ["*"],
        allowedOrigins: ["*"],
      },
    });

    const lambdaFunction2 = new lambda.DockerImageFunction(this, 'InvokeFunction2', {
      functionName: 'OpenWeatherPLambdaUpload',
      code: lambda.DockerImageCode.fromImageAsset('./lambda-image-transform'),
      memorySize: 128,
      timeout: cdk.Duration.seconds(30),
      environment: {
        // Specify your environment variables here
        s3_upload_bucket: bucket.bucketName,
      },
    });
    
    // Grant necessary permissions to the Lambda function to access the S3 bucket
    bucket.grantReadWrite(lambdaFunction2);
    // Add S3 trigger to Lambda function
    bucket.addObjectCreatedNotification(new s3n.LambdaDestination(lambdaFunction2), {
      prefix: 'raw/' // Specify the folder path within the bucket
    });

    // Create a CloudWatch Events rule with a schedule
    const rule = new events.Rule(this, 'OpenWeatherPTriggerDaily', {
      schedule: events.Schedule.cron({ minute: '0', hour: '12' }), // Trigger every day at 12 PM
    });

    // Add the Lambda function as a target for the CloudWatch Events rule
    rule.addTarget(new targets.LambdaFunction(lambdaFunction));
    
    new cdk.CfnOutput(this, "FunctionUrlValue", {
      value: functionUrl.url,
    });
  }
}
