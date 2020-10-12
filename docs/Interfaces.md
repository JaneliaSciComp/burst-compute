# Function interfaces

In order to use the burst compute framework, you need to define a **worker** function and a **reduce** function. 

## Worker Interface

You can define the worker function to do anything you like. If a job operates on N items, each worker should operate on the range of objects denoted by [startIndex-endIndex). Once the work is done, a single row must be added to DynamoDB to record the worker's results. 

Input:
```javascript
{
  jobId: "DynamoDB identifier where results should be written",
  batchId: "DynamoDB identifier where results should be written",
  jobParameters: {
    // Input parameter dictionary as received by dispatch function
  },
  startIndex: "First index to process",
  endIndex: "First index to not process",
}
```

After doing some computation, each function instance writes results as a new row in the DynamoDB **TasksTable** with the given **jobId** and **batchId** as the partition and sort key, respectively. Example code in Java:
```java
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("jobId", AttributeValue.builder().s(jobId).build());
    item.put("batchId", AttributeValue.builder().n(batchId.toString()).build());
    item.put("results", AttributeValue.builder().s(toJson(results)).build());
    PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).build();
    dynamoDbClient.putItem(putItemRequest);
```

The results can be any item supported by DynamoDB, including JSON objects and arrays. There is nothing special about the attribute name "results". Since DynamoDB is schemaless, you can add multiple attributes if you need to. 

This function will require dynamodb:PutItem permission to the **TasksTable**:

```json
{
    "Action": [
        "dynamodb:PutItem"
    ],
    "Resource": "arn:aws:dynamodb:<REGION>:*:table/burst-compute-<STAGE>-tasks",
    "Effect": "Allow"
},
```

## Reduce Interface

There are no expectations for the reduce function except that it should exist so that the framework can call it. 

Typically, the reduce function combines all of the individual intermediate results in the DynamoDB **TasksTable** which were produced by the worker functions. It then produces a final combined result. It may also do additional work such as writing the final result to S3, or notifying your web frontend via a Web Socket. 

Input:
```javascript
{
  jobId: "DynamoDB identifier where results should be written",
  jobParameters: {
    // Input parameter dictionary as received by dispatch function
  },
  startTime: "ISO date indicating when the job was started"
  elapsedSecs: "Number of seconds that elapsed since the job was started"
}
```

An example of fetching all non-empty intermediate results using Node.js. Note that using a consistent read is highly recommended
```javascript
  const params = {
      TableName: TASKS_TABLE_NAME,
      ConsistentRead: true,
      KeyConditionExpression: 'jobId = :jobId',
      FilterExpression: 'results <> :emptyList',
      ExpressionAttributeValues: {
          ':jobId': jobId,
          ':emptyList': '[ ]'
      },
    };
    
  const queryResult = await docClient.query(params).promise()
```

This function will requires dynamodb:Query permission to the **TasksTable**:

```json
{
    "Action": [
        "dynamodb:Query"
    ],
    "Resource": "arn:aws:dynamodb:<REGION>:*:table/burst-compute-<STAGE>-tasks",
    "Effect": "Allow"
},
```

## Dispatch Interface

You can invoke the dispatch function either synchronously or asynchronously, depending on whether or not you want the metadata it returns about the running job.

Input:
```javascript
{
  workerFunctionName: "Name or ARN of your worker Lambda function",
  reduceFunctionName: "Name or ARN of your reduce Lambda function",
  jobParameters: {
    // Any parameters that each worker should receive
  },
  startIndex: "Start index to process, inclusive, e.g. 0",
  endIndex: "End index to process, exclusive, e.g. N-1 if you have N items to process",
  batchSize: "How many items should each worker instance process",
  numLevels: "Number of levels in the dispatcher tree, e.g. 1 or 2",
  maxParallelism: "Maximum number of batches to run",
  searchTimeoutSecs: "Number of seconds to wait for job to finish before ending with a timeout"
}
```

Output:
```javascript
{
  jobId: "GUID assigned to the job instance",
  numBatches: "Number of batches to run, a.k.a. maximum parallelism"
}
```

## Resource References

Once the framework is deployed, you can write serverless applications that leverage it. The best way to reference the resource names is to use [cross-stack references](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/outputs-section-structure.html) using the provided CloudFormation Outputs. 

For example, if you are using Serverless Framework, you can reference the framework resources in your serverless.yml like this:
```yaml
custom:
  burstComputeStage: dev
  tasksTable: ${cf:burst-compute-${self:custom.burstComputeStage}.TasksTable}
  dispatchFunction: ${cf:burst-compute-${self:custom.burstComputeStage}.DispatchLambdaFunction}
```
