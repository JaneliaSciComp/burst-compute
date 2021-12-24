# Function interfaces

In order to use the burst compute framework, you need to define a **worker** function and a **combiner** function. 

## Worker Interface

You can define the worker function to do anything you like. Each worker should operate on the subset of objects defined by the range [startIndex-endIndex). Once the work is done, a single row must be added to DynamoDB to record the worker's results. 

Workers should accept the following input object:
```javascript
{
  tasksTableName: "name of the DynamoDB table where results should be written"
  jobId: "Unique id for the job. Used as the DynamoDB partition key for the results.",
  batchId: "Unique id for the batch. Used as the DynamoDB sort key for the results.",
  startIndex: "Start index to process, inclusive, e.g. 0",
  endIndex: "End index to process, exclusive, e.g. B if your batch size is B",
  jobParameters: {
    // Input parameter dictionary as received by dispatch function
  },
}
```

After doing some computation, each function instance writes results as a new row in the DynamoDB **TasksTable** with the given **jobId** and **batchId** as the partition and sort key, respectively. Typically, the row's *ttl* should be set to a small value like 1 hour or 1 day, so that this temporary result is cleaned up after its usefulness is outlived. Example code in Java:
```java
    long now = Instant.now().getEpochSecond(); // unix time
    long ttl = now + 60 * 60; // 60 minutes
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("jobId", AttributeValue.builder().s(jobId).build());
    item.put("batchId", AttributeValue.builder().n(batchId.toString()).build());
    item.put("ttl", AttributeValue.builder().n(ttl+"").build());
    item.put("results", AttributeValue.builder().s(toJson(results)).build());
    PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).build();
    dynamoDbClient.putItem(putItemRequest);
```

The **results** can be any item supported by DynamoDB, including JSON objects and arrays. In fact, there is nothing special about the attribute name "results". Since DynamoDB is schemaless, you can add multiple attributes if you need to. 

This function will require dynamodb:PutItem permission to the **TasksTable**, e.g.:

```javascript
{
    "Action": [
        "dynamodb:PutItem"
    ],
    "Resource": "arn:aws:dynamodb:<REGION>:*:table/burst-compute-<STAGE>-tasks",
    "Effect": "Allow"
},
```

## Combiner Interface

There are no expectations for the combiner function except that it should exist so that the framework can call it. 

Typically, the combiner function *combines* all of the individual intermediate results in the DynamoDB **TasksTable** that were produced by the worker functions. It then produces a final combined result. It may also do additional work such as writing the final result to S3, or notifying your web frontend via a Web Socket, but anything it does is application-specific. 

Combiners should accept the following input object:
```javascript
{
  tasksTableName: "Name of the DynamoDB table from which results should be read",
  jobId: "Unique id for the job. Used as the DynamoDB partition id for querying the tasks table.",
  startTime: "ISO date indicating when the job was started",
  elapsedSecs: "Number of seconds that elapsed since the job was started",
  jobParameters: {
    // Input parameter dictionary as received by dispatch function
  },
}
```

An example of fetching all non-empty intermediate results using NodeJS. Using a consistent read is highly recommended to ensure that all results are collected.
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
```javascript
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

The dispatch function expects the following input object:
```javascript
{
  workerFunctionName: "Name or ARN of the user-defined worker Lambda function",
  combinerFunctionName: "Name or ARN of the user-defined combiner Lambda function",
  startIndex: "Start index to process, inclusive, e.g. 0",
  endIndex: "End index to process, exclusive, e.g. N if you have N items to process",
  batchSize: "How many items should each worker instance process",
  numLevels: "Number of levels in the dispatcher tree, e.g. 1 or 2",
  maxParallelism: "Maximum number of batches to run",
  searchTimeoutSecs: "Number of seconds to wait for job to finish before ending with a timeout",
  jobParameters: {
    // Any parameters that each worker should receive
  },
}
```

Output:
```javascript
{
  jobId: "GUID assigned to the job instance",
  numBatches: "Number of batches to run, a.k.a. maximum parallelism"
}
```

## Cross-Stack References

If you don't want to hardcode the resource names of the dispatch function and tasks table, you can use [cross-stack references](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/outputs-section-structure.html) using the provided CloudFormation Outputs. 

For example, if you are using Serverless Framework, you can reference the framework resources in your serverless.yml like this:
```yaml
custom:
  burstComputeStage: dev
  tasksTable: ${cf:burst-compute-${self:custom.burstComputeStage}.TasksTable}
  dispatchFunction: ${cf:burst-compute-${self:custom.burstComputeStage}.DispatchLambdaFunction}
```
