# Burst Compute

Serverless burst-compute implementation for AWS, using only native AWS services.

Here's how it works (the code you write is in bold):
1) You define a *worker* function and a *reduce* function
2) Launch your burst compute job by calling the dispatch function with a range of items to process
3) The dispatcher will start copies of itself recursively and efficiently start your worker lambdas
4) Each *worker* is given a range of inputs and must compute results for those inputs and write results to DynamoDB
5) The Step Function monitors all the results and calls the reduce function when all workers are done
6) The *reduce* function reads all output from DynamoDB and aggregates them into the final result

## Dispatch interface:

Input:
```
{
  jobParameters: {
    // Any parameters that each worker should receive
  }
}
```

Output:
```
{
  jobId: "",
  numBatches: ""
}
```

## Worker interface:

Input:
```
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

Side effects:





##Deployment
```
npm run sls -- deploy
```


