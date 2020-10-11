# Burst Compute

Serverless burst-compute implementation for AWS, using only native AWS services.

In the diagram below, the code you write is indicated by the blue lambda icons.

![Architecture Diagram](docs/burst-compute-diagram.png)

Here's how it works:
1) You define a *worker* function and a *reduce* function
2) Launch your burst compute job by calling the dispatch function with a range of items to process
3) The dispatcher will start copies of itself recursively and efficiently start your worker lambdas
4) Each *worker* is given a range of inputs and must compute results for those inputs and write results to DynamoDB
5) The Step Function monitors all the results and calls the reduce function when all workers are done
6) The *reduce* function reads all output from DynamoDB and aggregates them into the final result

See the [Interfaces](docs/Interfaces.md) document to find out how to define your worker and reducer functions.

## Build

You need Node.js 12.x or later in your path, then:

```bash
npm install
```

## Deployment

Follow the build instructions above before attempting to deploy.

To deploy this framework to your AWS account, you must have the (AWS CLI configured](https://www.serverless.com/framework/docs/providers/aws/guide/credentials#sign-up-for-an-aws-account).

To deploy to the *dev* stage:
```bash
npm run-script sls -- deploy
```

To deploy to production:
```bash
npm run sls -- deploy -s prod
```
