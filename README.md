# Burst Compute Framework

[![DOI](https://zenodo.org/badge/301732359.svg)](https://zenodo.org/badge/latestdoi/301732359)
![CI workflow](https://github.com/JaneliaSciComp/burst-compute/actions/workflows/node.js.yml/badge.svg)

Serverless burst-compute implementation for AWS, using only native AWS services.

As seen in the [AWS Architecture Blog](https://aws.amazon.com/blogs/architecture/scaling-neuroscience-research-on-aws/).

For [embarassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel) workloads, **N** items may be trivially processed by **T** threads. Given **N** items, and a **batchSize** (the maximum number of items to be processed by a single process in serial), we divide the work into **N/batchSize** batches and invoke that many user-provided **worker** Lambdas. When all the worker functions are done, the results are combined by the user-provided **combiner** Lambda. 

In the diagram below, the code you write is indicated by the blue lambda icons.

![Architecture Diagram](docs/burst-compute-diagram.png)

Here's how it works, step-by-step:
1) You define a **worker** function and a **combiner** function
2) Launch your burst compute job by calling the **dispatch** function with a range of items to process
3) The dispatcher partitions the work into batches and starts an AWS Step Function that implements a map-reduce workflow.
4) The Step Function maps all the batches to workers, where each **worker** is given a range of inputs, computes the results for those inputs and then sends them to another workflow task that persists these results to a DynamoDB table.
5) When all wotkers complete and their results are persisted to the database, the Step Function invokes the **combiner** that reads all outputs from the DynamoDB table and aggregates them into the final result.
6) The Step Function also monitors for a timeout and terminates the process if the time configured time limit is reached.

## Build

You need Node.js 20.x or later in your path, then:

```bash
npm install
```

## Deployment

Follow the build instructions above before attempting to deploy.

Deployment will create all the necessary AWS services, including Lambda functions, DynamoDB tables, and Step Functions. To deploy this framework to your AWS account, you must have the [AWS CLI configured](https://www.serverless.com/framework/docs/providers/aws/guide/credentials#sign-up-for-an-aws-account). 

To deploy to the *dev* stage:
```bash
npm run sls -- deploy
```

This will create an application stack named `janelia-burst-compute-dev`. 

To deploy to a different stage (e.g. "prod") and a different organization (the default organtization is 'janelia'), add a stage and an org argument:
```bash
npm run sls -- deploy -s prod --org myorg
```

This will create an application stack named
`myorg-burst-compute-dev`

## Usage

1. Create **worker** and **combiner** functions which follow the input/output specification defined in the [Interfaces](docs/Interfaces.md) document.
2. Invoke the **dispatch** function to start a burst job. 
