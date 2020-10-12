const { v1: uuidv1 } = require('uuid');
const AWS = require('aws-sdk');

const DEBUG = !!process.env.DEBUG;
const {
  DISPATCH_FUNCTION_NAME,
  MONITOR_FUNCTION_NAME,
  STATE_MACHINE_ARN,
  JOB_TIMEOUT_SECS,
  TASKS_TABLE_NAME,
} = process.env;

const DEFAULTS = {
  level: 0,
  numLevels: 2,
  jobParameters: {},
  maxParallelism: 3000,
};

AWS.config.apiVersions = {
  lambda: '2015-03-31',
  s3: '2006-03-01',
};

const lambda = new AWS.Lambda();
const stepFunction = new AWS.StepFunctions();

// Invoke another Lambda function asynchronously
const invokeAsync = async (functionName, parameters) => {
  if (DEBUG) console.log(`Invoke async ${functionName} with`, parameters);
  const params = {
    FunctionName: functionName,
    InvokeArgs: JSON.stringify(parameters),
  };
  try {
    return await lambda.invokeAsync(params).promise();
  } catch (e) {
    console.error('Error invoking', params, e);
    throw e;
  }
};

// Start state machine
const startStepFunction = async (stateMachineArn, stateMachineParams, uniqueName) => {
  const params = {
    stateMachineArn,
    input: JSON.stringify(stateMachineParams),
    name: uniqueName,
  };
  const result = await stepFunction.startExecution(params).promise();
  console.log('Step function started: ', result.executionArn);
  return result;
};

exports.dispatchHandler = async (event) => {
  // This next log statement is parsed by the analyzer. DO NOT CHANGE.
  console.log('Input event:', JSON.stringify(event));

  // User defined parameters
  const { workerFunctionName, combinerFunctionName, searchTimeoutSecs = JOB_TIMEOUT_SECS } = event;
  const startIndex = parseInt(event.startIndex);
  const endIndex = parseInt(event.endIndex);
  let batchSize = parseInt(event.batchSize);

  // Parameters which have defaults
  const level = parseInt(event.level) || DEFAULTS.level;
  const numLevels = parseInt(event.numLevels) || DEFAULTS.numLevels;
  const jobParameters = event.jobParameters || DEFAULTS.jobParameters;
  const maxParallelism = event.maxParallelism || DEFAULTS.maxParallelism;

  // Programmatic parameters. In the case of the root manager, these may be null initially
  // and then generated for later invocations.
  let { jobId } = event;
  let numBatches = parseInt(event.numBatches);
  let branchingFactor = parseInt(event.branchingFactor);

  if (level === 0) {
    // This next log statement is parsed by the analyzer. DO NOT CHANGE.
    console.log('Root Dispatcher');
    const now = new Date();
    const startTime = now.toISOString();

    // Generate new job id
    jobId = uuidv1();

    numBatches = Math.ceil(endIndex / batchSize);
    console.log(`Partition ${endIndex} searches into ${numBatches} of size ${batchSize}`);
    if (numBatches > maxParallelism) {
      // adjust the batch size so that we don't exceed the max parallelism requested
      batchSize = Math.ceil(endIndex / maxParallelism);
      numBatches = Math.ceil(endIndex / batchSize);
      console.log(`Capping batch size to ${batchSize} due to max parallelism (${maxParallelism})`);
    }
    // the branchingFactor formula assumes that each leaf node at level = <numLevels> corresponds
    // to a batch of size <batchSize>
    //   nLeafNodes = totalSearches / batchSize = branchingFactor ^ numLevels
    //   e.g. ceil(695^(1/3)) = ceil(8.86) = 9
    branchingFactor = Math.ceil(numBatches ** (1 / numLevels));

    // Start monitoring
    const monitorParams = {
      jobId,
      jobParameters,
      numBatches,
      searchTimeoutSecs,
      startTime,
      monitorFunctionName: MONITOR_FUNCTION_NAME,
      combinerFunctionName,
      tasksTableName: TASKS_TABLE_NAME,
    };
    console.log(`Starting state machine ${STATE_MACHINE_ARN}`);
    await startStepFunction(STATE_MACHINE_ARN, monitorParams, jobId);
  }

  // This next log statement is parsed by the analyzer. DO NOT CHANGE.
  console.log(`Job Id: ${jobId}`);

  const nextLevelManagerRange = (branchingFactor ** (numLevels - level - 1)) * batchSize;
  console.log(`Level ${level} -> next range: ${nextLevelManagerRange}`);
  const nextEvent = {
    level: level + 1,
    numLevels,
    jobId,
    jobParameters,
    batchSize,
    numBatches,
    branchingFactor,
    workerFunctionName,
    combinerFunctionName,
    searchTimeoutSecs,
  };

  const invokePromises = [];

  if (level + 1 < numLevels) {
    // start more intermediate dispatchers
    let index = 0;
    for (let i = startIndex; i < endIndex; i += nextLevelManagerRange) {
      const workerStart = i;
      const workerEnd = i + nextLevelManagerRange > endIndex ? endIndex : i + nextLevelManagerRange;
      const params = {
        startIndex: workerStart,
        endIndex: workerEnd,
        ...nextEvent,
      };
      invokePromises.push(invokeAsync(DISPATCH_FUNCTION_NAME, params));
      console.log(`(Promise#${index}) Dispatched sub-dispatcher ${workerStart} - ${workerEnd}`);
      index += 1;
    }
  } else {
    // this is the parent of leaf node (each leaf node corresponds to a batch) so start the batch
    let batchId = Math.ceil(startIndex / batchSize);
    let index = 0;
    for (let i = startIndex; i < endIndex; i += batchSize) {
      const workerStart = i;
      const workerEnd = i + batchSize > endIndex ? endIndex : i + batchSize;
      // This next log statement is parsed by the analyzer. DO NOT CHANGE.
      console.log(`Dispatching Batch Id: ${batchId}`);
      const params = {
        jobId,
        batchId,
        jobParameters,
        startIndex: workerStart,
        endIndex: workerEnd,
        tasksTableName: TASKS_TABLE_NAME,
        ...nextEvent,
      };
      invokePromises.push(invokeAsync(workerFunctionName, params));
      console.log(`(Promise#${index}) Dispatched worker #${batchId} to ${workerFunctionName}`);
      batchId += 1;
      index += 1;
    }
  }

  const responses = await Promise.all(invokePromises);
  responses.forEach((r, i) => console.log(`(Promise#${i}) status=${r.Status}`));

  return {
    jobId,
    numBatches,
    branchingFactor,
  };
};
