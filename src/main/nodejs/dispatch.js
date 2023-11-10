import { v1 as uuidv1 } from 'uuid';

import { LambdaClient, InvokeCommand, LogType } from '@aws-sdk/client-lambda';
import { SFNClient, StartExecutionCommand } from '@aws-sdk/client-sfn';

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
  batchSize: 50,
  numLevels: 2,
  jobParameters: {},
  maxParallelism: 3000,
};

const lambdaClient = new LambdaClient({});
const stepFunctionClient = new SFNClient({});

// Invoke another Lambda function synchronously
const invokeFunction = async (functionName, parameters) => {
  if (DEBUG) console.log(`Invoke sync ${functionName} with`, parameters);
  const params = {
    FunctionName: functionName,
    InvocationType: 'RequestResponse', // innvoke synchronously
    PayLoad: JSON.stringify(parameters),
    LogType: LogType.None,
  };
  const invokeCmd = new InvokeCommand(params);
  try {
    const result = await lambdaClient.send(invokeCmd);
    console.log(`Invoke ${functionName} result:`, result);
    return result;
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
  const result = await stepFunctionClient.send(new StartExecutionCommand(params));
  console.log('Step function started: ', result.executionArn);
  return result;
};

const adjustParallelismParams = (inputBatchSize, datasetSize, maxParallelism, numLevels) => {
  const numBatches = Math.ceil(datasetSize / inputBatchSize);
  console.log(`Partition ${datasetSize} dataset into ${numBatches} batches of size ${inputBatchSize}`);

  // the branchingFactor formula assumes that each leaf node at level = <numLevels> corresponds
  // to a batch of size <batchSize>
  //   nLeafNodes = datasetSize / batchSize = branchingFactor ^ numLevels
  //   e.g. ceil(695^(1/3)) = ceil(8.86) = 9
  const branchingFactor = Math.ceil(numBatches ** (1 / numLevels));

  if (numBatches > maxParallelism) {
    // adjust the batch size so that we don't exceed the max parallelism requested
    const adjustedBatchSize = Math.ceil(datasetSize / maxParallelism);
    const adjustedNumBatches = Math.ceil(datasetSize / adjustedBatchSize);
    console.log(`Capping batch size to ${adjustedBatchSize} due to max parallelism (${maxParallelism})`);

    const adjustedBranchingFactor = Math.ceil(adjustedNumBatches ** (1 / numLevels));

    return [adjustedBatchSize, adjustedNumBatches, adjustedBranchingFactor];
  }
  return [inputBatchSize, numBatches, branchingFactor];
};

// This Lambda is called recursively to dispatch all of the burst workers.
const dispatchBatches = async (input) => {
  // This next log statement is parsed by the analyzer. DO NOT CHANGE.
  console.log('Input:', input);

  // User defined parameters
  const { workerFunctionName, combinerFunctionName, searchTimeoutSecs = JOB_TIMEOUT_SECS } = input;
  const startIndex = parseInt(input.startIndex);
  const endIndex = parseInt(input.endIndex);

  // Parameters which have defaults
  const level = parseInt(input.level) || DEFAULTS.level;
  const numLevels = parseInt(input.numLevels) || DEFAULTS.numLevels;
  const jobParameters = input.jobParameters || DEFAULTS.jobParameters;
  const maxParallelism = input.maxParallelism || DEFAULTS.maxParallelism;
  const inputBatchSize = parseInt(input.batchSize) || DEFAULTS.batchSize;
  const inputNumBatches = parseInt(input.numBatches);
  const inputBranchingFactor = parseInt(input.branchingFactor);

  // Programmatic parameters. In the case of the root manager, these may be null initially
  // and then generated for later invocations.
  let { jobId } = input;
  const [batchSize, numBatches, branchingFactor] = level === 0
    ? adjustParallelismParams(inputBatchSize, endIndex, maxParallelism, numLevels)
    : [inputBatchSize, inputNumBatches, inputBranchingFactor];

  if (level === 0) {
    // This next log statement is parsed by the analyzer. DO NOT CHANGE.
    console.log('Root Dispatcher');
    const now = new Date();
    const startTime = now.toISOString();

    // Generate new job id
    jobId = uuidv1();

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
    console.log(`Starting state machine ${STATE_MACHINE_ARN} with:`, monitorParams);
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

  if (level + 1 < numLevels) {
    // start more intermediate dispatchers
    const subdispatcherjobs = Array.from(
      { length: (endIndex - startIndex) / nextLevelManagerRange + 1 },
      (v, index) => startIndex + index * nextLevelManagerRange,
    );
    const subdispatchInvocations = subdispatcherjobs.map(async (workerStart) => {
      const workerEnd = workerStart + nextLevelManagerRange > endIndex
        ? endIndex
        : workerStart + nextLevelManagerRange;
      const params = {
        startIndex: workerStart,
        endIndex: workerEnd,
        ...nextEvent,
      };
      const invokeResult = await invokeFunction(DISPATCH_FUNCTION_NAME, params);
      console.log(`Dispatched sub-dispatcher ${workerStart} - ${workerEnd}`);
      return invokeResult;
    });
    await Promise.all(subdispatchInvocations);
  } else {
    // this is the parent of leaf node (each leaf node corresponds to a batch) so start the batch
    const batchjobs = Array.from(
      { length: (endIndex - startIndex) / batchSize + 1 },
      (v, index) => startIndex + index * batchSize,
    );
    const workerInvocations = batchjobs.map(async (workerStart, workerIndex) => {
      const workerEnd = workerStart + batchSize > endIndex ? endIndex : workerStart + batchSize;
      const batchId = Math.ceil(startIndex / batchSize) + workerIndex;
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
      const invokeResult = await invokeFunction(workerFunctionName, params);
      console.log(`Dispatched worker #${batchId} to ${workerFunctionName}`);
      return invokeResult;
    });
    await Promise.all(workerInvocations);
  }

  return {
    jobId,
    numBatches,
    branchingFactor,
  };
};

// This Lambda is called recursively to dispatch all of the burst workers.
export const dispatchHandler = async (event) => {
  try {
    console.log('Input event:', JSON.stringify(event));
    const res = await dispatchBatches(event);
    return res;
  } catch (err) {
    console.error('Error dispatching batches for', event, err);
    throw err;
  }
};
