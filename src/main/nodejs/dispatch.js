import { v1 as uuidv1 } from 'uuid';

import { SFNClient, StartExecutionCommand } from '@aws-sdk/client-sfn';

const {
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

const stepFunctionClient = new SFNClient({});

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

const adjustParallelismParams = (inputBatchSize, datasetSize, maxParallelism) => {
  const numBatches = Math.ceil(datasetSize / inputBatchSize);
  console.log(`Partition ${datasetSize} dataset into ${numBatches} batches of size ${inputBatchSize}`);

  if (numBatches > maxParallelism) {
    // adjust the batch size so that we don't exceed the max parallelism requested
    const adjustedBatchSize = Math.ceil(datasetSize / maxParallelism);
    const adjustedNumBatches = Math.ceil(datasetSize / adjustedBatchSize);
    console.log(`Capping batch size to ${adjustedBatchSize} due to max parallelism (${maxParallelism})`);

    return [adjustedBatchSize, adjustedNumBatches];
  }
  return [inputBatchSize, numBatches];
};

const startBatchJobs = async (input) => {
  // This next log statement is parsed by the analyzer. DO NOT CHANGE.
  console.log('Input:', input);

  // User defined parameters
  const { workerFunctionName, combinerFunctionName, searchTimeoutSecs = JOB_TIMEOUT_SECS } = input;
  const startIndex = parseInt(input.startIndex);
  const endIndex = parseInt(input.endIndex);

  // Parameters which have defaults
  const jobParameters = input.jobParameters || DEFAULTS.jobParameters;
  const maxParallelism = input.maxParallelism || DEFAULTS.maxParallelism;
  const inputBatchSize = parseInt(input.batchSize) || DEFAULTS.batchSize;

  // Generate new job id
  const jobId = uuidv1();

  const [batchSize, numBatches] = adjustParallelismParams(inputBatchSize, endIndex, maxParallelism);

  const batchRanges = Array.from(
    { length: (endIndex - startIndex) / batchSize + 1 },
    (v, index) => startIndex + index * batchSize,
  );

  const batchesParams = batchRanges.map((batchStart, batchId) => {
    const batchEnd = batchStart + batchSize > endIndex ? endIndex : batchStart + batchSize;
    return {
      batchId,
      startIndex: batchStart,
      endIndex: batchEnd,
    };
  });

  const now = new Date();
  const startTime = now.toISOString();

  const workflowParams = {
    jobId,
    jobParameters,
    batchSize,
    numBatches: batchesParams.length,
    startTime,
    searchTimeoutSecs,
    tasksTableName: TASKS_TABLE_NAME,
    workerFunctionName,
    combinerFunctionName,
    monitorFunctionName: MONITOR_FUNCTION_NAME,
    batches: batchesParams,
  };

  console.log(`Starting state machine ${STATE_MACHINE_ARN} - job ${jobId} to process ${numBatches} batches `, workflowParams);

  const startWorkflowRes = await startStepFunction(STATE_MACHINE_ARN, workflowParams, jobId);
  // This next log statement is parsed by the analyzer. DO NOT CHANGE.
  console.log(`Job Id: ${jobId}`);

  return {
    jobId,
    numBatches,
    workflowArn: startWorkflowRes.executionArn,
  };
};

// This Lambda is called recursively to dispatch all of the burst workers.
export const dispatchHandler = async (event) => {
  try {
    console.log('Input event:', JSON.stringify(event));
    const res = await startBatchJobs(event);
    return res;
  } catch (err) {
    console.error('Error dispatching batches for', event, err);
    throw err;
  }
};
