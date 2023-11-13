import { v1 as uuidv1 } from 'uuid';

import { SFNClient, StartExecutionCommand } from '@aws-sdk/client-sfn';

const DEBUG = !!process.env.DEBUG;
const {
  DISPATCH_FUNCTION_NAME,
  MONITOR_FUNCTION_NAME,
  STATE_MACHINE_ARN,
  JOB_TIMEOUT_SECS,
  TASKS_TABLE_NAME,
  DEFAULT_CONCURRENT_JOBS,
} = process.env;

const DEFAULTS = {
  level: 0,
  batchSize: 50,
  numLevels: 2,
  jobParameters: {},
  maxParallelism: 4608,
};

const stepFunctionClient = new SFNClient({});

const defaultConcurrentJobs = parseInt(DEFAULT_CONCURRENT_JOBS) || 4000;

// Start state machine
const startStepFunction = async (stateMachineArn, stateMachineParams, uniqueName) => {
  const params = {
    stateMachineArn,
    input: JSON.stringify(stateMachineParams, null, null), // no spacing to reduce size
    name: uniqueName,
  };
  const result = await stepFunctionClient.send(new StartExecutionCommand(params));
  console.log('Step function started: ', result.executionArn);
  return result;
};

const computeNumBatches = (inputBatchSize, datasetSize) => {
  const numBatches = Math.ceil(datasetSize / inputBatchSize);
  console.log(`Partition ${datasetSize} dataset into ${numBatches} batches of size ${inputBatchSize}`);

  return [inputBatchSize, numBatches];
};

const prepareBatchJobs = async (input) => {
  // This next log statement is parsed by the analyzer. DO NOT CHANGE.
  console.log('Input:', input);

  // User defined parameters
  const {
    jobId, workerFunctionName, combinerFunctionName,
    searchTimeoutSecs = JOB_TIMEOUT_SECS,
  } = input;
  const datasetStartIndex = parseInt(input.datasetStartIndex);
  const datasetEndIndex = parseInt(input.datasetEndIndex);
  const lastBatchId = parseInt(input.lastBatchId) || 0;
  const inputNumBatches = parseInt(input.numBatches) || -1;

  // Parameters which have defaults
  const jobParameters = input.jobParameters || DEFAULTS.jobParameters;
  const maxParallelism = input.maxParallelism || DEFAULTS.maxParallelism;
  const inputBatchSize = parseInt(input.batchSize) || DEFAULTS.batchSize;

  const [batchSize, numBatches] = jobId === undefined || jobId === null
    ? computeNumBatches(inputBatchSize, datasetEndIndex - datasetStartIndex)
    : [inputBatchSize, inputNumBatches];

  const batchRanges = Array.from(
    { length: (datasetEndIndex - datasetStartIndex) / batchSize + 1 },
    (v, index) => datasetStartIndex + index * batchSize,
  );

  const batchesParams = batchRanges
    .map((batchStart, batchId) => {
      const batchEnd = batchStart + batchSize >= datasetEndIndex
        ? datasetEndIndex
        : batchStart + batchSize;
      return {
        batchId,
        startIndex: batchStart, // job start and end index
        endIndex: batchEnd,
      };
    })
    .filter((batchParams) => batchParams.batchId >= lastBatchId);

  const maxConcurrentJobs = defaultConcurrentJobs < maxParallelism
    ? defaultConcurrentJobs
    : maxParallelism;

  console.log(
    `Max concurrent jobs: ${maxConcurrentJobs}, `,
    `nbatches: ${numBatches}`,
    `batches length: ${batchesParams.length}`,
  );

  const nextBatchId = batchesParams.length > maxConcurrentJobs
    ? lastBatchId + maxConcurrentJobs
    : numBatches;

  console.log(`Next batch: ${lastBatchId} - ${nextBatchId}`);
  const now = new Date();
  const startTime = now.toISOString();

  // if there are too many batches we limit the dispatch to a subset of the batchjobs
  // and the state machine will take care of invoking the next batches.
  // this limitation is because of two reasons
  // 1. the max lambda concurrency
  // 2. the size of the step function input which now is limited to 256K
  //    the lambdas support larger inputs but for that we have to dump the batches on an S3 bucket
  return [jobId, {
    datasetStartIndex, // global start index
    datasetEndIndex, // global end index
    jobParameters,
    batchSize,
    firstBatchId: lastBatchId,
    lastBatchId: nextBatchId,
    numBatches,
    startTime,
    searchTimeoutSecs,
    tasksTableName: TASKS_TABLE_NAME,
    dispatchFunctionName: DISPATCH_FUNCTION_NAME,
    workerFunctionName,
    combinerFunctionName,
    monitorFunctionName: MONITOR_FUNCTION_NAME,
    batches: batchesParams.slice(0, nextBatchId - lastBatchId),
  }];
};

// This Lambda is called recursively to dispatch all of the burst workers.
export const dispatchHandler = async (event) => {
  try {
    console.log('Input event:', JSON.stringify(event));

    const [jobId, batchJobsInputs] = await prepareBatchJobs(event);
    if (jobId === undefined || jobId === null) {
      // if this is the first call - no jobId yet - start the workflow
      // also this will be considered the root dispatcher,
      // since this starts the state machine
      // the next log statement is parsed by the analyzer. DO NOT CHANGE.
      console.log('Root Dispatcher');

      const workflowParams = {
        jobId: uuidv1(),
        ...batchJobsInputs,
      };
      console.log(
        `Starting state machine ${STATE_MACHINE_ARN}`,
        `job ${workflowParams.jobId} => ${workflowParams.numBatches} batches `,
        workflowParams,
        ` - stringified size: ${JSON.stringify(workflowParams).length}`,
      );
      const startWorkflowRes = await startStepFunction(
        STATE_MACHINE_ARN,
        workflowParams,
        workflowParams.jobId,
      );

      // This next log statement is parsed by the analyzer. DO NOT CHANGE.
      console.log(`Job Id: ${workflowParams.jobId}`);

      return {
        jobId: workflowParams.jobId,
        numBatches: workflowParams.numBatches,
        workflowArn: startWorkflowRes.executionArn,
      };
    }
    // else dispatch the next batches

    // This next log statement is parsed by the analyzer. DO NOT CHANGE.
    console.log(
      'Dispatching Batch Id: ',
      `${batchJobsInputs.firstBatchId}-${batchJobsInputs.lastBatchId}`,
    );

    if (DEBUG) {
      console.log(
        `Dispatch parameters for ${jobId} `,
        `between ${batchJobsInputs.firstBatchId} to ${batchJobsInputs.lastBatchId} `,
        `out of total ${batchJobsInputs.numBatches} batches `,
        batchJobsInputs,
        ` - stringified size: ${JSON.stringify(batchJobsInputs).length}`,
      );
    }

    return {
      jobId,
      ...batchJobsInputs,
    };
  } catch (err) {
    console.error('Error dispatching batches for', event, err);
    throw err;
  }
};
