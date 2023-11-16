import moment from 'moment';
import { v1 as uuidv1 } from 'uuid';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { SFNClient, StartExecutionCommand } from '@aws-sdk/client-sfn';

const {
  DISPATCH_FUNCTION_NAME,
  MONITOR_FUNCTION_NAME,
  STATE_MACHINE_ARN,
  JOB_TIMEOUT_SECS,
  MAP_JOBS_INPUTS_BUCKET_NAME,
  TASKS_TABLE_NAME,
  DEFAULT_CONCURRENT_JOBS,
} = process.env;

const DEFAULTS = {
  level: 0,
  batchSize: 50,
  numLevels: 2,
  jobParameters: {},
  maxParallelism: 20608,
};

const s3Client = new S3Client();
const stepFunctionClient = new SFNClient();

const defaultConcurrentJobs = parseInt(DEFAULT_CONCURRENT_JOBS) || DEFAULTS.maxParallelism;

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

const putObject = async (Bucket, Key, object) => {
  const inputsTTL = 15 * 60; // 15 min TTL

  const res = await s3Client.send(new PutObjectCommand({
    Bucket,
    Key,
    Body: JSON.stringify(object),
    ContentType: 'application/json',
    Expires: moment(new Date()).add(inputsTTL, 'm').toDate(),
  }));

  return res;
};

const computeNumBatches = (batchSize, datasetSize) => {
  const numBatches = Math.ceil(datasetSize / batchSize);
  console.log(`Partition ${datasetSize} dataset into ${numBatches} batches of size ${batchSize}`);

  return numBatches;
};

const prepareWorkflowParams = (input) => {
  // This next log statement is parsed by the analyzer. DO NOT CHANGE.
  console.log('Input:', input);
  const {
    workerFunctionName,
    combinerFunctionName,
    jobsTimeoutSecs = JOB_TIMEOUT_SECS,
    jobParameters = DEFAULTS.jobParameters,
  } = input;

  const datasetStartIndex = parseInt(input.datasetStartIndex);
  const datasetEndIndex = parseInt(input.datasetEndIndex);
  const maxParallelism = parseInt(input.maxParallelism) || DEFAULTS.maxParallelism;
  const batchSize = parseInt(input.batchSize) || DEFAULTS.batchSize;
  const numBatches = computeNumBatches(batchSize, datasetEndIndex - datasetStartIndex);
  const startTime = new Date().toISOString();

  const maxConcurrentJobs = defaultConcurrentJobs < maxParallelism
    ? defaultConcurrentJobs
    : maxParallelism;

  return {
    jobId: uuidv1(),
    datasetStartIndex, // global start index
    datasetEndIndex, // global end index
    jobParameters,
    maxParallelism: maxConcurrentJobs,
    batchSize,
    numBatches,
    startTime,
    jobsTimeoutSecs,
    dispatchFunctionName: DISPATCH_FUNCTION_NAME,
    monitorFunctionName: MONITOR_FUNCTION_NAME,
    workerFunctionName,
    combinerFunctionName,
    mapJobsInputBucket: MAP_JOBS_INPUTS_BUCKET_NAME,
    tasksTableName: TASKS_TABLE_NAME,
  };
};

const prepareBatchJobs = async (input) => {
  console.log('Prepare next batch:', input);

  const {
    jobId,
    datasetStartIndex,
    datasetEndIndex,
    maxParallelism,
    batchSize,
    lastBatchId = 0,
    numBatches,
    mapJobsInputBucket,
  } = input;

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

  console.log(`Found ${batchesParams.length} remaining batches`);

  // if there are too many batches we limit the dispatch to a subset of the batchjobs
  // and the state machine will take care of invoking the next batches.
  const nextBatchId = batchesParams.length > maxParallelism
    ? lastBatchId + maxParallelism
    : numBatches;

  console.log(`Next batch: ${lastBatchId} - ${nextBatchId}`);

  const batchesToProcess = batchesParams.slice(0, nextBatchId - lastBatchId);
  const inputJobsFile = `${jobId}-${lastBatchId}-${nextBatchId}.json`;
  await putObject(mapJobsInputBucket, inputJobsFile, batchesToProcess);
  return {
    ...input,
    firstBatchId: lastBatchId,
    lastBatchId: nextBatchId,
    inputJobsFile,
  };
};

// This handler is called first time to begin the workflow
export const dispatchHandler = async (event) => {
  // the next log statement is parsed by the analyzer. DO NOT CHANGE.
  console.log('Root Dispatcher');

  // First time we prepare all workflow parameters
  // including the batches that need to be processed
  const commonWorkflowParams = prepareWorkflowParams(event);

  const workflowParams = await prepareBatchJobs(commonWorkflowParams);

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
};

export const dispatchNextBatchesHandler = async (event) => {
  const nextBatchParams = await prepareBatchJobs(event);
  console.log(`Next batch for ${event.jobId}`, nextBatchParams);
  return nextBatchParams;
};
