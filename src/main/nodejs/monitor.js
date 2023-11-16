import moment from 'moment';
import { DynamoDBDocumentClient, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';

const ddbClient = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

// This Lambda is called by the monitor step function to see if the job is completed.
// It checks DynamoDB to see if all the tasks are done and if so, returns the completed:true
// in the result object.
const monitorJob = async (monitorInput) => {
  // Parameters
  const {
    jobId,
    numBatches,
    jobsTimeoutSecs,
    tasksTableName,
  } = monitorInput;

  const startTime = moment(monitorInput.startTime);

  // Find out how many tasks are remaining
  const params = {
    TableName: tasksTableName,
    ConsistentRead: true,
    Select: 'COUNT',
    KeyConditionExpression: 'jobId = :jobId',
    ExpressionAttributeValues: {
      ':jobId': jobId,
    },
    ScanIndexForward: true,
    ReturnConsumedCapacity: 'TOTAL',
  };

  console.log('Fetching result count: ', params);

  let numComplete = 0;
  let countResult;
  do {
    // eslint-disable-next-line no-await-in-loop
    countResult = await ddbDocClient.send(new QueryCommand(params));
    console.log('Scanned count:', JSON.stringify(countResult));
    numComplete += countResult.Count;
    params.ExclusiveStartKey = countResult.LastEvaluatedKey;
  } while (countResult.LastEvaluatedKey);

  console.log(`Tasks completed: ${numComplete}/${numBatches}`);

  // Calculate total time
  const now = new Date();
  const endTime = moment(now.toISOString());
  const elapsedSecs = endTime.diff(startTime, 's');
  const numRemaining = numBatches - numComplete;

  // Return result for next state input
  if (numRemaining === 0) {
    console.log(`Job took ${elapsedSecs} seconds`);
    return {
      ...monitorInput,
      elapsedSecs,
      numRemaining: 0,
      completed: true,
      timedOut: false,
    };
  }
  if (elapsedSecs > jobsTimeoutSecs) {
    console.log(`Job timed out after ${elapsedSecs} seconds. Completed ${numComplete} of ${numBatches} tasks.`);
    return {
      ...monitorInput,
      elapsedSecs,
      numRemaining,
      completed: false,
      timedOut: true,
    };
  }
  console.log(`Job still running after ${elapsedSecs} seconds (${jobsTimeoutSecs - elapsedSecs}s remaining). Completed ${numComplete} of ${numBatches} tasks.`);
  return {
    ...monitorInput,
    elapsedSecs,
    numRemaining,
    completed: false,
    timedOut: false,
  };
};

export const monitorHandler = async (event) => {
  console.log('Input event:', JSON.stringify(event));
  try {
    return await monitorJob(event);
  } catch (e) {
    console.log('Error while checking if job completed', event, e);
    return {
      ...event,
      completed: true,
      timedOut: false,
      withErrors: true,
    };
  }
};
