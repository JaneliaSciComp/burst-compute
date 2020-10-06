'use strict';

const AWS = require('aws-sdk')
const moment = require('moment')

const JOB_TIMEOUT_SECS = process.env.JOB_TIMEOUT_SECS
const JOB_TABLE_NAME = process.env.JOB_TABLE_NAME
var docClient = new AWS.DynamoDB.DocumentClient()

exports.monitorHandler = async (event) =>  {
    console.log(event);
    try {
        return await monitorJob(event);
    } catch (e) {
        console.log('Error while checking if job completed', event, e);
        return {
            ...event,
            completed: true,
            timedOut: false,
            withErrors: true
        };
    }
}
const monitorJob = async (jobParams) => {

    // Parameters
    const jobId = jobParams.jobId;
    const numBatches = jobParams.numBatches;
    const startTime = moment(jobParams.startTime);

    // Find out how many tasks are remaining
    const params = {
        TableName: JOB_TABLE_NAME,
        ConsistentRead: true,
        Select: "COUNT",
        KeyConditionExpression: 'id = :id',
        ExpressionAttributeValues: {
            ':id': jobId
        },
    };
    console.log('Fetching result count: ', params)
    const countResult = await docClient.query(params).promise()
    const numComplete = countResult.Count
    console.log(`Tasks completed: ${numComplete}/${numBatches}`);

    // Calculate total time
    const now = new Date();
    const endTime = moment(now.toISOString());
    const elapsedSecs = endTime.diff(startTime, "s");
    const numRemaining = numBatches - numComplete;
    
    // Return result for next state input
    if (numRemaining === 0) {
        console.log(`Job took ${elapsedSecs} seconds`);
        return {
            ...jobParams,
            elapsedSecs: elapsedSecs,
            numRemaining: 0,
            completed: true,
            timedOut: false
        };
    } else if (elapsedSecs > JOB_TIMEOUT_SECS) {
        console.log(`Job timed out after ${elapsedSecs} seconds. Completed ${numComplete} of ${numBatches} tasks.`);
        return {
            ...jobParams,
            elapsedSecs: elapsedSecs,
            numRemaining: numRemaining,
            completed: false,
            timedOut: true
        };
    } else {
        console.log(`Job still running after ${elapsedSecs} seconds. Completed ${numComplete} of ${numBatches} tasks.`);
        return {
            ...jobParams,
            elapsedSecs: elapsedSecs,
            numRemaining: numRemaining,
            completed: false,
            timedOut: false
        };
    }
}