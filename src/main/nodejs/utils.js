'use strict';

const AWSXRay = require('aws-xray-sdk-core');
// const AWS = process.env.DISABLE_XRAY ? require('aws-sdk') : AWSXRay.captureAWS(require('aws-sdk'));
const AWS = require('aws-sdk');
const stream = require('stream');

AWS.config.apiVersions = {
    lambda: '2015-03-31',
    s3: '2006-03-01',
};

const s3 = new AWS.S3();
const lambda = process.env.DISABLE_XRAY ? new AWS.Lambda() : AWSXRay.captureAWSClient(new AWS.Lambda());
const stepFunction = new AWS.StepFunctions();

const DEBUG = !!process.env.DEBUG;
const RETRY_DELAY = 500;

// Retrieve all the keys in a particular bucket
const getAllKeys = async params => {
    const allKeys = [];
    var result;
    do {
        result = await s3.listObjectsV2(params).promise();
        result.Contents.forEach(obj => allKeys.push(obj.Key));
        params.ContinuationToken = result.NextContinuationToken;
    } while (result.NextContinuationToken);
    return allKeys;
};

// Retrieve a JSON file from S3
const getObject = async (bucket, key, defaultValue) => {
    try {
        if (DEBUG)
            console.log(`Getting object from ${bucket}:${key}`);
        const response = await s3.getObject({ Bucket: bucket, Key: key}).promise();
        const data = response.Body.toString();
        const jsonObject = JSON.parse(data);
        if (DEBUG)
            console.log(`Got object from ${bucket}:${key}:`, jsonObject);
        return jsonObject;
    } catch (e) {
        console.error(`Error getting object ${bucket}:${key}`, e);
        if (defaultValue === undefined) {
            throw e; // rethrow it
        } else {
            return defaultValue;
        }
    }
};

const sleep = async (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
}

const getObjectWithRetry = async (bucket, key, retries) => {
    for(let retry = 0; retry < retries; retry++) {
        try {
            return await getObject(bucket, key);
        } catch (e) {
            if (retry + 1 >= retries) {
                console.error(`Error getting object ${bucket}:${key} after ${retries} retries`, e);
                throw e;
            }
            await sleep(RETRY_DELAY);
        }
    }
}

// Retrieve a file from S3
const getS3Content = async (bucket, key) => {
    try {
        if (DEBUG)
            console.log(`Getting content from ${bucket}:${key}`);
        const response = await s3.getObject({ Bucket: bucket, Key: key}).promise();
        return response.Body;
    } catch (e) {
        console.error(`Error getting content ${bucket}:${key}`, e);
        throw e; // rethrow it
    }
};

const getS3ContentWithRetry = async (bucket, key, retries) => {
    for(let retry = 0; retry < retries; retry++) {
        try {
            return await getS3Content(bucket, key);
        } catch (e) {
            if (retry + 1 >= retries) {
                console.error(`Error getting content ${bucket}:${key} after ${retries} retries`, e);
                throw e;
            }
            await sleep(RETRY_DELAY);
        }
    }
}

const putObjectWithRetry = async (bucket, key, data, retries) => {
    for(let retry = 0; retry < retries; retry++) {
        try {
            return await putObject(bucket, key, data);
        } catch (e) {
            if (retry + 1 >= retries) {
                console.error(`Error putting content ${bucket}:${key} after ${retries} retries`, e);
                throw e;
            }
            await sleep(RETRY_DELAY);
        }
    }
}

// Write an object into S3 as JSON
const putObject = async (Bucket, Key, data) => {
    try {
        if (DEBUG)
            console.log(`Putting object to ${Bucket}:${Key}`);
        const res =  await s3.putObject({
            Bucket,
            Key,
            Body: JSON.stringify(data),
            ContentType: 'application/json'
        }).promise();
        if (DEBUG) {
            console.log(`Put object to ${Bucket}:${Key}:`, data, res);
        }
    } catch (e) {
        console.error('Error putting object', data, `to ${Bucket}:${Key}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`
};

// Write content to an S3 bucket
const putS3Content = async (Bucket, Key, contentType, content) => {
    try {
        if (DEBUG) {
            console.log(`Putting content to ${Bucket}:${Key}`);
        }
        const res = await s3.putObject({
            Bucket,
            Key,
            Body: content,
            ContentType: contentType
        }).promise();
        if (DEBUG) {
            console.log(`Put content to ${Bucket}:${Key}`, res);
        }
    } catch (e) {
        console.error('Error putting content', `to ${Bucket}:${Key}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`
};

const copyS3Content = async (Bucket, Source, Key) => {
   try {
        if (DEBUG) {
            console.log(`Copying content to ${Bucket}:${Key} from ${Source}`);
        }
        const res = await s3.copyObject({
            Bucket,
            CopySource: Source,
            Key,
        }).promise();
        if (DEBUG) {
            console.log(`Copied content to ${Bucket}:${Key} from ${Source}`, res);
        }
    } catch (e) {
        console.error(`Error copying content to ${Bucket}:${Key} from ${Source}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`

};

// Remove key from an S3 bucket
const removeKey = async (Bucket, Key) => {
    try {
        const res = await s3.deleteObject({
            Bucket,
            Key}).promise();
        console.log(`DeleteObject ${Bucket}:${Key}`, res);
    } catch (e) {
        console.error(`Error deleting object ${Bucket}:${Key}`, e);
    }
};

const streamObject = async (Bucket, Key, data) => {
    try {
        console.log(`Streaming object to ${Bucket}:${Key}`);
        const writeStream = new stream.PassThrough();
        const uploadPromise = s3.upload({
            Bucket,
            Key,
            Body: writeStream,
            ContentType: 'application/json'
        });

        const dataStream = new stream.Readable({objectMode: true});
        dataStream.pipe(writeStream);
        dataStream.on('end', () => {
            console.log('Finished writing the data stream');
        });
        // json serialiaze the data
        dataStream.push('{\n');
        Object.entries(data).forEach(([key, value],  index) => {
            if (index > 0)  {
                dataStream.push(',\n');
            }
            dataStream.push(`"${key}": `);
            if (Array.isArray(value)) {
                dataStream.push('[');
                value.forEach((arrayElem, arrayIndex) => {
                    if (arrayIndex > 0) {
                        dataStream.push(',\n');
                    }
                    dataStream.push(JSON.stringify(arrayElem));
                })
                dataStream.push(']');
            } else {
                dataStream.push(JSON.stringify(value));
            }
        });
        dataStream.push('\n}');
        dataStream.push(null);
        await uploadPromise.promise();
        console.log(`Finished streaming data to ${Bucket}:${Key}`);
    } catch (e) {
        console.error(`Error streaming data to ${Bucket}:${Key}`, e);
        throw e;
    }
    return `s3://${Bucket}/${Key}`
};

// Returns consecutive sublists of a list, each of the same size (the final list may be smaller)
const partition = (list, size) => {
    // If the size was passed in as a string, concatenation would happen instead of addition.
    const sublistSize = parseInt(size);
    const plist = [];
    for (var i = 0; i < list.length; i += sublistSize) {
        let arr = list.slice(i, i + sublistSize);
        plist.push(arr);
    }
    return plist;
}

// Invoke another Lambda function
const invokeFunction = async (functionName, parameters) => {
    if (DEBUG)
        console.log(`Invoke sync ${functionName} with`, parameters);
    const params = {
        FunctionName: functionName,
        Payload: JSON.stringify(parameters),
        //LogType: "Tail"
    };
    try {
        return await lambda.invoke(params).promise();
    } catch (e) {
        console.error('Error invoking', params, e);
        throw e;
    }
}

// Invoke another Lambda function asynchronously
const invokeAsync = async (functionName, parameters) => {
    if (DEBUG)
        console.log(`Invoke async ${functionName} with`, parameters);
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
}

// Start state machine
const startStepFunction = async (uniqueName, stateMachineParams, stateMachineArn) => {
    const params = {
        stateMachineArn: stateMachineArn,
        input: JSON.stringify(stateMachineParams),
        name: uniqueName
    };
    const result = await stepFunction.startExecution(params).promise();
    console.log("Step function started: ", result.executionArn);
    return result
}

// Verify that key exists on S3
const verifyKey = async (Bucket, Key) => {
    try {
        const response = await s3.headObject({
            Bucket,
            Key}).promise();
        console.log(`Found object ${Bucket}:${Key}`);
        return true;
    } catch (e) {
        console.error(`Error looking up ${Bucket}:${Key}`, e);
        return false;
    }
}

module.exports = {
    DEBUG,
    getAllKeys: getAllKeys,
    getObject: getObject,
    getObjectWithRetry: getObjectWithRetry,
    getS3Content: getS3Content,
    getS3ContentWithRetry: getS3ContentWithRetry,
    putObjectWithRetry: putObjectWithRetry,
    putObject: putObject,
    putS3Content: putS3Content,
    removeKey: removeKey,
    streamObject: streamObject,
    partition: partition,
    invokeFunction: invokeFunction,
    invokeAsync: invokeAsync,
    startStepFunction: startStepFunction,
    verifyKey: verifyKey,
    copyS3Content
};
