'use strict';

const AWS = require('aws-sdk');
const { v1: uuidv1 } = require('uuid');
const { invokeAsync, startStepFunction } = require('./utils');

const DEFAULTS = {
    level: 0,
    numLevels: 2,
    jobParameters: {}
};

const MAX_PARALLELISM = process.env.MAX_PARALLELISM || 3000
const dispatchFunctionArn = process.env.DISPATCH_FUNCTION_ARN
const monitorFunctionArn = process.env.MONITOR_FUNCTION_ARN

exports.searchDispatch = async (event) => {

    // This next log statement is parsed by the analyzer. DO NOT CHANGE.
    console.log('Input event:', JSON.stringify(event))

    const workerFunctionArn = event.workerFunctionArn
    const reduceFunctionArn = event.reduceFunctionArn
    
    // Parameters which have defaults
    const level = parseInt(event.level) || DEFAULTS.level
    const numLevels = parseInt(event.numLevels) || DEFAULTS.numLevels
    const jobParameters = event.jobParameters || DEFAULTS.jobParameters

    // Programmatic parameters. In the case of the root manager, these will be null initially and then generated for later invocations.
    let jobId = event.jobId
    let libraries = event.libraries
    let monitorName = event.monitorName
    let batchSize = parseInt(event.batchSize)
    let numBatches = parseInt(event.numBatches)
    let branchingFactor = parseInt(event.branchingFactor)
    let startIndex = parseInt(event.startIndex)
    let endIndex = parseInt(event.endIndex)
    let response = {}

    if (level === 0) {
        // This next log statement is parsed by the analyzer. DO NOT CHANGE.
        console.log("Root Dispatcher");
        
        // Generate new job id
        jobId = uuidv1();

        numBatches = Math.ceil(endIndex / batchSize);
        console.log(`Partition ${endIndex} searches into ${numBatches} of size ${batchSize}`);
        if (numBatches > MAX_PARALLELISM) {
            // adjust the batch size
            batchSize = Math.ceil(endIndex / MAX_PARALLELISM)
            numBatches = Math.ceil(endIndex / batchSize);
            console.log(`Capping batch size to ${batchSize} due to max parallelism (${MAX_PARALLELISM})`);
        }
        // the branchingFactor formula assumes that each leaf node at level = <numLevels> corresponds to a batch of size <batchSize>
        // nLeafNodes = totalSearches / batchSize = branchingFactor ^ numLevels
        branchingFactor = Math.ceil(Math.pow(numBatches, 1/numLevels)); // e.g. ceil(695^(1/3)) = ceil(8.86) = 9

        // Start monitoring
        const now = new Date();
        const monitorParams = {
            jobId,
            numBatches,
            startTime: now.toISOString()
        }
        monitorName = await startMonitor(jobId, monitorParams, monitorFunctionArn, reduceFunctionArn);
        response.monitorUniqueName = monitorName       

    }

    if (monitorName) {
        // This next log statement is parsed by the analyzer. DO NOT CHANGE.
        console.log(`Monitor: ${monitorName}`);
    }

    const nextLevelManagerRange = Math.pow(branchingFactor, numLevels-level-1) * batchSize;
    console.log(`Level ${level} -> next range: ${nextLevelManagerRange}`);
    const nextEvent = {
        level: level + 1,
        numLevels: numLevels,
        libraries: libraries,
        jobId: jobId,
        jobParameters: jobParameters,
        batchSize: batchSize,
        numBatches: numBatches,
        branchingFactor: branchingFactor,
        monitorName: monitorName
    }

    if (level + 1 < numLevels) {
        // start more intermediate dispatchers
        for(let i = startIndex; i < endIndex; i += nextLevelManagerRange) {
            const workerStart = i;
            const workerEnd = i+nextLevelManagerRange > endIndex ? endIndex : i+nextLevelManagerRange;
            const invokeResponse = await invokeAsync(
                dispatchFunctionArn, {
                    startIndex: workerStart,
                    endIndex: workerEnd,
                    ...nextEvent
                });
            console.log(`Dispatched sub-dispatcher ${workerStart} - ${workerEnd} [status=${invokeResponse.Status}]`);
        }
    } else {
        // this is the parent of leaf node (each leaf node corresponds to a batch) so start the batch
        let batchId = Math.ceil(startIndex / batchSize)
        for(let i = startIndex; i < endIndex; i += batchSize) {
            const workerStart = i;
            const workerEnd = i+batchSize > endIndex ? endIndex : i+batchSize;
            // This next log statement is parsed by the analyzer. DO NOT CHANGE.
            console.log(`Dispatching Batch Id: ${batchId}`)
            const searchParams = {
                monitorName: monitorName,
                jobId: jobId,
                batchId: batchId,
                jobParameters: jobParameters,
                startIndex: workerStart,
                endIndex: workerEnd,
                ...nextEvent
            };
            const invokeResponse = await invokeAsync(workerFunctionArn, searchParams);
            console.log(`Dispatched batch #${batchId} to ${workerFunctionArn} [status=${invokeResponse.Status}]`);
            batchId++;
        }
    }

    return response;
}

const startMonitor = async (jobId, monitorParams, monitorFunctionArn, reduceFunctionArn) => {
    const timestamp = new Date().getTime();
    
    const monitorUniqueName = `Job_${jobId}_${timestamp}`;

    const stepFunction = new AWS.StepFunctions();

    const config = {
        Comment: "Monitors a parallel Color Depth Search and notifies the user upon completion",
        StartAt: "Monitor",
        States: {
            Monitor: {
                Type: "Task",
                Resource: monitorFunctionArn,
                Retry: [
                    {
                        ErrorEquals: ["Lambda.TooManyRequestsException"],
                        IntervalSeconds: 1,
                        MaxAttempts: 100
                    }
                ],
                Next: "IsTimedOut"
            },
            IsTimedOut: {
                Type: "Choice",
                Choices: [
                    {
                        Variable: "$.timedOut",
                        BooleanEquals: true,
                        Next: "Reduce"
                    }
                ],
                Default: "AreWeDoneYet"
            },
            AreWeDoneYet: {
                Type: "Choice",
                Choices: [
                    {
                        Variable: "$.completed",
                        BooleanEquals: true,
                        Next: "Reduce"
                    }
                ],
                Default: "Wait"
            },
            Wait: {
                Type: "Wait",
                Seconds: 1,
                Next: "Monitor"
            },
            Reduce: {
                Type: "Task",
                Resource: reduceFunctionArn,
                Next: "EndState"
            },
            EndState: {
                Type: "Pass",
                End: true
            }
        }
    }
    
    const params = {
        definition: JSON.stringify(config), /* required */
        name: 'bayview', /* required */
        roleArn: 'arn:aws:iam::012345678901:role/DummyRole', /* required */
    };
    stepFunction.createStateMachine(params, function (err, data) {
        if (err) {
            delete params.name;
            params.stateMachineArn = 'arn:aws:states:us-east-1:123456789012:stateMachine:bayview'
            stepFunction.updateStateMachine(params, function (err, data) {
                if (err) console.log(err, err.stack); // an error occurred
                else console.log(data);           // successful response
            });
        } // an error occurred
        else console.log(data);           // successful response
    });




    await startStepFunction(
        monitorUniqueName,
        monitorParams,
        stateMachineArn
    );
    return monitorUniqueName;
}


