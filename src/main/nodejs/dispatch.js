'use strict';

const { invokeAsync, startStepFunction } = require('./utils');
const { v1: uuidv1 } = require('uuid');

const DEFAULTS = {
    level: 0,
    numLevels: 2,
    jobParameters: {},
    maxParallelism: 3000
};

const dispatchFunctionName = process.env.DISPATCH_FUNCTION_ARN
const monitorFunctionName = process.env.MONITOR_FUNCTION_ARN
const stateMachineName = process.env.STATE_MACHINE_NAME

exports.dispatchHandler = async (event) => {

    // This next log statement is parsed by the analyzer. DO NOT CHANGE.
    console.log('Input event:', JSON.stringify(event))

    // User defined parameters
    const workerFunctionName = event.workerFunctionName
    const reduceFunctionName = event.reduceFunctionName
    
    // Parameters which have defaults
    const level = parseInt(event.level) || DEFAULTS.level
    const numLevels = parseInt(event.numLevels) || DEFAULTS.numLevels
    const jobParameters = event.jobParameters || DEFAULTS.jobParameters
    const maxParallelism = event.maxParallelism || DEFAULTS.maxParallelism

    // Programmatic parameters. In the case of the root manager, these will be null initially and then generated for later invocations.
    let jobId = event.jobId
    let monitorName = event.monitorName
    let batchSize = parseInt(event.batchSize)
    let numBatches = parseInt(event.numBatches)
    let branchingFactor = parseInt(event.branchingFactor)
    let startIndex = parseInt(event.startIndex)
    let endIndex = parseInt(event.endIndex)

    if (level === 0) {
        // This next log statement is parsed by the analyzer. DO NOT CHANGE.
        console.log("Root Dispatcher");
        
        // Generate new job id
        jobId = uuidv1();

        numBatches = Math.ceil(endIndex / batchSize);
        console.log(`Partition ${endIndex} searches into ${numBatches} of size ${batchSize}`);
        if (numBatches > maxParallelism) {
            // adjust the batch size so that we don't exceed the max parallelism requested
            batchSize = Math.ceil(endIndex / maxParallelism)
            numBatches = Math.ceil(endIndex / batchSize);
            console.log(`Capping batch size to ${batchSize} due to max parallelism (${maxParallelism})`);
        }
        // the branchingFactor formula assumes that each leaf node at level = <numLevels> corresponds to a batch of size <batchSize>
        // nLeafNodes = totalSearches / batchSize = branchingFactor ^ numLevels
        branchingFactor = Math.ceil(Math.pow(numBatches, 1/numLevels)); // e.g. ceil(695^(1/3)) = ceil(8.86) = 9

        // Start monitoring
        const now = new Date();
        const monitorParams = {
            jobId,
            numBatches,
            startTime: now.toISOString(),
            monitorFunctionName, 
            reduceFunctionName
        }
        monitorName = await startMonitor(jobId, stateMachineName, monitorParams, monitorFunctionName);
    }

    // This next log statement is parsed by the analyzer. DO NOT CHANGE.
    console.log(`Monitor: ${monitorName}`);

    const nextLevelManagerRange = Math.pow(branchingFactor, numLevels-level-1) * batchSize;
    console.log(`Level ${level} -> next range: ${nextLevelManagerRange}`);
    const nextEvent = {
        level: level + 1,
        numLevels: numLevels,
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
            const params = {
                startIndex: workerStart,
                endIndex: workerEnd,
                ...nextEvent
            }
            const invokeResponse = await invokeAsync(dispatchFunctionName, params);
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
            const params = {
                monitorName: monitorName,
                jobId: jobId,
                batchId: batchId,
                jobParameters: jobParameters,
                startIndex: workerStart,
                endIndex: workerEnd,
                ...nextEvent
            };
            const invokeResponse = await invokeAsync(workerFunctionName, params);
            console.log(`Dispatched batch #${batchId} to ${workerFunctionName} [status=${invokeResponse.Status}]`);
            batchId++;
        }
    }

    return {
        monitorName,
        jobId
    };
}

const startMonitor = async (jobId, stateMachineName, monitorParams) => {
    await startStepFunction(
        jobId,
        monitorParams,
        stateMachineName
    );
    return jobId;
}


