// This file replaces the lambda function code and can be used to schedule the batch task.
// Requires the following environment variables:
//  - EVENT_LOGGING_ENABLED
//  - JOB_DEFINITION_ARN
//  - FUNCTION_NAME
//  - JOB_QUEUE_ARN

'use strict';
const process = require("process");
const { BatchClient, SubmitJobCommand } = require('@aws-sdk/client-batch')
const batch = new BatchClient();

function zeroPad(n) {
    return n <= 9 ? "0" + n : n;
}

function getFormattedNow() {
    let currentDate = new Date();
    return currentDate.getFullYear()
        + "-" + zeroPad(currentDate.getMonth() + 1)
        + "-" + zeroPad(currentDate.getDate())
        + "-" + zeroPad(currentDate.getHours())
        + "-" + zeroPad(currentDate.getMinutes())
        + "-" + zeroPad(currentDate.getSeconds());
}

module.exports.schedule = (event, context, callback) => {

    if (process.env.EVENT_LOGGING_ENABLED === 'true') {
        console.log(`Received event: ${JSON.stringify(event, null, 2)}`);
    }

    const jobDefinition = process.env.JOB_DEFINITION_ARN;
    const jobName = `${process.env.FUNCTION_NAME}-${getFormattedNow()}`;
    const jobQueue = process.env.JOB_QUEUE_ARN;

    console.log(`Submitting job: ${jobName} with jobDefinition: ${jobDefinition} to queue: ${jobQueue}`);

    // delete headers from the event, we are exceeding the 8092 limit for container overrides
    delete event.headers;
    delete event.multiValueHeaders;

    let params = {
        jobDefinition: jobDefinition,
        jobName: jobName,
        jobQueue: jobQueue,
        parameters: {
            event: JSON.stringify(event)
        },
        containerOverrides: {
            environment: [
                { name: "AWS_LAMBDA_FUNCTION_NAME", value: process.env.AWS_LAMBDA_FUNCTION_NAME },
                { name: "AWS_LAMBDA_FUNCTION_VERSION", value: process.env.AWS_LAMBDA_FUNCTION_VERSION },
                { name: "AWS_REQUEST_ID", value: context.awsRequestId }
            ]
        }
    };

    batch.send( new SubmitJobCommand(params), function(err, data) {
        let response;

        const jsonHeaders = {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',        // Required for CORS support to work
            'Access-Control-Allow-Credentials': true   // Required for cookies, authorization headers with HTTPS 
        }
        
        if (err) {
            console.log(`Error submitting job: ${err}`, err.stack);
            response = {
                statusCode: 500,
                headers: jsonHeaders,
                body: JSON.stringify({
                    'error': err
                })
            }
        }
        else {
            console.log(`Submitted job: ${JSON.stringify(data, null, 2)}`);
            response = {
                statusCode: 200,
                headers: jsonHeaders,
                body: JSON.stringify({
                    data
                })
            }
        }

        callback(null, response);
    });
};