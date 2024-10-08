// Handles setting up our ECR repository so that we can push our docker image to it

'use strict';

const BbPromise = require('bluebird');
const path = require('path');
const _ = require('lodash');
var util = require('util');

const lifeCyclePolicies = {
    "rules": [
        {
            "rulePriority": 1,
            "description": "Remove images over 90 days old",
            "selection": {
                "tagStatus": "any",
                "countType": "sinceImagePushed",
                "countUnit": "days",
                "countNumber": 90
            },
            "action": {
                "type": "expire"
            }
        }
    ]
};

/**
 * Adds the ECR repository to the "create" template so that we have a repository we can upload our docker image to
 * during deployment.
 */
function generateCoreTemplate() {
    // Setup our ECR Repository to delete untagged images after 1 day
    // const ecrTemplate = `
    //   {
    //     "Type" : "AWS::ECR::Repository",
    //     "Properties" : {
    //       "LifecyclePolicy" : {
    //         "LifecyclePolicyText" : ${JSON.stringify(JSON.stringify(lifeCyclePolicies))}
    //       },
    //       "RepositoryName" : "${this.provider.naming.getECRRepositoryName()}"
    //     }
    //   }
    // `;
    const ecrTemplate = `
      {
        "Type" : "AWS::ECR::Repository",
        "Properties" : {
          "RepositoryName" : "${this.provider.naming.getECRRepositoryName()}"
        }
      }
    `;

    const newECRObject = {
        [this.provider.naming.getECRLogicalId()]: JSON.parse(ecrTemplate)
    };

    // Add it to our initial compiled cloud formation templates
    _.merge(
        this.serverless.service.provider.compiledCloudFormationTemplate.Resources,
        newECRObject
    );

    // Look for the serverless-log-forwarding custom parameters, if found, then forward the logs
    // from our batch jobs to the same ARN...
    const logGroupName = _.get(this.serverless.service,"custom.awsBatch.logGroupName");
    const logDestinationArn = _.get(this.serverless.service,"custom.logForwarding.destinationARN" );
    if ( logDestinationArn && logGroupName ) {
        const batchLogGroupTemplate = {
            "BatchLogGroupLambdaPermission": {
                "Type": "AWS::Lambda::Permission",
                "Properties": {
                    "FunctionName": logDestinationArn,
                    "Action": "lambda:InvokeFunction",
                    "Principal": "logs.us-east-1.amazonaws.com"
                }
            },
            "BatchLogGroup": {
                "Type": "AWS::Logs::LogGroup",
                "Properties": {
                    "LogGroupName": logGroupName,
                    "RetentionInDays": 7
                }
            },
            "BatchLogGroupSubscriptionFilter": {
                "Type": "AWS::Logs::SubscriptionFilter",
                "Properties": {
                    "DestinationArn": logDestinationArn,
                    "FilterPattern": "",
                    "LogGroupName": logGroupName
                },
                "DependsOn": [
                    "BatchLogGroupLambdaPermission",
                    "BatchLogGroup"
                ]
            }
        }
    
        _.merge(
            this.serverless.service.provider.compiledCloudFormationTemplate.Resources,
            batchLogGroupTemplate
        );
    }

    // Then write it back out to the file
    const coreTemplateFileName = this.provider.naming.getCoreTemplateFileName();

    const coreTemplateFilePath = path.join(this.serverless.config.servicePath,
        '.serverless',
        coreTemplateFileName);

    this.serverless.utils.writeFileSync(coreTemplateFilePath,
        this.serverless.service.provider.compiledCloudFormationTemplate);

    this.serverless.service.provider.coreCloudFormationTemplate =
        _.cloneDeep(this.serverless.service.provider.compiledCloudFormationTemplate);

    return BbPromise.resolve();
}

module.exports = { generateCoreTemplate };