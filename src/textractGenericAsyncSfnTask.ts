import * as path from 'path';
import { Duration, Aws, ArnFormat, Stack, RemovalPolicy } from 'aws-cdk-lib';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import { IRole, ManagedPolicy } from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { FilterPattern, MetricFilter } from 'aws-cdk-lib/aws-logs';
import * as sns from 'aws-cdk-lib/aws-sns';
import { LambdaSubscription } from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';

export function validatePatternSupported(
  integrationPattern: sfn.IntegrationPattern,
  supportedPatterns: sfn.IntegrationPattern[],
) {
  if (!supportedPatterns.includes(integrationPattern)) {
    throw new Error(
      `Unsupported service integration pattern. Supported Patterns: ${supportedPatterns}. Received: ${integrationPattern}`,
    );
  }
}

const resourceArnSuffix: Record<sfn.IntegrationPattern, string> = {
  [sfn.IntegrationPattern.REQUEST_RESPONSE]: '',
  [sfn.IntegrationPattern.RUN_JOB]: '.sync',
  [sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN]: '.waitForTaskToken',
};

export function integrationResourceArn(
  service: string,
  api: string,
  integrationPattern?: sfn.IntegrationPattern,
): string {
  if (!service || !api) {
    throw new Error("Both 'service' and 'api' must be provided to build the resource ARN.");
  }
  return `arn:${Aws.PARTITION}:states:::${service}:${api}` +
    (integrationPattern ? resourceArnSuffix[integrationPattern] : '');
}

export interface TextractGenericAsyncSfnTaskProps extends sfn.TaskStateBaseProps {
  readonly s3OutputBucket: string;
  readonly s3TempOutputPrefix: string;
  readonly s3InputBucket?: string;
  readonly s3InputPrefix?: string;
  readonly textractAPI?: 'GENERIC' | 'EXPENSE' | 'LENDING';
  readonly textractAsyncCallMaxRetries?: number;
  readonly textractAsyncCallBackoffRate?: number;
  readonly lambdaLogLevel?: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR' | 'FATAL';
  readonly lambdaTimeout?: number;
  readonly lambdaMemory?: number;
  readonly textractAsyncCallInterval?: number;
  readonly textractStateMachineTimeoutMinutes?: number;
  readonly inputPolicyStatements?: iam.PolicyStatement[];
  readonly outputPolicyStatements?: iam.PolicyStatement[];
  readonly enableCloudWatchMetricsAndDashboard?: boolean;
  readonly taskTokenTable?: dynamodb.ITable;
  readonly snsRoleTextract?: iam.IRole;
  readonly input?: sfn.TaskInput;
  readonly name?: string;
  readonly associateWithParent?: boolean;
}

export class TextractGenericAsyncSfnTask extends sfn.TaskStateBase {
  private static readonly SUPPORTED_INTEGRATION_PATTERNS = [
    sfn.IntegrationPattern.REQUEST_RESPONSE,
    sfn.IntegrationPattern.RUN_JOB,
    sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
  ];

  protected readonly taskMetrics?: sfn.TaskMetricsConfig;
  protected readonly taskPolicies?: iam.PolicyStatement[];

  private readonly integrationPattern: sfn.IntegrationPattern;
  public stateMachine: sfn.IStateMachine;
  public taskTokenTable: dynamodb.ITable;
  public taskTokenTableName: string;
  public textractAsyncSNSRole: IRole;
  public textractAsyncSNS: sns.ITopic;
  public textractAsyncCallFunction: lambda.IFunction;
  public textractAsyncReceiveSNSFunction: lambda.IFunction;
  public asyncDurationMetric?: cloudwatch.IMetric;
  public asyncJobFailureMetric?: cloudwatch.IMetric;
  public asyncNumberPagesMetric?: cloudwatch.IMetric;
  public asyncJobFinshedMetric?: cloudwatch.IMetric;
  public asyncJobStartedMetric?: cloudwatch.IMetric;
  public asyncNumberPagesSendMetric?: cloudwatch.IMetric;

  constructor(scope: Construct, id: string, private readonly props: TextractGenericAsyncSfnTaskProps) {
    super(scope, id, props);

    this.integrationPattern = props.integrationPattern || sfn.IntegrationPattern.REQUEST_RESPONSE;
    validatePatternSupported(
      this.integrationPattern,
      TextractGenericAsyncSfnTask.SUPPORTED_INTEGRATION_PATTERNS,
    );

    if (
      this.integrationPattern === sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN &&
      !sfn.FieldUtils.containsTaskToken(props.input)
    ) {
      throw new Error('Task Token is required in `input` for callback. Use JsonPath.taskToken to set the token.');
    }

    if (this.props.associateWithParent && props.input && props.input.type !== sfn.InputType.OBJECT) {
      throw new Error(
        'Could not enable `associateWithParent` because `input` is taken directly from a JSON path. Use `sfn.TaskInput.fromObject` instead.',
      );
    }

    const textractStateMachineTimeoutMinutes =
      props.textractStateMachineTimeoutMinutes === undefined ? 2880 : props.textractStateMachineTimeoutMinutes;
    const lambdaLogLevel = props.lambdaLogLevel === undefined ? 'INFO' : props.lambdaLogLevel;
    const textractAPI = props.textractAPI === undefined ? 'GENERIC' : props.textractAPI;
    const textractAsyncCallMaxRetries = props.textractAsyncCallMaxRetries === undefined ? 100 : props.textractAsyncCallMaxRetries;
    const textractAsyncCallBackoffRate = props.textractAsyncCallBackoffRate === undefined ? 1.1 : props.textractAsyncCallBackoffRate;
    const lambdaTimeout = props.lambdaTimeout === undefined ? 300 : props.lambdaTimeout;
    const lambdaMemory = props.lambdaMemory === undefined ? 512 : props.lambdaMemory;
    const textractAsyncCallInterval = props.textractAsyncCallInterval === undefined ? 1 : props.textractAsyncCallInterval;
    const s3TempOutputPrefix = props.s3TempOutputPrefix === undefined ? '' : props.s3TempOutputPrefix;
    const s3InputPrefix = props.s3InputPrefix === undefined ? '' : props.s3InputPrefix;
    const enableMetrics = props.enableCloudWatchMetricsAndDashboard === undefined ? false : props.enableCloudWatchMetricsAndDashboard;

    if (props.taskTokenTable === undefined) {
      this.taskTokenTable = new dynamodb.Table(this, 'TextractTaskTokenTable', {
        partitionKey: { name: 'ID', type: dynamodb.AttributeType.STRING },
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        removalPolicy: RemovalPolicy.DESTROY,
        timeToLiveAttribute: 'ttltimestamp',
      });
    } else {
      this.taskTokenTable = props.taskTokenTable;
    }
    this.taskTokenTableName = this.taskTokenTable.tableName;

    if (props.snsRoleTextract === undefined) {
      this.textractAsyncSNSRole = new iam.Role(this, 'TextractAsyncSNSRole', {
        assumedBy: new iam.ServicePrincipal('textract.amazonaws.com'),
        managedPolicies: [
          ManagedPolicy.fromAwsManagedPolicyName('AmazonSQSFullAccess'),
          ManagedPolicy.fromAwsManagedPolicyName('AmazonSNSFullAccess'),
          ManagedPolicy.fromAwsManagedPolicyName('AmazonS3ReadOnlyAccess'),
          ManagedPolicy.fromAwsManagedPolicyName('AmazonTextractFullAccess'),
        ],
      });
    } else {
      this.textractAsyncSNSRole = props.snsRoleTextract;
    }

    this.textractAsyncSNS = new sns.Topic(this, 'TextractAsyncSNS');
    this.textractAsyncCallFunction = new lambda.DockerImageFunction(this, 'TextractAsyncCall', {
      code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, '../lambda/textract_async/')),
      memorySize: lambdaMemory,
      timeout: Duration.seconds(lambdaTimeout),
      architecture: lambda.Architecture.X86_64,
      environment: {
        NOTIFICATION_SNS: this.textractAsyncSNS.topicArn,
        NOTIFICATION_ROLE_ARN: this.textractAsyncSNSRole.roleArn,
        TOKEN_STORE_DDB: this.taskTokenTableName,
        S3_OUTPUT_BUCKET: props.s3OutputBucket,
        S3_TEMP_OUTPUT_PREFIX: props.s3TempOutputPrefix,
        LOG_LEVEL: lambdaLogLevel,
        TEXTRACT_API: textractAPI,
      },
    });
    const textractAsyncCallTask = new tasks.LambdaInvoke(this, 'TextractAsyncCallTask', {
      lambdaFunction: this.textractAsyncCallFunction,
    });
    textractAsyncCallTask.addRetry({
      maxAttempts: textractAsyncCallMaxRetries,
      backoffRate: textractAsyncCallBackoffRate,
      interval: Duration.seconds(textractAsyncCallInterval),
      errors: [
        'ThrottlingException',
        'LimitExceededException',
        'InternalServerError',
        'ProvisionedThroughputExceededException',
        'Lambda.TooManyRequestsException',
        'ConnectionClosedException',
        'Lambda.Unknown',
      ],
    });

    this.textractAsyncCallFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['textract:Start*', 'textract:Get*'],
        resources: ['*'],
      }),
    );

    if (props.inputPolicyStatements === undefined) {
      if (props.s3InputBucket === undefined) {
        this.textractAsyncCallFunction.addToRolePolicy(
          new iam.PolicyStatement({ actions: ['s3:GetObject', 's3:ListBucket'], resources: ['*'] }),
        );
      } else {
        this.textractAsyncCallFunction.addToRolePolicy(
          new iam.PolicyStatement({
            actions: ['s3:GetObject'],
            resources: [
              path.join(`arn:aws:s3:::${props.s3InputBucket}`, '/*'),
              path.join(`arn:aws:s3:::${props.s3InputBucket}`, s3InputPrefix, '/*'),
            ],
          }),
        );
        this.textractAsyncCallFunction.addToRolePolicy(
          new iam.PolicyStatement({ actions: ['s3:ListBucket'], resources: [path.join(`arn:aws:s3:::${props.s3InputBucket}`)] }),
        );
      }
    } else {
      for (const policyStatement of props.inputPolicyStatements) {
        this.textractAsyncCallFunction.addToRolePolicy(policyStatement);
      }
    }

    if (props.outputPolicyStatements === undefined) {
      if (props.s3OutputBucket === undefined) {
        this.textractAsyncCallFunction.addToRolePolicy(
          new iam.PolicyStatement({ actions: ['s3:PutObject'], resources: ['*'] }),
        );
      } else {
        this.textractAsyncCallFunction.addToRolePolicy(
          new iam.PolicyStatement({
            actions: ['s3:PutObject'],
            resources: [
              path.join(`arn:aws:s3:::${props.s3OutputBucket}`, s3TempOutputPrefix, '/'),
              path.join(`arn:aws:s3:::${props.s3OutputBucket}`, s3TempOutputPrefix, '/*'),
            ],
          }),
        );
      }
    } else {
      for (const policyStatement of props.outputPolicyStatements) {
        this.textractAsyncCallFunction.addToRolePolicy(policyStatement);
      }
    }

    this.textractAsyncCallFunction.addToRolePolicy(
      new iam.PolicyStatement({ actions: ['dynamodb:PutItem', 'dynamodb:GetItem'], resources: [this.taskTokenTable.tableArn] }),
    );

    this.textractAsyncReceiveSNSFunction = new lambda.DockerImageFunction(this, 'TextractAsyncSNSFunction', {
      code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, '../lambda/textract_async_sns_listener/')),
      memorySize: 128,
      architecture: lambda.Architecture.X86_64,
      timeout: Duration.seconds(900),
      environment: {
        TOKEN_STORE_DDB: this.taskTokenTableName,
        S3_OUTPUT_BUCKET: props.s3OutputBucket,
        S3_TEMP_OUTPUT_PREFIX: props.s3TempOutputPrefix,
        TEXTRACT_API: textractAPI,
        LOG_LEVEL: lambdaLogLevel,
      },
    });
    this.textractAsyncSNS.addSubscription(new LambdaSubscription(this.textractAsyncReceiveSNSFunction));
    this.textractAsyncReceiveSNSFunction.addToRolePolicy(
      new iam.PolicyStatement({ actions: ['dynamodb:GetItem'], resources: [this.taskTokenTable.tableArn] }),
    );

    const workflow_chain = sfn.Chain.start(textractAsyncCallTask);

    this.stateMachine = new sfn.StateMachine(this, 'StateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(workflow_chain),
      timeout: Duration.hours(textractStateMachineTimeoutMinutes),
    });

    this.textractAsyncReceiveSNSFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['states:SendTaskSuccess', 'states:SendTaskFailure'],
        resources: ['*'],
      }),
    );
    this.textractAsyncCallFunction.addToRolePolicy(
      new iam.PolicyStatement({ actions: ['states:SendTaskFailure'], resources: ['*'] }),
    );

    if (enableMetrics) {
      const appName = this.node.tryGetContext('appName');
      const customMetricNamespace = 'TextractConstructGenericAsync';

      const asyncDurationMetricFilter = new MetricFilter(this, `${appName}-DurationFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncReceiveSNSFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'Duration',
        filterPattern: FilterPattern.spaceDelimited('INFO', 'timestamp', 'id', 'message', 'durationMs').whereString(
          'message',
          '=',
          `textract_async_${textractAPI}_job_duration_in_ms:`,
        ),
        metricValue: '$durationMs',
      });
      this.asyncDurationMetric = asyncDurationMetricFilter.metric({ statistic: 'avg' });

      const asyncJobFailureMetricFilter = new MetricFilter(this, `${appName}-JobFailureFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncReceiveSNSFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'JobFailure',
        filterPattern: FilterPattern.spaceDelimited('INFO', 'timestamp', 'id', 'message', 'durationMs').whereString(
          'message',
          '=',
          `textract_async_${textractAPI}_failed_job`,
        ),
        metricValue: '1',
      });
      this.asyncJobFailureMetric = asyncJobFailureMetricFilter.metric({ statistic: 'sum' });
      const asyncNumberPagesMetricFilter = new MetricFilter(this, `${appName}-NumberPagesFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncReceiveSNSFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'NumberPagesProcessed',
        filterPattern: FilterPattern.spaceDelimited('INFO', 'timestamp', 'id', 'message', 'pages').whereString(
          'message',
          '=',
          `textract_async_${textractAPI}_number_of_pages_processed:`,
        ),
        metricValue: '$pages',
      });
      this.asyncNumberPagesMetric = asyncNumberPagesMetricFilter.metric({ statistic: 'sum' });

      const asyncJobFinshedMetricFilter = new MetricFilter(this, `${appName}-JobsFinishedFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncReceiveSNSFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'JobsFinished',
        filterPattern: FilterPattern.spaceDelimited('INFO', 'timestamp', 'id', 'message', 'pages').whereString(
          'message',
          '=',
          `textract_async_${textractAPI}_number_of_pages_processed:`,
        ),
        metricValue: '1',
      });
      this.asyncJobFinshedMetric = asyncJobFinshedMetricFilter.metric({ statistic: 'sum' });

      const asyncJobStartedMetricFilter = new MetricFilter(this, `${appName}-JobsStartedFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncCallFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'JobsStarted',
        filterPattern: FilterPattern.spaceDelimited('INFO', 'timestamp', 'id', 'message').whereString(
          'message',
          '=',
          `textract_async_${textractAPI}_job_started`,
        ),
        metricValue: '1',
      });
      this.asyncJobStartedMetric = asyncJobStartedMetricFilter.metric({ statistic: 'sum' });

      const asyncNumberPagesSendMetricFilter = new MetricFilter(this, `${appName}-NumberPagesSendFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncCallFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'NumberPagesSend',
        filterPattern: FilterPattern.spaceDelimited('INFO', 'timestamp', 'id', 'message', 'pages').whereString(
          'message',
          '=',
          `textract_async_${textractAPI}_number_of_pages_send_to_process:`,
        ),
        metricValue: '$pages',
      });
      this.asyncNumberPagesSendMetric = asyncNumberPagesSendMetricFilter.metric({ statistic: 'sum' });

      const pagesPerSecond = new cloudwatch.MathExpression({
        expression: 'pages / (duration / 1000)',
        usingMetrics: {
          pages: this.asyncNumberPagesMetric!,
          duration: this.asyncDurationMetric!,
        },
      });

      const errorFilterMetric = new MetricFilter(this, `${appName}-ErrorFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncCallFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'Errors',
        filterPattern: FilterPattern.anyTerm('ERROR', 'Error', 'error'),
        metricValue: '1',
      });

      const limitExceededExceptionFilterMetric = new MetricFilter(this, `${appName}-limitExceededExceptionFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncCallFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'LimitExceededException',
        filterPattern: FilterPattern.anyTerm('textract.exceptions.LimitExceededException'),
        metricValue: '1',
      });
      const throttlingException = new MetricFilter(this, `${appName}-throttlingExceptionFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncCallFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'ThrottlingException',
        filterPattern: FilterPattern.anyTerm('textract.exceptions.ThrottlingException'),
        metricValue: '1',
      });
      const provisionedThroughputExceededException = new MetricFilter(this, `${appName}-provisionedThroughputFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncCallFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'ProvisionedThroughputExceededException',
        filterPattern: FilterPattern.anyTerm('textract.exceptions.ProvisionedThroughputExceededException'),
        metricValue: '1',
      });
      const internalServerError = new MetricFilter(this, `${appName}-internalServerErrorFilter`, {
        logGroup: (<lambda.Function>this.textractAsyncCallFunction).logGroup,
        metricNamespace: customMetricNamespace,
        metricName: 'InternalServerError',
        filterPattern: FilterPattern.anyTerm('textract.exceptions.InternalServerError'),
        metricValue: '1',
      });

      const errorMetric: cloudwatch.IMetric = errorFilterMetric.metric({ statistic: 'sum' });
      const limitExceededMetric: cloudwatch.IMetric = limitExceededExceptionFilterMetric.metric({ statistic: 'sum' });
      const throttlingMetric: cloudwatch.IMetric = throttlingException.metric({ statistic: 'sum' });
      const provisionedThroughputMetric: cloudwatch.IMetric = provisionedThroughputExceededException.metric({
        statistic: 'sum',
      });
      const internalServerErrorMetric: cloudwatch.IMetric = internalServerError.metric({ statistic: 'sum' });

      const textractStartDocumentTextThrottle: cloudwatch.IMetric = new cloudwatch.Metric({
        namespace: 'AWS/Textract',
        metricName: 'ThrottledCount',
        dimensionsMap: {
          Operation: 'StartDocumentTextDetection',
        },
        statistic: 'sum',
      });

      const dashboardWidth = 24;
      new cloudwatch.Dashboard(this, `${appName}-TestDashboard`, {
        end: 'end',
        periodOverride: cloudwatch.PeriodOverride.AUTO,
        start: 'start',
        widgets: [
          [new cloudwatch.Column(new cloudwatch.TextWidget({ markdown: '# Operational Data Row widgets', width: dashboardWidth }))],
          [
            new cloudwatch.Column(
              new cloudwatch.GraphWidget({ title: 'PagesPerSecond', left: [pagesPerSecond], width: Math.floor(dashboardWidth / 2) }),
            ),
            new cloudwatch.Column(
              new cloudwatch.GraphWidget({
                title: 'JobsStartedAndFinished',
                left: [this.asyncJobFinshedMetric!, this.asyncJobFailureMetric!, this.asyncJobStartedMetric!],
                width: Math.floor(dashboardWidth / 2),
              }),
            ),
          ],
          [
            new cloudwatch.Column(
              new cloudwatch.GraphWidget({ title: 'Duration', left: [this.asyncDurationMetric!], width: Math.floor(dashboardWidth / 3) }),
            ),
            new cloudwatch.Column(
              new cloudwatch.GraphWidget({ title: 'NumberPagesProcessed', left: [this.asyncNumberPagesMetric!], width: Math.floor(dashboardWidth / 3) }),
            ),
            new cloudwatch.Column(
              new cloudwatch.GraphWidget({ title: 'NumberPagesSendToProcess', left: [this.asyncNumberPagesSendMetric!], width: Math.floor(dashboardWidth / 3) }),
            ),
          ],
          [new cloudwatch.Column(new cloudwatch.TextWidget({ markdown: '# Async Textract Exceptions Row', width: dashboardWidth }))],
          [
            new cloudwatch.GraphWidget({ title: 'Errors', left: [errorMetric], width: Math.floor(dashboardWidth / 5) }),
            new cloudwatch.GraphWidget({ title: 'LimitExceeded', left: [limitExceededMetric], width: Math.floor(dashboardWidth / 5) }),
            new cloudwatch.GraphWidget({ title: 'Throttling', left: [throttlingMetric], width: Math.floor(dashboardWidth / 5) }),
            new cloudwatch.GraphWidget({
              title: 'ProvisionedThrougput',
              left: [provisionedThroughputMetric],
              width: Math.floor(dashboardWidth / 5),
            }),
            new cloudwatch.GraphWidget({
              title: 'InternalServerError',
              left: [internalServerErrorMetric],
              width: Math.floor(dashboardWidth / 5),
            }),
          ],
          [new cloudwatch.TextWidget({ markdown: '# Textract', width: dashboardWidth })],
          [new cloudwatch.GraphWidget({ title: 'Textract-StartDetectText-ThrottledCount', left: [textractStartDocumentTextThrottle] })],
          [new cloudwatch.TextWidget({ markdown: '# textractAsyncCallFunction', width: dashboardWidth })],
          [
            new cloudwatch.GraphWidget({ title: 'Async-Function-Errors', left: [this.textractAsyncCallFunction.metricErrors()], width: Math.floor(dashboardWidth / 3) }),
            new cloudwatch.GraphWidget({ title: 'Async-Function-Invocations', left: [this.textractAsyncCallFunction.metricInvocations()], width: Math.floor(dashboardWidth / 3) }),
            new cloudwatch.GraphWidget({ title: 'Async-Function-Throttles', left: [this.textractAsyncCallFunction.metricThrottles()], width: Math.floor(dashboardWidth / 3) }),
          ],
          [new cloudwatch.TextWidget({ markdown: '# textractAsyncReceiveSNSFunction', width: dashboardWidth })],
          [
            new cloudwatch.GraphWidget({ title: 'SNS-Function-Errors', left: [this.textractAsyncReceiveSNSFunction.metricErrors()], width: Math.floor(dashboardWidth / 3) }),
            new cloudwatch.GraphWidget({ title: 'SNS-Function-Invocations', left: [this.textractAsyncReceiveSNSFunction.metricInvocations()], width: Math.floor(dashboardWidth / 3) }),
            new cloudwatch.GraphWidget({ title: 'SNS-Function-Throttles', left: [this.textractAsyncReceiveSNSFunction.metricThrottles()], width: Math.floor(dashboardWidth / 3) }),
          ],
        ],
      });
    }
    this.taskPolicies = this.createScopedAccessPolicy();
  }

  protected _renderTask(): any {
    const suffix = this.integrationPattern === sfn.IntegrationPattern.RUN_JOB ? ':2' : '';
    let input: any;
    if (this.props.associateWithParent) {
      const associateWithParentEntry = {
        AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID: sfn.JsonPath.stringAt('$$.Execution.Id'),
      };
      input = this.props.input ? { ...this.props.input.value, ...associateWithParentEntry } : associateWithParentEntry;
    } else {
      input = this.props.input ? this.props.input.value : sfn.TaskInput.fromJsonPathAt('$').value;
    }

    return {
      Resource: `${integrationResourceArn('states', 'startExecution', this.integrationPattern)}${suffix}`,
      Parameters: sfn.FieldUtils.renderObject({
        Input: input,
        StateMachineArn: this.stateMachine.stateMachineArn,
        Name: this.props.name,
      }),
    };
  }

  private createScopedAccessPolicy(): iam.PolicyStatement[] {
    const stack = Stack.of(this);

    const policyStatements = [
      new iam.PolicyStatement({ actions: ['states:StartExecution'], resources: [this.stateMachine.stateMachineArn] }),
    ];

    if (this.integrationPattern === sfn.IntegrationPattern.RUN_JOB) {
      policyStatements.push(
        new iam.PolicyStatement({
          actions: ['states:DescribeExecution', 'states:StopExecution'],
          resources: [
            stack.formatArn({
              service: 'states',
              resource: 'execution',
              arnFormat: ArnFormat.COLON_RESOURCE_NAME,
              resourceName: `${stack.splitArn(this.stateMachine.stateMachineArn, ArnFormat.COLON_RESOURCE_NAME).resourceName}*`,
            }),
          ],
        }),
      );

      policyStatements.push(
        new iam.PolicyStatement({
          actions: ['events:PutTargets', 'events:PutRule', 'events:DescribeRule'],
          resources: [
            stack.formatArn({
              service: 'events',
              resource: 'rule',
              resourceName: 'StepFunctionsGetEventsForStepFunctionsExecutionRule',
            }),
          ],
        }),
      );
    }

    return policyStatements;
  }
}
