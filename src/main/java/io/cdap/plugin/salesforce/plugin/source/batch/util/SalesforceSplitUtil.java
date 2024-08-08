/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.salesforce.plugin.source.batch.util;

import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.TimeoutExceededException;
import io.cdap.plugin.salesforce.BulkAPIBatchException;
import io.cdap.plugin.salesforce.InvalidConfigException;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceBulkUtil;
import io.cdap.plugin.salesforce.SalesforceQueryUtil;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceBulkRecordReader;
import io.cdap.plugin.salesforce.plugin.source.batch.SalesforceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class which provides methods to generate Salesforce splits for a query.
 */
public final class SalesforceSplitUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSplitUtil.class);

  /**
   * Generates and returns Salesforce splits for a query
   *
   * @param query          the query for the sobject
   * @param bulkConnection used to create salesforce jobs
   * @param enablePKChunk  indicates if pk chunking is enabled
   * @return list of salesforce splits
   */
  public static List<SalesforceSplit> getQuerySplits(String query, BulkConnection bulkConnection,
                                                     boolean enablePKChunk, String operation,
                                                     Long initialRetryDuration, Long maxRetryDuration,
                                                     Integer maxRetryCount, Boolean retryOnBackendError) {
    return Stream.of(getBatches(query, bulkConnection, enablePKChunk, operation, initialRetryDuration, maxRetryDuration,
                                maxRetryCount, retryOnBackendError))
      .map(batch -> new SalesforceSplit(batch.getJobId(), batch.getId(), query))
      .collect(Collectors.toList());
  }

  /**
   * Based on query length sends query to Salesforce to receive array of batch info. If query is within limit, executes
   * original query. If not, switches to wide object logic, i.e. generates Id query to retrieve batch info for Ids only
   * that will be used later to retrieve data using SOAP API.
   *
   * @param query          SOQL query
   * @param bulkConnection bulk connection
   * @param enablePKChunk  enable PK Chunking
   * @return array of batch info
   */
  private static BatchInfo[] getBatches(String query, BulkConnection bulkConnection,
                                        boolean enablePKChunk, String operation,
                                        Long initialRetryDuration, Long maxRetryDuration,
                                        Integer maxRetryCount, Boolean retryOnBackendError) {
    try {
      if (!SalesforceQueryUtil.isQueryUnderLengthLimit(query)) {
        LOG.debug("Wide object query detected. Query length '{}'", query.length());
        query = SalesforceQueryUtil.createSObjectIdQuery(query);
      }
      BatchInfo[] batches = runBulkQuery(bulkConnection, query, enablePKChunk, operation, initialRetryDuration,
                                         maxRetryDuration, maxRetryCount, retryOnBackendError);
      LOG.debug("Number of batches received from Salesforce: '{}'", batches.length);
      return batches;
    } catch (AsyncApiException | IOException | InterruptedException e) {
      throw new RuntimeException(
        String.format("Failed to run a Salesforce bulk query (%s): %s", query, e.getMessage()), e);
    }
  }

  /**
   * Start batch job of reading a given guery result.
   *
   * @param bulkConnection bulk connection instance
   * @param query          a SOQL query
   * @param enablePKChunk  enable PK Chunk
   * @return an array of batches
   * @throws AsyncApiException if there is an issue creating the job
   * @throws IOException       failed to close the query
   */
  private static BatchInfo[] runBulkQuery(BulkConnection bulkConnection, String query,
                                          boolean enablePKChunk, String operation,
                                          Long initialRetryDuration, Long maxRetryDuration,
                                          Integer maxRetryCount, Boolean retryOnBackendError)
    throws AsyncApiException, IOException, InterruptedException {
    BulkConnectionRetryWrapper bulkConnectionRetryWrapper = new BulkConnectionRetryWrapper(bulkConnection);
    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
    JobInfo job = SalesforceBulkUtil.createJob(bulkConnection, sObjectDescriptor.getName(), getOperationEnum(operation),
      null, ConcurrencyMode.Parallel, ContentType.CSV);
    final BatchInfo batchInfo;
    try {
      if (retryOnBackendError) {
        batchInfo =
          Failsafe.with(getRetryPolicy(initialRetryDuration, maxRetryDuration, maxRetryCount))
            .get(() -> createBatchFromStream(bulkConnection, query, job));
      } else {
        try (ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes())) {
          batchInfo = bulkConnection.createBatchFromStream(job, bout);
        }
      }
      if (enablePKChunk) {
        LOG.debug("PKChunking is enabled");
        return waitForBatchChunks(bulkConnection, job.getId(), batchInfo.getId());
      }
      LOG.debug("PKChunking is not enabled");
      BatchInfo[] batchInfos = bulkConnectionRetryWrapper.getBatchInfoList(job.getId()).getBatchInfo();
      LOG.info("Job id {}, status: {}", job.getId(), bulkConnectionRetryWrapper.getJobStatus(job.getId()).getState());
      if (batchInfos.length > 0) {
        LOG.info("Batch size {}, state {}", batchInfos.length, batchInfos[0].getState());
      }
      return batchInfos;
    } catch (TimeoutExceededException e) {
      throw new AsyncApiException("Exhausted retries trying to create batch from stream", AsyncExceptionCode.Timeout);
    } catch (FailsafeException e) {
      if (e.getCause() instanceof InterruptedException) {
        throw (InterruptedException) e.getCause();
      }
      if (e.getCause() instanceof AsyncApiException) {
        throw (AsyncApiException) e.getCause();
      }
      throw e;
    }
  }

  private static BatchInfo createBatchFromStream(BulkConnection bulkConnection, String query, JobInfo job) throws
    SalesforceQueryExecutionException, IOException, AsyncApiException {
    BatchInfo batchInfo = null;
    try (ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes())) {
      batchInfo = bulkConnection.createBatchFromStream(job, bout);
    } catch (AsyncApiException exception) {
      LOG.warn("The bulk query job {} failed. Job State: {}", job.getId(), job.getState());
      if (SalesforceBulkRecordReader.RETRY_ON_REASON.contains(exception.getExceptionCode())) {
        throw new SalesforceQueryExecutionException(exception);
      }
      throw exception;
    }
    return batchInfo;
  }

  /**
   * Initializes bulk connection based on given Hadoop credentials configuration.
   *
   * @return bulk connection instance
   */
  public static BulkConnection getBulkConnection(AuthenticatorCredentials authenticatorCredentials) {
    try {
      return new BulkConnection(Authenticator.createConnectorConfig(authenticatorCredentials));
    } catch (AsyncApiException e) {
      throw new RuntimeException(
        String.format("Failed to create a connection to Salesforce bulk API: %s", e.getMessage()),
        e);
    }
  }

  /**
   * When PK Chunk is enabled, wait for state of initial batch to be NotProcessed, in this case Salesforce API will
   * decide how many batches will be created
   *
   * @param bulkConnection bulk connection instance
   * @param jobId          a job id
   * @param initialBatchId a batch id
   * @return Array with Batches created by Salesforce API
   * @throws AsyncApiException if there is an issue creating the job
   */
  private static BatchInfo[] waitForBatchChunks(BulkConnection bulkConnection, String jobId, String initialBatchId)
    throws AsyncApiException {
    BatchInfo initialBatchInfo = null;
    BulkConnectionRetryWrapper bulkConnectionRetryWrapper = new BulkConnectionRetryWrapper(bulkConnection);
    for (int i = 0; i < SalesforceSourceConstants.GET_BATCH_RESULTS_TRIES; i++) {
      //check if the job is aborted
      if (bulkConnectionRetryWrapper.getJobStatus(jobId).getState() == JobStateEnum.Aborted) {
        LOG.info(String.format("Job with Id: '%s' is aborted", jobId));
        return new BatchInfo[0];
      }
      try {
        initialBatchInfo = bulkConnectionRetryWrapper.getBatchInfo(jobId, initialBatchId);
      } catch (AsyncApiException e) {
        if (i == SalesforceSourceConstants.GET_BATCH_RESULTS_TRIES - 1) {
          throw e;
        }
        LOG.warn("Failed to get info for batch {}. Will retry after some time.", initialBatchId, e);
        continue;
      }

      if (initialBatchInfo.getState() == BatchStateEnum.NotProcessed) {
        BatchInfo[] result = bulkConnectionRetryWrapper.getBatchInfoList(jobId).getBatchInfo();
        return Arrays.stream(result).filter(batchInfo -> batchInfo.getState() != BatchStateEnum.NotProcessed)
          .toArray(BatchInfo[]::new);
      } else if (initialBatchInfo.getState() == BatchStateEnum.Failed) {
        throw new BulkAPIBatchException("Batch failed", initialBatchInfo);
      } else {
        try {
          Thread.sleep(SalesforceSourceConstants.GET_BATCH_RESULTS_SLEEP_MS);
        } catch (InterruptedException e) {
          throw new RuntimeException(String.format("Job is aborted: %s", e.getMessage()), e);
        }
      }
    }
    throw new BulkAPIBatchException("Timeout waiting for batch results", initialBatchInfo);
  }

  public static void closeJobs(Set<String> jobIds, AuthenticatorCredentials authenticatorCredentials) {
    BulkConnection bulkConnection = SalesforceSplitUtil.getBulkConnection(authenticatorCredentials);
    RuntimeException runtimeException = null;
    for (String jobId : jobIds) {
      try {
        SalesforceBulkUtil.closeJob(bulkConnection, jobId);
      } catch (AsyncApiException e) {
        if (runtimeException == null) {
          runtimeException = new RuntimeException(e);
        } else {
          runtimeException.addSuppressed(e);
        }
      }
    }
    if (runtimeException != null) {
      throw runtimeException;
    }
  }

  private static OperationEnum getOperationEnum(String operation) {
    try {
      return OperationEnum.valueOf(operation);
    } catch (IllegalArgumentException ex) {
      throw new InvalidConfigException("Unsupported value for operation: " + operation,
                                       SalesforceSourceConstants.PROPERTY_OPERATION);
    }
  }

  public static RetryPolicy<Object> getRetryPolicy(Long initialRetryDuration, Long maxRetryDuration,
                                                    Integer maxRetryCount) {
    // Exponential backoff with initial retry of 5 seconds and max retry of 80 seconds.
    return RetryPolicy.builder()
      .handle(SalesforceQueryExecutionException.class)
      .withBackoff(Duration.ofSeconds(initialRetryDuration), Duration.ofSeconds(maxRetryDuration), 2)
      .withMaxRetries(maxRetryCount)
      .onRetry(event -> LOG.debug("Retrying Salesforce Bulk Query. Retry count: {}", event
        .getAttemptCount()))
      .onSuccess(event -> LOG.debug("Salesforce Bulk Query executed successfully."))
      .onRetriesExceeded(event -> LOG.error("Retry limit reached for Salesforce Bulk Query."))
      .build();
  }
}
