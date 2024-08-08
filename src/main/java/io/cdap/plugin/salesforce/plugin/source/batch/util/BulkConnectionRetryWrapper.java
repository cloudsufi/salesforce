/*
 * Copyright © 2024 Cask Data, Inc.
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
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchInfoList;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;

/**
 * BulkConnectionRetryWrapper class to retry all the salesforce api calls in case of failure.
 */
public class BulkConnectionRetryWrapper {

  private final BulkConnection bulkConnection;
  private final RetryPolicy retryPolicy;
  private static final Logger LOG = LoggerFactory.getLogger(BulkConnectionRetryWrapper.class);

  public BulkConnectionRetryWrapper(BulkConnection bulkConnection) {
    this.bulkConnection = bulkConnection;
    this.retryPolicy = getRetryPolicy(5L, 80L, 5);
  }

  public JobInfo createJob(JobInfo jobInfo) {
    Object resultJobInfo = Failsafe.with(retryPolicy)
      .onFailure(event -> LOG.info("Failed while creating job"))
      .get(() -> bulkConnection.createJob(jobInfo));
    return (JobInfo) resultJobInfo;
  }

  public JobInfo getJobStatus(String jobId) {
    Object resultJobInfo = Failsafe.with(retryPolicy)
      .onFailure(event -> LOG.info("Failed while getting job status"))
      .get(() -> bulkConnection.getJobStatus(jobId));
    return (JobInfo) resultJobInfo;
  }

  public void updateJob(JobInfo jobInfo) {
    Failsafe.with(retryPolicy)
      .onFailure(event -> LOG.info("Failed while updating job."))
      .get(() -> bulkConnection.updateJob(jobInfo));
  }

  public BatchInfoList getBatchInfoList(String jobId) throws AsyncApiException {
    Object batchInfoList = Failsafe.with(retryPolicy)
      .onFailure(event -> LOG.info("Failed while getting batch info list"))
      .get(() -> bulkConnection.getBatchInfoList(jobId));
    return (BatchInfoList) batchInfoList;
  }

  public BatchInfo getBatchInfo(String jobId, String batchId) throws AsyncApiException {
    Object batchInfo = Failsafe.with(retryPolicy)
      .onFailure(event -> LOG.info("Failed while getting batc status"))
      .get(() -> bulkConnection.getBatchInfo(jobId, batchId));
    return (BatchInfo) batchInfo;
  }

  public InputStream getBatchResultStream(String jobId, String batchId) throws AsyncApiException {
    Object inputStream = Failsafe.with(retryPolicy)
      .onFailure(event -> LOG.info("Failed while getting batch result stream"))
      .get(() -> bulkConnection.getBatchResultStream(jobId, batchId));
    return (InputStream) inputStream;
  }

  public static RetryPolicy<Object> getRetryPolicy(Long initialRetryDuration, Long maxRetryDuration,
                                                   Integer maxRetryCount) {
    // Exponential backoff with initial retry of 5 seconds and max retry of 80 seconds.
    return RetryPolicy.builder()
      .handle(AsyncApiException.class)
      .withBackoff(Duration.ofSeconds(initialRetryDuration), Duration.ofSeconds(maxRetryDuration), 2)
      .withMaxRetries(maxRetryCount)
      .onRetry(event -> LOG.info("Retrying Salesforce Bulk Query. Retry count: {}", event
        .getAttemptCount()))
      .onSuccess(event -> LOG.debug("Salesforce api call executed successfully."))
      .onRetriesExceeded(event -> LOG.error("Retry limit reached for Salesforce Bulk Query."))
      .build();
  }

}
