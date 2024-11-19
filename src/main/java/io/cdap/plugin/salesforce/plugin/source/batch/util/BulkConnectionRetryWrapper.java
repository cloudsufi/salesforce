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
import com.sforce.ws.ConnectorConfig;
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
    this.retryPolicy = SalesforceSplitUtil.getRetryPolicy(5L, 80L, 5);
  }

  public JobInfo createJob(JobInfo jobInfo) {
    Object resultJobInfo = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while creating job"))
        .get(() -> {
          try {
            return bulkConnection.createJob(jobInfo);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e.getMessage());
          }
        });
    return (JobInfo) resultJobInfo;
  }

  public JobInfo getJobStatus(String jobId) {
    Object resultJobInfo = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while getting job status"))
        .get(() -> {
          try {
            return bulkConnection.getJobStatus(jobId);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e.getMessage());
          }
        });
    return (JobInfo) resultJobInfo;
  }

  public void updateJob(JobInfo jobInfo) {
    Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while updating job."))
        .get(() -> {
          try {
            return bulkConnection.updateJob(jobInfo);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e.getMessage());
          }
        });
  }

  public BatchInfoList getBatchInfoList(String jobId) throws AsyncApiException {
    Object batchInfoList = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while getting batch info list"))
        .get(() -> {
          try {
            return bulkConnection.getBatchInfoList(jobId);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e.getMessage());
          }
        });
    return (BatchInfoList) batchInfoList;
  }

  public BatchInfo getBatchInfo(String jobId, String batchId) throws AsyncApiException {
    Object batchInfo = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while getting batc status"))
        .get(() -> {
          try {
            return bulkConnection.getBatchInfo(jobId, batchId);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e.getMessage());
          }
        });
    return (BatchInfo) batchInfo;
  }

  public InputStream getBatchResultStream(String jobId, String batchId) throws AsyncApiException {
    Object inputStream = Failsafe.with(retryPolicy)
        .onFailure(event -> LOG.info("Failed while getting batch result stream"))
        .get(() -> {
          try {
            return bulkConnection.getBatchResultStream(jobId, batchId);
          } catch (AsyncApiException e) {
            throw new SalesforceQueryExecutionException(e.getMessage());
          }
        });
    return (InputStream) inputStream;
  }

  public ConnectorConfig getConfig() {
    return bulkConnection.getConfig();
  }

}
