/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.plugin.source.streaming;

import io.cdap.cdap.etl.api.streaming.StreamingEventHandler;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * SalesforceDirectDStream implementation.
 * A snapshot is taken and saved before each batch and deleted on successful completion.
 * On retrying a batch, snapshot is restored.
 *
 * @param <T> Type of object returned by RDD
 */
public class SalesforceDirectDStream<T> extends InputDStream implements StreamingEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceDirectDStream.class);
  private static final String CDAP_PIPELINE = "cdap_pipeline";

  private final AuthenticatorCredentials credentials;
  private final SalesforceStreamingSourceConfig config;
  private final io.cdap.cdap.etl.api.streaming.StreamingContext context;
  private final StreamingContext streamingContext;

  public SalesforceDirectDStream(io.cdap.cdap.etl.api.streaming.StreamingContext context,
                                 SalesforceStreamingSourceConfig config,
                                 AuthenticatorCredentials credentials) {
    super(context.getSparkStreamingContext().ssc(), scala.reflect.ClassTag$.MODULE$.apply(String.class));
    this.streamingContext = context.getSparkStreamingContext().ssc();
    this.config = config;
    this.context = context;
    this.credentials = credentials;
  }

  @Override
  public Option<RDD<T>> compute(Time validTime) {
    LOG.debug("Computing RDD for time {}.", validTime);
    SalesforceRDD salesforceRDD = new SalesforceRDD(streamingContext.sparkContext(), validTime, config, credentials);
    RDD<T> mapped = salesforceRDD.map(new SalesforceStructuredRecordConverter(config),
                                      scala.reflect.ClassTag$.MODULE$.apply(String.class));
    return Option.apply(mapped);
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void onBatchCompleted(io.cdap.cdap.etl.api.streaming.StreamingContext streamingContext) {
    LOG.debug("Batch completed called.");
  }

  @Override
  public boolean initializeLogIfNecessary(boolean isInterpreter, boolean silent) {
    return super.initializeLogIfNecessary(isInterpreter, silent);
  }

  @Override
  public void initializeForcefully(boolean isInterpreter, boolean silent) {
    super.initializeForcefully(isInterpreter, silent);
  }
}
