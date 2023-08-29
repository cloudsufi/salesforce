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

import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.RDD$;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;

import java.util.Collections;

/**
 * SalesforceRDD that returns PubSubMessage for each partition.
 * Partition count is same as the number of readers.
 */
public class SalesforceRDD extends RDD {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceRDD.class);

  private final Time batchTime;
  private final long readDuration;
  private final SalesforceStreamingSourceConfig config;
  private final AuthenticatorCredentials credentials;

  SalesforceRDD(SparkContext sparkContext, Time batchTime, long readDuration, SalesforceStreamingSourceConfig config,
                AuthenticatorCredentials credentials) {
    super(sparkContext, scala.collection.JavaConverters.asScalaBuffer(Collections.emptyList()),
          scala.reflect.ClassTag$.MODULE$.apply(String.class));
    this.batchTime = batchTime;
    this.readDuration = readDuration;
    this.config = config;
    this.credentials = credentials;
  }

  @Override
  public Iterator<String> compute(Partition split, TaskContext context) {
    LOG.info("Computing for partition {} .", split.index());
    return new SalesforceRDDIterator(config, context, batchTime, readDuration, credentials);
  }

  @Override
  public Partition[] getPartitions() {
    //int partitionCount = config.getNumberOfReaders();
    //int partitionCount = 10;
    int partitionCount = 1;
    Partition[] partitions = new Partition[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      final int index = i;
      partitions[i] = () -> index;
    }
    return partitions;
  }
}
