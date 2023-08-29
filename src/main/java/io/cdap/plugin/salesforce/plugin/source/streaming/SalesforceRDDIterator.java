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

import com.google.common.base.Strings;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Time;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.cometd.common.JSONContext;
import org.cometd.common.JacksonJSONContextClient;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Iterator for Salesforce RDD.
 * Fetches a fixed amount of messages at a time and acknowledges them.
 * Finishes when the acknowledged messages are completed and the batch time limit is met.
 * Returns immediately if there are no more messages to read.
 */
public class SalesforceRDDIterator implements Iterator<String> {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceRDDIterator.class);
  private static final int MAX_MESSAGES = 1000;
  private static final String DEFAULT_PUSH_ENDPOINT = "/cometd/" + SalesforceConstants.API_VERSION;
  /**
   * Timeout of 110 seconds is enforced by Salesforce Streaming API and is not configurable.
   * So we enforce the same on client.
   */
  private static final long CONNECTION_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(110);
  private static final long HANDSHAKE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(110);

  private static final int HANDSHAKE_CHECK_INTERVAL_MS = 1000;

  private final long startTime;
  private final SalesforceStreamingSourceConfig config;
  private final TaskContext context;
  private final long batchDuration;
  private final AuthenticatorCredentials credentials;
  private final Queue<String> receivedMessages;
  // store message string not JSONObject, since it's not serializable for later Spark usage
  private final BlockingQueue<String> messagesQueue = new LinkedBlockingQueue<>();
  private long messageCount;
  private BayeuxClient bayeuxClient;
  private JSONContext.Client jsonContext;
  private ConcurrentMap<String, Integer> replay = new ConcurrentHashMap<>();

  public SalesforceRDDIterator(SalesforceStreamingSourceConfig config, TaskContext context, Time batchTime,
                               long batchDuration, AuthenticatorCredentials credentials) {
    this.config = config;
    this.context = context;
    this.credentials = credentials;
    this.batchDuration = batchDuration;
    this.startTime = batchTime.milliseconds();
    receivedMessages = new ConcurrentLinkedDeque<>();
    // Fetch previous messages
    replay.put("/topic/" + config.getPushTopicName(), -1);
    fetch();

  }

  @Override
  public boolean hasNext() {
    LOG.info("In hasNext()");
    try {
      LOG.info("Hostname: {}", InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException e) {
      LOG.error("Unknown Host", e);
    }
    // Complete processing of messages before finishing the batch.
    if (!receivedMessages.isEmpty()) {
      return true;
    }

    try {
      //If there are no messages to process, continue.
      if (receivedMessages.isEmpty()) {
        LOG.info("No more messages.");
        return false;
      }
      return true;
    } catch (Exception e) {
      throw new RuntimeException("Error reading messages from Salesforce. ", e);
    }
  }

  @Override
  public String next() {
    LOG.info("In next()");
    if (receivedMessages.isEmpty()) {
      // This should not happen, if hasNext() returns true, then a message should be available in queue.
      throw new IllegalStateException("Unexpected state. No messages available.");
    }

    String currentMessage = receivedMessages.poll();
    LOG.info("Current Message: {}", currentMessage);
    messageCount += 1;
    LOG.info("Message Count: {}", messageCount);
    return currentMessage;
  }

  private BayeuxClient getClient(AuthenticatorCredentials credentials) throws Exception {
    OAuthInfo oAuthInfo = Authenticator.getOAuthInfo(credentials);

    SslContextFactory sslContextFactory = new SslContextFactory();

    // Set up a Jetty HTTP client to use with CometD
    HttpClient httpClient = new HttpClient(sslContextFactory);
    httpClient.setConnectTimeout(CONNECTION_TIMEOUT_MS);
    if (!Strings.isNullOrEmpty(credentials.getProxyUrl())) {
      Authenticator.setProxy(credentials, httpClient);
    }

    httpClient.start();

    // Use the Jackson implementation
    jsonContext = new JacksonJSONContextClient();

    Map<String, Object> transportOptions = new HashMap<>();
    transportOptions.put(ClientTransport.JSON_CONTEXT_OPTION, jsonContext);

    // Adds the OAuth header in LongPollingTransport
    LongPollingTransport transport = new LongPollingTransport(
      transportOptions, httpClient) {
      @Override
      protected void customize(Request exchange) {
        super.customize(exchange);
        exchange.header("Authorization", "OAuth " + oAuthInfo.getAccessToken());
      }
    };

    // Now set up the Bayeux client itself
    return new BayeuxClient(oAuthInfo.getInstanceURL() + DEFAULT_PUSH_ENDPOINT, transport);
  }

  private void fetch() {
    try {
      if (bayeuxClient == null) {
        bayeuxClient = getClient(credentials);
        bayeuxClient.addExtension(new ReplayExtension(replay));
        context.addTaskCompletionListener(context1 -> {
          LOG.info("Task Completed.");
          if (bayeuxClient != null && bayeuxClient.isConnected()) {
            LOG.info("Unsubscribing Bayeux client...");
            bayeuxClient.getChannel("/topic/" + config.getPushTopicName()).unsubscribe();
            LOG.info("Disconnecting Bayeux client...");
            bayeuxClient.disconnect();
            bayeuxClient.waitFor(30, BayeuxClient.State.DISCONNECTED);
          }
        });
      }
      waitForHandshake();
      LOG.info("Bayeux client IsConnected? : {}", bayeuxClient.isConnected());
      LOG.info("Bayeux client IsDisonnected? : {}", bayeuxClient.isDisconnected());
      LOG.info("Bayeux client IsHandshook? : {}", bayeuxClient.isHandshook());

      bayeuxClient.getChannel("/topic/" + config.getPushTopicName()).subscribe((channel, message)
      -> {
        LOG.info("Message Received: {}", message.getData().toString());
        receivedMessages.add(jsonContext.getGenerator()
                               .generate(message.getDataAsMap()));
      });
      long expiryTimeMillis = startTime + batchDuration;
      while (System.currentTimeMillis() <= expiryTimeMillis) {
        //Add 10 sec delay.
        Thread.sleep(10000);
      }
      LOG.info("Received Messages Count: {}", receivedMessages.size());
    } catch (Exception exception) {
      LOG.error("Exception occurred while receiving the messages {}", exception);
    }
  }

  private void waitForHandshake() throws IOException {
    bayeuxClient.handshake();

    try {
      Awaitility.await()
        .atMost(HANDSHAKE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .pollInterval(HANDSHAKE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS)
        .until(() -> bayeuxClient.isHandshook());
    } catch (ConditionTimeoutException e) {
      throw new IOException("Client could not handshake with Salesforce server", e);
    }
    LOG.info("Client handshake done");
  }
  
}
