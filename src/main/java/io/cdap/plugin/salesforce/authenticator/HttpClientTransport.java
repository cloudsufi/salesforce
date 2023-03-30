/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.authenticator;


import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.tools.VersionInfo;
import com.sforce.ws.transport.LimitingOutputStream;
import com.sforce.ws.transport.MessageHandlerOutputStream;
import com.sforce.ws.transport.Transport;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * This class implements the Transport interface for WSC with HttpClient in order to properly work
 * with NTLM proxies.  The existing JdkHttpTransport in WSC does not work with NTLM proxies when
 * compiled on Java 1.6
 *
 * @author Jeff Lai
 * @since 25.0.2
 */
public class HttpClientTransport implements Transport {

  private ConnectorConfig config;
  private boolean successful;
  private HttpPost post;
  private OutputStream output;
  private ByteArrayOutputStream entityByteOut;

  public HttpClientTransport() {
  }

  public HttpClientTransport(ConnectorConfig config) {
    setConfig(config);
  }

  @Override
  public void setConfig(ConnectorConfig config) {
    this.config = config;
  }

  @Override
  public OutputStream connect(String url, String soapAction) throws IOException {
    if (soapAction == null) {
      soapAction = "";
    }

    HashMap<String, String> header = new HashMap<String, String>();

    header.put("SOAPAction", "\"" + soapAction + "\"");
    header.put("Content-Type", "text/xml; charset=UTF-8");
    header.put("Accept", "text/xml");

    return connect(url, header);
  }

  @Override
  public InputStream getContent() throws IOException {
    InputStream input;
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    if (config.getProxy().address() != null) {
      String proxyUser = config.getProxyUsername() == null ? "" : config.getProxyUsername();
      String proxyPassword = config.getProxyPassword() == null ? "" : config.getProxyPassword();

      Credentials credentials;

      if (config.getNtlmDomain() != null && !config.getNtlmDomain().equals("")) {
        String computerName = InetAddress.getLocalHost().getCanonicalHostName();
        credentials = new NTCredentials(proxyUser, proxyPassword, computerName, config.getNtlmDomain());
      } else {
        credentials = new UsernamePasswordCredentials(proxyUser, proxyPassword);
      }

      InetSocketAddress proxyAddress = (InetSocketAddress) config.getProxy().address();
      HttpHost proxyHost = new HttpHost(proxyAddress.getHostName(), proxyAddress.getPort(), "http");
      httpClientBuilder.setProxy(proxyHost);

      CredentialsProvider credentialsprovider = new BasicCredentialsProvider();
      AuthScope scope = new AuthScope(proxyAddress.getHostName(), proxyAddress.getPort(), null, null);
      credentialsprovider.setCredentials(scope, credentials);
      httpClientBuilder.setDefaultCredentialsProvider(credentialsprovider);
    }

    try (CloseableHttpClient httpClient = httpClientBuilder.build()) {

      byte[] entityBytes = entityByteOut.toByteArray();
      HttpEntity entity = new ByteArrayEntity(entityBytes);
      post.setEntity(entity);

      if (config.getNtlmDomain() != null && !config.getNtlmDomain().equals("")) {
        // need to send a HEAD request to trigger NTLM authentication
        try (CloseableHttpResponse ignored = httpClient.execute(new HttpHead("http://salesforce.com"))) {
        }
      }

      try (CloseableHttpResponse response = httpClient.execute(post)) {
        successful = true;
        if (response.getStatusLine().getStatusCode() > 399) {
          successful = false;
          if (response.getStatusLine().getStatusCode() == 407) {
            throw new RuntimeException(
              response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          }
        }

        // copy input stream data into a new input stream because releasing the connection will close the input stream
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        try (InputStream inStream = response.getEntity().getContent()) {
          IOUtils.copy(inStream, bOut);
          input = new ByteArrayInputStream(bOut.toByteArray());
          if (response.containsHeader("Content-Encoding") && response.getHeaders("Content-Encoding")[0].getValue()
            .equals("gzip")) {
            input = new GZIPInputStream(input);
          }
        }
      }
    }
    return input;
  }

  @Override
  public boolean isSuccessful() {
    return successful;
  }

  @Override
  public OutputStream connect(String endpoint, HashMap<String, String> httpHeaders) throws IOException {
    return connect(endpoint, httpHeaders, true);
  }

  @Override
  public OutputStream connect(String endpoint, HashMap<String, String> httpHeaders, boolean enableCompression) throws
    IOException {
    post = new HttpPost(endpoint);

    for (String name : httpHeaders.keySet()) {
      post.addHeader(name, httpHeaders.get(name));
    }

    post.addHeader("User-Agent", VersionInfo.info());

    if (enableCompression) {
      post.addHeader("Content-Encoding", "gzip");
      post.addHeader("Accept-Encoding", "gzip");
    }

    entityByteOut = new ByteArrayOutputStream();
    output = entityByteOut;

    if (config.getMaxRequestSize() > 0) {
      output = new LimitingOutputStream(config.getMaxRequestSize(), output);
    }

    if (enableCompression && config.isCompression()) {
      output = new GZIPOutputStream(output);
    }

    if (config.isTraceMessage()) {
      output = config.teeOutputStream(output);
    }

    if (config.hasMessageHandlers()) {
      URL url = new URL(endpoint);
      output = new MessageHandlerOutputStream(config, url, output);
    }

    return output;
  }

}
