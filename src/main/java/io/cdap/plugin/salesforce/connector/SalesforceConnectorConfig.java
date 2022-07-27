/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.plugin.salesforce.connector;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.salesforce.SalesforceConstants;

/**
 * PluginConfig for Salesforce Connector
 */

public class SalesforceConnectorConfig extends PluginConfig {
  @Name(SalesforceConstants.PROPERTY_CONSUMER_KEY)
  @Macro
  @Description(" The Consumer Key for Salesforce Instance.")
  private final String consumerKey;

  @Name(SalesforceConstants.PROPERTY_CONSUMER_SECRET)
  @Macro
  @Description("The Consumer Secret for ServiceNow Instance.")
  private final String consumerSecret;

  @Name(SalesforceConstants.PROPERTY_USERNAME)
  @Macro
  @Description("The username for Salesforce Instance.")
  private final String username;

  @Name(SalesforceConstants.PROPERTY_PASSWORD)
  @Macro
  @Description("The password for Salesforce Instance.")
  private final String password;

  @Name(SalesforceConstants.PROPERTY_SECURITY_TOKEN)
  @Macro
  @Description("The Security Token for Salesforce Instance.")
  private final String securityToken;

  @Name(SalesforceConstants.PROPERTY_LOGIN_URL)
  @Macro
  @Description("The Login URl for Salesforce Instance.")
  private final String loginUrl;

  public SalesforceConnectorConfig(String consumerKey, String consumerSecret, String username, String password,
                                   String securityToken, String loginUrl) {
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
    this.username = username;
    this.password = password;
    this.securityToken = securityToken;
    this.loginUrl = loginUrl;
  }

  public String getConsumerKey() {
    return consumerKey;
  }

  public String getConsumerSecret() {
    return consumerSecret;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getSecurityToken() {
    return securityToken;
  }

  public String getLoginUrl() {
    return loginUrl;
  }


  /**
   * validates all the fields which are mandatory for the connection.
   */
  public void validateCredentialsFields(FailureCollector collector) {
    if (isNullOrEmpty(getConsumerKey())) {
      collector.addFailure("Consumer Key must be specified.", null)
        .withConfigProperty(SalesforceConstants.PROPERTY_CONSUMER_KEY);
    }

    if (isNullOrEmpty(getConsumerSecret())) {
      collector.addFailure("Consumer Secret must be specified.", null)
        .withConfigProperty(SalesforceConstants.PROPERTY_CONSUMER_SECRET);
    }

    if (isNullOrEmpty(getSecurityToken())) {
      collector.addFailure("Security Token must be specified.", null)
        .withConfigProperty(SalesforceConstants.PROPERTY_SECURITY_TOKEN);
    }

    if (isNullOrEmpty(getUsername())) {
      collector.addFailure("User name must be specified.", null)
        .withConfigProperty(SalesforceConstants.PROPERTY_USERNAME);
    }

    if (isNullOrEmpty(getPassword())) {
      collector.addFailure("Password must be specified.", null)
        .withConfigProperty(SalesforceConstants.PROPERTY_PASSWORD);
    }

    if (isNullOrEmpty(getLoginUrl())) {
      collector.addFailure("Login Url must be specified.", null)
        .withConfigProperty(SalesforceConstants.PROPERTY_LOGIN_URL);
    }
  }
  /**
   * Utility function to check if incoming string is empty or not.
   *
   * @param string The value to be checked for emptyness
   * @return true if string is empty otherwise false
   */
  public static boolean isNullOrEmpty(String string) {
    return Strings.isNullOrEmpty(Strings.nullToEmpty(string).trim());
  }

}
