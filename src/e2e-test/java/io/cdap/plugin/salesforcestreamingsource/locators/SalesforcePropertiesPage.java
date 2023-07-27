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

package io.cdap.plugin.salesforcestreamingsource.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Salesforce Streaming source - Locators.
 */
public class SalesforcePropertiesPage {

  // SalesforceStreaming  Source - Properties page - Reference section
  @FindBy(how = How.XPATH, using = "//input[@data-cy='referenceName']")
  public static WebElement referenceInput;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='pushTopicName']")
  public static WebElement topicnameInput;

  @FindBy(how = How.XPATH, using = "//input[@data-cy='pushTopicQuery']")
  public static WebElement topicqueryInput;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='pipeline-configure-modeless-btn']")
  public static WebElement configButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='tab-content-Pipeline config']")
  public static WebElement pipelineConfig;

  @FindBy(how = How.XPATH, using = "//span[contains(text(), \"Batch interval\")]//following-sibling::div//select[1]")
  public static WebElement batchTime;

  @FindBy(how = How.XPATH, using = "//span[contains(text(), \"Batch interval\")]//following-sibling::div//select[2]")
  public static WebElement timeSelect;

  @FindBy(how = How.XPATH, using = "//button[@data-testid='config-apply-close']")
  public static WebElement saveButton;
}
