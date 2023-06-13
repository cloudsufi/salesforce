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

package io.cdap.plugin;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.utils.SalesforceClient;

import io.cdap.plugin.utils.enums.SObjects;
import org.junit.Assert;


import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;


/**
 * BQValidation.
 */
public class BQValidation {

  static List<JsonObject> bigQueryResponse = new ArrayList<>();
  static List<Object> bigQueryRows = new ArrayList<>();
  static Gson gson = new Gson();

  public static void main(String[] args) throws IOException, InterruptedException, SQLException, ParseException {
    validateSalesforceMultiObectToBQ(SObjects.AUTOMATION_CUSTOM__C.value);
  }


  public static boolean validateSalesforceToBQRecordValues(String objectName, String targetTable)
    throws SQLException, IOException, InterruptedException, ParseException {
    String Id=SalesforceClient.Id;

    getBigQueryTableData(targetTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject jsonData = gson.fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(jsonData);
    }
    SalesforceClient.queryObject(Id, objectName);
    return compareSalesforceAndJsonData(SalesforceClient.objectResponseList, bigQueryResponse, targetTable);

  }
  public static boolean validateBQToSalesforceRecordValues(String targetTable,String objectName)
    throws SQLException, IOException, InterruptedException, ParseException {
    SalesforceClient.queryObjectBQ(objectName);
    String Id= SalesforceClient.Id;

    getBigQueryTableData(targetTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject jsonData = gson.fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(jsonData);
    }
    SalesforceClient.queryObject(Id, objectName);
    return compareSalesforceAndJsonData(SalesforceClient.objectResponseList, bigQueryResponse, targetTable);

  }

  public static boolean validateSalesforceMultiObectToBQ(String objectName)
    throws SQLException,IOException,InterruptedException,ParseException{
    //String Id= SalesforceClient.Id;
    String Id="a03Dn000008cJfzIAE";
    //getTableNamesFromDataSet();
   // getTableByName();
   String targetTable = getTableByName();

    getBigQueryTableData(targetTable, bigQueryRows);
    for (Object rows : bigQueryRows) {
      JsonObject jsonData = gson.fromJson(String.valueOf(rows), JsonObject.class);
      bigQueryResponse.add(jsonData);
    }
    SalesforceClient.queryObject(Id,objectName);
    return compareSalesforceAndJsonData(SalesforceClient.objectResponseList, bigQueryResponse, targetTable);
  }

  private static void getBigQueryTableData(String table, List<Object> bigQueryRows)
    throws IOException, InterruptedException {

    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
    result.iterateAll().forEach(value -> bigQueryRows.add(value.get(0).getValue()));
  }
  public static TableResult getTableNamesFromDataSet() throws IOException, InterruptedException {
    String projectId = PluginPropertyUtils.pluginProp("projectId");
     String dataset=PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT table_name FROM `" + projectId + "." + dataset +
      "`.INFORMATION_SCHEMA.TABLES ";

    return BigQueryClient.getQueryResult(selectQuery);
  }
  public static String getTableByName() throws IOException, InterruptedException {
    String tableName="Automation_custom__c";
    TableResult tableResult = getTableNamesFromDataSet();
    Iterable<FieldValueList> rows = tableResult.iterateAll();

    for (FieldValueList row : rows) {
      FieldValue fieldValue = row.get(0);
      String currentTableName = fieldValue.getStringValue();

      if (currentTableName.equals(tableName)) {
        return currentTableName;
      }
    }

    return null; // Table not found
  }

  public static boolean compareSalesforceAndJsonData(List<JsonObject> salesforceData,
                                                     List<JsonObject> bigQueryData, String tableName)
    throws SQLException, ParseException {
    boolean result = false;
    if (bigQueryData == null) {
      Assert.fail("bigQueryData is null");
      return result;
    }

    int columnCountTarget = 0;
    if (salesforceData.size() > 0) {
      columnCountTarget = salesforceData.get(0).entrySet().size();
    }
    // Get the column count of the first JsonObject in bigQueryData
    int jsonObjectIdx = 0;
    int columnCountSource = 0;
    if (bigQueryData.size() > 0) {
      columnCountSource = bigQueryData.get(jsonObjectIdx).entrySet().size();
    }

    BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");

    // Build the table reference
    TableId tableRef = TableId.of(projectId, dataset, tableName);

    // Get the table schema
    Schema schema = bigQuery.getTable(tableRef).getDefinition().getSchema();

    // Iterate over the fields and print the column name and type
    int currentColumnCount = 1;
    while (currentColumnCount <= columnCountSource) {
      for (Field field : schema.getFields()) {
        String columnName = field.getName();
        String columnType = field.getType().toString();
        String columnTypeName = field.getType().getStandardType().name();

        switch (columnType) {

          case "BOOLEAN":
            boolean sourceAsBoolean = salesforceData.get(0).get(columnName).getAsBoolean();
            boolean targetAsBoolean = bigQueryData.get(jsonObjectIdx).get(columnName).getAsBoolean();
            Assert.assertEquals("Different values found for column : %s", sourceAsBoolean, targetAsBoolean);
            break;

          case "FLOAT":
            Double sourceVal = salesforceData.get(0).get(columnName).getAsDouble();
            Double targetVal = bigQueryData.get(jsonObjectIdx).get(columnName).getAsDouble();
            Assert.assertTrue(String.format("Different values found for column: %s", columnName),
                              sourceVal.compareTo(targetVal) == 0);
            break;

          case "TIMESTAMP":
            OffsetDateTime SourceTimestamp = OffsetDateTime.parse(
              salesforceData.get(0).get(columnName).getAsString(),
              DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ"));

            OffsetDateTime TargetTimestamp = OffsetDateTime.parse(
              bigQueryData.get(jsonObjectIdx).get(columnName).getAsString());
            Assert.assertEquals("Different values found for column : %s", SourceTimestamp, TargetTimestamp);
            break;

          case "TIME":
            DateTimeFormatter formatterSource = DateTimeFormatter.ofPattern("HH:mm:ss.SSSX");
            DateTimeFormatter formatterTarget = DateTimeFormatter.ofPattern("HH:mm:ss");
            LocalTime sourceTime = LocalTime.parse(salesforceData.get(jsonObjectIdx).get(columnName).getAsString(),
                                              formatterSource);
            LocalTime targetTime = LocalTime.parse(bigQueryData.get(jsonObjectIdx).get(columnName).getAsString(),
                                              formatterTarget);
            Assert.assertEquals("Different values found for column : %s", sourceTime, targetTime);
            break;

          case "DATE":
            JsonElement jsonElementSource = salesforceData.get(0).get(columnName);
            Date sourceDate = (jsonElementSource != null && !jsonElementSource.isJsonNull()) ? Date.valueOf(
              jsonElementSource.getAsString()) : null;
            JsonElement jsonElementTarget = bigQueryData.get(jsonObjectIdx).get(columnName);
            Date targetDate = (jsonElementTarget != null && !jsonElementTarget.isJsonNull()) ? Date.valueOf(
              jsonElementTarget.getAsString()) : null;
            Assert.assertEquals("Different values found for column : %s", sourceDate, targetDate);
            break;

          default:
            JsonElement sourceElement = salesforceData.get(0).get(columnName);
            String sourceString = (sourceElement != null && !sourceElement.isJsonNull()) ? sourceElement.getAsString() : null;
            JsonElement targetElement = bigQueryData.get(jsonObjectIdx).get(columnName);
            String targetString = (targetElement != null && !targetElement.isJsonNull()) ? targetElement.getAsString() : null;
            Assert.assertEquals(
              String.format("Different  values found for column : %s", columnName),
              String.valueOf(sourceString), String.valueOf(targetString));
            break;
        }
        currentColumnCount++;
      }
      jsonObjectIdx++;
    }
    return true;
  }
}

