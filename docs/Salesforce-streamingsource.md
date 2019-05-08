# Salesforce Streaming Source


Description
-----------
This source tracks updates in Salesforce sObjects.
Examples of sObjects are opportunities, contacts, accounts, leads, any custom object, etc.

To track the updates Salesforce PushTopic events are used.

The generation of PushTopic notifications in Salesforce follows this sequence:
1. Create a PushTopic based on a SOQL query. The PushTopic defines the channel.
2. Clients subscribe to the channel.
3. A record is created, updated, deleted, or undeleted (an event occurs). The changes to that record are evaluated.
4. If the record changes match the criteria of the PushTopic query, a notification is generated by the server and received by the subscribed clients.

The plugin can either create a PushTopic for the user, or use an existing one.

Configuration
-------------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Username:** Salesforce username.

**Password:** Salesforce password.

**Consumer Key:** Application Consumer Key. This is also known as the OAuth client id.
A Salesforce connected application must be created in order to get a consumer key.

**Consumer Secret:** Application Consumer Secret. This is also known as the OAuth client secret.
A Salesforce connected application must be created in order to get a client secret.

**Login Url:** Salesforce OAuth2 login url.

**Topic Name:** Salesforce push topic name. Plugin will track updates from this topic. If the topic does
not exist, the plugin creates it. To manually create pushTopic use Salesforce workbench, Apex code or API.


**Query:** Salesforce push topic query. The query is used by Salesforce to send updates to push topic.
This field not required if you are using an existing push topic.


**Notify On Create:** Push topic property, which specifies if a create operation should generate a record.


**Notify On Update:** Push topic property, which specifies if a update operation should generate a record.


**Notify On Delete:** Push topic property, which specifies if an delete operation should generate a record.


**Notify For Fields:** Push topic property, which specifies how the record is evaluated against the
PushTopic query. The NotifyForFields values are:<br>
_All_	- Notifications are generated for all record field changes, provided the evaluated records match
the criteria specified in the WHERE clause.<br>
_Referenced (default)_ -	Changes to fields referenced in the SELECT and WHERE clauses are evaluated.
Notifications are generated for the evaluated records only if they match the criteria specified
in the WHERE clause.<br>
_Select_	- Changes to fields referenced in the SELECT clause are evaluated. Notifications are generated
for the evaluated records only if they match the criteria specified in the WHERE clause.<br>
_Where_	- Changes to fields referenced in the WHERE clause are evaluated. Notifications are generated
for the evaluated records only if they match the criteria specified in the WHERE clause.

**SObject Name:** Salesforce object name to read. If value is provided, plugin will get all fields for this object from
Salesforce and generate SOQL query (`select <FIELD_1, FIELD_2, ..., FIELD_N> from ${sObjectName}`).
Ignored if SOQL query is provided.

**Schema:** The schema of output objects.
The Salesforce types will be automatically mapped to schema types as shown below:


+-------------+----------------------------------------------------------------------------+--------------+
| Schema type |                              Salesforce type                               |    Notes     |
+-------------+----------------------------------------------------------------------------+--------------+
| bool        | _bool                                                                      |              |
| int         | _int                                                                       |              |
| long        | _long                                                                      |              |
| double      | _double, currency, percent                                                 |              |
| date        | date                                                                       |              |
| timestamp   | datetime                                                                   | Microseconds |
| time        | time                                                                       | Microseconds |
| string      | picklist, multipicklist, combobox, reference, base64,                      |              |
|             | textarea, phone, id, url, email, encryptedstring,                          |              |
|             | datacategorygroupreference, location, address, anyType, json, complexvalue |              |
+-------------+----------------------------------------------------------------------------+--------------+