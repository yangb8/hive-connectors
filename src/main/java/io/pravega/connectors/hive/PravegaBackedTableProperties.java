/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.connectors.hive;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PravegaBackedTableProperties {
  private static final Logger LOG = Logger.getLogger(PravegaBackedTableProperties.class);
  public static final String KAFKA_WHITELIST_TOPICS = "kafka.whitelist.topics";
  public static final String KAFKA_AVRO_SCHEMA_FILE = "kafka.avro.schema.file";
  public static final String COLUMN_NAMES = "columns";
  public static final String COLUMN_TYPES = "columns.types";
  public static final String COLUMN_COMMENTS = "columns.comments";

  /*
   * This method initializes properties of the external table and populates
   * the jobProperties map with them so that they can be used throughout the job.
   */
  public void initialize(Properties tableProperties, Map<String,String> jobProperties,
                         TableDesc tableDesc) {

    // Set kafka.whitelist.topics in the jobProperty
    String pravegaScopeName = tableProperties.getProperty(PravegaInputFormat.SCOPE_NAME);
    LOG.debug("Pravega Scope Name : " + pravegaScopeName);
    jobProperties.put(PravegaInputFormat.SCOPE_NAME, pravegaScopeName);

    String pravegaStreamName = tableProperties.getProperty(PravegaInputFormat.STREAM_NAME);
    LOG.debug("Pravega Stream Name : " + pravegaStreamName);
    jobProperties.put(PravegaInputFormat.STREAM_NAME, pravegaStreamName);

    String pravegaUri = tableProperties.getProperty(PravegaInputFormat.URI_STRING);
    LOG.debug("Pravega URI : " + pravegaUri);
    jobProperties.put(PravegaInputFormat.URI_STRING, pravegaUri);

    // Set kafka.avro.schema.file in the jobProperty
	/*
    String kafkaAvroSchemaFile = tableProperties.getProperty(KAFKA_AVRO_SCHEMA_FILE, null);
    if (kafkaAvroSchemaFile != null) {
      LOG.debug("Kafka avro schema file : " + kafkaAvroSchemaFile);
      jobProperties.put(KAFKA_AVRO_SCHEMA_FILE, kafkaAvroSchemaFile);
    }

    // Set column names in the jobProperty
    String columnNames = tableProperties.getProperty(COLUMN_NAMES);
    LOG.debug("Column Names : " + columnNames);
    jobProperties.put(COLUMN_NAMES, columnNames);

    // Set column types in the jobProperty
    String columnTypes = tableProperties.getProperty(COLUMN_TYPES);
    LOG.debug("Column Types : " + columnTypes);
    jobProperties.put(COLUMN_TYPES, columnTypes);

    // Set column types in the jobProperty
    String columnComments = tableProperties.getProperty(COLUMN_COMMENTS);
    LOG.debug("Column Comments : " + columnComments);
    jobProperties.put(COLUMN_COMMENTS, columnComments);
	*/
  }
}
