/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.sdk.file.util;

import org.apache.carbondata.sdk.file.CarbonReader;
import org.apache.carbondata.sdk.file.CarbonReaderBuilder;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;

import org.apache.hadoop.conf.Configuration;

import py4j.GatewayServer;

public class CarbonPythonGatewayServer {

  public CarbonReaderBuilder createCarbonReaderBuilder() {
    return CarbonReader.builder();
  }

  public CarbonWriterBuilder createCarbonWriterBuilder() {
    return CarbonWriter.builder();
  }

  public Configuration createConfiguration() {
    return new Configuration();
  }

  public static void main(String[] args) {
    CarbonPythonGatewayServer app = new CarbonPythonGatewayServer();
    // app is now the gateway.entry_point
    GatewayServer server = new GatewayServer(app);
    server.start();
  }
}