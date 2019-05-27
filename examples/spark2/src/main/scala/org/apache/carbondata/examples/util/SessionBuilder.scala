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

package org.apache.carbondata.examples.util

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties


// scalastyle:off println

object SessionBuilder {

  def createCarbonSession(appName: String, warehouse: String, storeLocation: String, metastoredb: String): SparkSession = {


    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
      .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "")
    import org.apache.spark.sql.CarbonSession._

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("yarn")
      .appName(appName)
      .config("spark.sql.warehouse.dir", warehouse)
      .config(CarbonCommonConstants.STORE_LOCATION, storeLocation)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark
  }
}
// scalastyle:on println
