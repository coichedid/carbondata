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

package org.apache.carbondata.store.rpc;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.store.rpc.model.BaseResponse;
import org.apache.carbondata.store.rpc.model.LoadDataRequest;
import org.apache.carbondata.store.rpc.model.QueryRequest;
import org.apache.carbondata.store.rpc.model.QueryResponse;
import org.apache.carbondata.store.rpc.model.ShutdownRequest;
import org.apache.carbondata.store.rpc.model.ShutdownResponse;

import org.apache.hadoop.ipc.VersionedProtocol;

@InterfaceAudience.Internal
public interface StoreService extends VersionedProtocol {

  long versionID = 1L;

  BaseResponse loadData(LoadDataRequest request);

  QueryResponse query(QueryRequest request);

  ShutdownResponse shutdown(ShutdownRequest request);
}