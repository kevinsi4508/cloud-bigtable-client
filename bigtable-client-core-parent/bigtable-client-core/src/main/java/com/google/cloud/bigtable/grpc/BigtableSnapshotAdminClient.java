/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc;

import com.google.bigtable.admin.v2.CreateSnapshotRequest;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.bigtable.admin.v2.GetSnapshotRequest;
import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.admin.v2.ListSnapshotsRequest;
import com.google.bigtable.admin.v2.ListSnapshotsResponse;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.ListTablesResponse;
import com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest;
import com.google.bigtable.admin.v2.Snapshot;
import com.google.bigtable.admin.v2.Table;

/**
 * A client for the Cloud Bigtable Table Admin API.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public interface BigtableSnapshotAdminClient {


  /**
   * Creates a new table, to be served from a specified cluster.
   * The table can be created with a full set of initial column families,
   * specified in the request.
   *
   * @param request a {@link com.google.bigtable.admin.v2.CreateTableRequest} object.
   */
  com.google.longrunning.Operation createSnapshot(CreateSnapshotRequest request);

  /**
   * Gets the details of a table.
   *
   * @param request a {@link com.google.bigtable.admin.v2.GetTableRequest} object.
   * @return a {@link com.google.bigtable.admin.v2.Table} object.
   */
  Snapshot getSnapshot(GetSnapshotRequest request);
  
  ListSnapshotsResponse listSnapshots(ListSnapshotsRequest request);
}
