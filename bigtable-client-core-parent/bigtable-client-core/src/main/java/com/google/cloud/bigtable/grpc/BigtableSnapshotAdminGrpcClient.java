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

import com.google.bigtable.admin.v2.BigtableSnapshotAdminGrpc;
import com.google.bigtable.admin.v2.BigtableTableAdminGrpc;
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

import io.grpc.Channel;

/**
 * A gRPC client for accessing the Bigtable Table Admin API.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class BigtableSnapshotAdminGrpcClient implements BigtableSnapshotAdminClient {

  private final BigtableSnapshotAdminGrpc.BigtableSnapshotAdminBlockingStub blockingStub;

  /**
   * <p>Constructor for BigtableTableAdminGrpcClient.</p>
   *
   * @param channel a {@link io.grpc.Channel} object.
   */
  public BigtableSnapshotAdminGrpcClient(Channel channel) {
    blockingStub = BigtableSnapshotAdminGrpc.newBlockingStub(channel);
  }

  /** {@inheritDoc} */
  @Override
  public com.google.longrunning.Operation createSnapshot(CreateSnapshotRequest request) {
    return blockingStub.createSnapshot(request);
  }

  /** {@inheritDoc} */
  @Override
  public Snapshot getSnapshot(GetSnapshotRequest request) {
    return blockingStub.getSnapshot(request);
  }
  
  @Override
  public ListSnapshotsResponse listSnapshots(ListSnapshotsRequest request) {
    return blockingStub.listSnapshots(request);
  }
}
