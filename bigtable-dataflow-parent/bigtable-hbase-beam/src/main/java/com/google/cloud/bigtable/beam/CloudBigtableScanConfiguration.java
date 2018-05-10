/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.beam;

import java.io.Serializable;
import java.nio.charset.CharacterCodingException;
import java.util.Map;
import java.util.Objects;

import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ParseFilter;

import com.google.bigtable.repackaged.com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.repackaged.com.google.bigtable.v2.RowRange;
import com.google.bigtable.repackaged.com.google.bigtable.v2.RowSet;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.util.ByteStringer;
import com.google.bigtable.repackaged.com.google.common.base.MoreObjects;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.cloud.bigtable.hbase.adapters.Adapters;
import com.google.cloud.bigtable.hbase.adapters.read.DefaultReadHooks;
import com.google.cloud.bigtable.hbase.adapters.read.ReadHooks;

/**
 * This class defines configuration that a Cloud Bigtable client needs to connect to a user's Cloud
 * Bigtable instance; a table to connect to in the instance; and a filter on the table in the form of
 * a {@link Scan}.
 */
public class CloudBigtableScanConfiguration extends CloudBigtableTableConfiguration {

  private static final long serialVersionUID = 2435897354284600685L;

  /**
   * Converts a {@link CloudBigtableTableConfiguration} object to a {@link
   * CloudBigtableScanConfiguration} that will perform the specified {@link Scan} on the table.
   *
   * @param config The {@link CloudBigtableTableConfiguration} object.
   * @param scan The {@link Scan} to add to the configuration.
   * @return The new {@link CloudBigtableScanConfiguration}.
   */
  public static CloudBigtableScanConfiguration fromConfig(
      CloudBigtableTableConfiguration config, Scan scan) {
    CloudBigtableScanConfiguration.Builder builder = new CloudBigtableScanConfiguration.Builder();
    config.copyConfig(builder);
    return builder.withScan(scan).build();
  }

  /**
   * Builds a {@link CloudBigtableScanConfiguration}.
   */
  public static class Builder extends CloudBigtableTableConfiguration.Builder {
    private Scan scan;
    private ValueProvider<ReadRowsRequest> request;

    public Builder() {}

    /**
     * Specifies the {@link Scan} that will be used to filter the table.
     *
     * @param scan The {@link Scan} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    public Builder withScan(Scan scan) {
      this.scan = scan;
      this.request = null;
      return this;
    }

    /**
     * Specifies the {@link ReadRowsRequest} that will be used to filter the table.
     * @param request The {@link ReadRowsRequest} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    Builder withRequest(ValueProvider<ReadRowsRequest> request) {
      this.request = request;
      this.scan = null;
      return this;
    }

    /**
     * Specifies the {@link ReadRowsRequest} that will be used to filter the table.
     * @param request The {@link ReadRowsRequest} to add to the configuration.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    public Builder withRequest(ReadRowsRequest request) {
      return withRequest(StaticValueProvider.of(request));
    }

    static class RequestWithKeysValueProvider
        implements ValueProvider<ReadRowsRequest>, Serializable {
      private final ByteString startKey;
      private final ByteString stopKey;
      private final ValueProvider<ReadRowsRequest> request;
      private transient volatile ReadRowsRequest cachedRequest;

      RequestWithKeysValueProvider(
          ByteString startKey, ByteString stopKey, ValueProvider<ReadRowsRequest> request) {
        this.startKey = startKey;
        this.stopKey = stopKey;
        this.request = request;
      }

      @Override
      public ReadRowsRequest get() {
        if (cachedRequest == null) {
          cachedRequest =
              request
                  .get()
                  .toBuilder()
                  .setRows(
                      RowSet.newBuilder()
                          .addRowRanges(
                              RowRange.newBuilder()
                                  .setStartKeyClosed(startKey)
                                  .setEndKeyOpen(stopKey)))
                  .build();
        }
        return cachedRequest;
      }

      @Override
      public boolean isAccessible() {
        return request.isAccessible();
      }

      @Override
      public String toString() {
        if (isAccessible()) {
          return String.valueOf(get());
        }
        return MoreObjects.toStringHelper(this)
            .add("startKey", startKey)
            .add("stopKey", stopKey)
            .toString();
      }
    }

    /**
     * Internal API that allows a Source to configure the request with a new start/stop row range.
     *
     * @param startKey The first key, inclusive.
     * @param stopKey The last key, exclusive.
     * @return The {@link CloudBigtableScanConfiguration.Builder} for chaining convenience.
     */
    Builder withKeys(byte[] startKey, byte[] stopKey) {
      ByteString start = ByteStringer.wrap(startKey);
      ByteString stop = ByteStringer.wrap(stopKey);
      ValueProvider<ReadRowsRequest> request =
          this.request == null
              ? StaticValueProvider.of(ReadRowsRequest.getDefaultInstance())
              : this.request;
      return withRequest(new RequestWithKeysValueProvider(start, stop, request));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withProjectId(ValueProvider)} so
     * that it returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    Builder withProjectId(ValueProvider<String> projectId) {
      super.withProjectId(projectId);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withProjectId(String)} so that it
     * returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withProjectId(String projectId) {
      return withProjectId(StaticValueProvider.of(projectId));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withInstanceId(ValueProvider)} so
     * that it returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    Builder withInstanceId(ValueProvider<String> instanceId) {
      super.withInstanceId(instanceId);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withInstanceId(String)} so that
     * it returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withInstanceId(String instanceId) {
      return withInstanceId(StaticValueProvider.of(instanceId));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withConfiguration(String,
     * ValueProvider)} so that it returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    Builder withConfiguration(String key, ValueProvider<String> value) {
      super.withConfiguration(key, value);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withConfiguration(String,
     * String)} so that it returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withConfiguration(String key, String value) {
      return withConfiguration(key, StaticValueProvider.of(value));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withTableId(ValueProvider)} so
     * that it returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    Builder withTableId(ValueProvider<String> tableId) {
      super.withTableId(tableId);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Overrides {@link CloudBigtableTableConfiguration.Builder#withTableId(String)} so that it
     * returns {@link CloudBigtableScanConfiguration.Builder}.
     */
    @Override
    public Builder withTableId(String tableId) {
      return withTableId(StaticValueProvider.of(tableId));
    }

    /**
     * Builds the {@link CloudBigtableScanConfiguration}.
     *
     * @return The new {@link CloudBigtableScanConfiguration}.
     */
    @Override
    public CloudBigtableScanConfiguration build() {
      if (request == null) {
        ReadHooks readHooks = new DefaultReadHooks();
        if (scan == null) {
          scan = new Scan();
        }
        ReadRowsRequest.Builder builder = Adapters.SCAN_ADAPTER.adapt(scan, readHooks);
        request = StaticValueProvider.of(readHooks.applyPreSendHook(builder.build()));
      }
      return new CloudBigtableScanConfiguration(
          projectId, instanceId, tableId, request, additionalConfiguration);
    }
  }

  private ValueProvider<ReadRowsRequest> request;

  /**
   * Creates a {@link CloudBigtableScanConfiguration} using the specified project ID, instance ID,
   * table ID, {@link Scan} and additional connection configuration.
   *
   * @param projectId The project ID for the instance.
   * @param instanceId The instance ID.
   * @param tableId The table to connect to in the instance.
   * @param request The {@link ReadRowsRequest} that will be used to filter the table.
   * @param additionalConfiguration A {@link Map} with additional connection configuration.
   */
  protected CloudBigtableScanConfiguration(
      ValueProvider<String> projectId,
      ValueProvider<String> instanceId,
      ValueProvider<String> tableId,
      ValueProvider<ReadRowsRequest> request,
      Map<String, ValueProvider<String>> additionalConfiguration) {
    super(projectId, instanceId,  tableId, additionalConfiguration);
    this.request = request;
  }

  /**
   * Gets the {@link ReadRowsRequest}. Sets the table name if it is not yet set.
   *
   * @return The {@link ReadRowsRequest}.
   */
  public ReadRowsRequest getRequest() {
    if (request.get().getTableName().isEmpty()) {
      BigtableInstanceName bigtableInstanceName =
          new BigtableInstanceName(getProjectId(), getInstanceId());
      String fullTableName = bigtableInstanceName.toTableNameStr(getTableId());
      request =
          StaticValueProvider.of(request.get().toBuilder().setTableName(fullTableName).build());
    }
    return request.get();
  }

  /**
   * @return The start row for this configuration.
   */
  public byte[] getStartRow() {
    return getStartRowByteString().toByteArray();
  }

  /**
   * @return The stop row for this configuration.
   */
  public byte[] getStopRow() {
    return getStopRowByteString().toByteArray();
  }

  /**
   * @return The start row for this configuration.
   */
  byte[] getZeroCopyStartRow() {
    return ByteStringer.extract(getStartRowByteString());
  }

  /**
   * @return The stop row for this configuration.
   */
  byte[] getZeroCopyStopRow() {
    return ByteStringer.extract(getStopRowByteString());
  }

  ByteString getStartRowByteString() {
    return getRowRange().getStartKeyClosed();
  }

  ByteString getStopRowByteString() {
    return getRowRange().getEndKeyOpen();
  }

  RowRange getRowRange() {
    RowSet rows = getRequest().getRows();
    return rows.getRowRanges(0);
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj)
        && Objects.equals(getRequest(), ((CloudBigtableScanConfiguration) obj).getRequest());
  }

  @Override
  public Builder toBuilder() {
    Builder builder = new Builder();
    copyConfig(builder);
    return builder;
  }

  public void copyConfig(Builder builder) {
    super.copyConfig(builder);
    builder.withRequest(request);
  }

  /**
   * Creates a {@link ByteKeyRange} representing the start and stop keys for this instance.
   * @return A {@link ByteKeyRange}.
   */
  public ByteKeyRange toByteKeyRange() {
    return ByteKeyRange.of(ByteKey.copyFrom(getZeroCopyStartRow()),
      ByteKey.copyFrom(getZeroCopyStopRow()));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .add(DisplayData.item("readRowsRequest", request.toString()).withLabel("ReadRowsRequest"));
  }

}
