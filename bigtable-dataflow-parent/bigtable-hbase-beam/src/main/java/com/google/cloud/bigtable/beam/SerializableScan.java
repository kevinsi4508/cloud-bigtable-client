/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

/**
 * This is just a wrapper class to serialize HBase {@link Scan} using Protobuf. Ported from
 * org.apache.beam.sdk.io.hbase.SerializableScan. Remove this file once the class in beam SDK is
 * public.
 */
class SerializableScan implements Serializable {
  private transient Scan scan;

  public SerializableScan() {}

  public SerializableScan(Scan scan) {
    if (scan == null) {
      throw new NullPointerException("Scan must not be null.");
    }
    this.scan = scan;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    ProtobufUtil.toScan(scan).writeDelimitedTo(out);
  }

  private void readObject(ObjectInputStream in) throws IOException {
    scan = ProtobufUtil.toScan(ClientProtos.Scan.parseDelimitedFrom(in));
  }

  public Scan get() {
    return scan;
  }
}