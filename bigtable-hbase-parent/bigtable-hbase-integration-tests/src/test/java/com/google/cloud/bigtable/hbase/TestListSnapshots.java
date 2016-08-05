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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.IntegrationTests.COLUMN_FAMILY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestListSnapshots extends AbstractTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private List<TableName> tablesToDelete = new ArrayList<>();
  
  @Test
  public void testSnapshotNames() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      List<HBaseProtos.SnapshotDescription> list = admin.listSnapshots();
      for (HBaseProtos.SnapshotDescription d : list) {
        System.out.println("SnapshotDescription: " + d);
      }
    }
  }
}
