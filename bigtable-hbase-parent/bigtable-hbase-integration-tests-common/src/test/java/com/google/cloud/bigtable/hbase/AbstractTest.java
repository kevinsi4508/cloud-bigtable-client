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

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AssumptionViolatedException;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

public abstract class AbstractTest {

  public SharedTestEnvRule sharedTestEnv = SharedTestEnvRule.getInstance();

  protected static DataGenerationHelper dataHelper = new DataGenerationHelper();
  protected Logger logger = new Logger(this.getClass());
  @Rule
  public TestWatcher loggingRule = new TestWatcher() {
    private long start;

    @Override
    public void starting(Description description) {
      this.start = System.currentTimeMillis();
      logger.info("Starting: %s", description.getDisplayName());
    }

    @Override
    protected void failed(Throwable e, Description description) {
      logger.warn("Test: %s failed in %d ms.", e,description.getDisplayName(),
          System.currentTimeMillis() - start);
    }

    @Override
    protected void succeeded(Description description) {
      logger.info("Test: %s succeeded in %d ms.", description.getDisplayName(),
          System.currentTimeMillis() - start);
    }

    @Override
    protected void skipped(AssumptionViolatedException e, Description description) {
      logger.info("Test: %s skipped in %d ms.", description.getDisplayName(),
          System.currentTimeMillis() - start);
    }
  };

  // This is for when we need to look at the results outside of the current connection
  protected Connection createNewConnection() throws IOException {
    return sharedTestEnv.createConnection();
  }

  protected Connection getConnection() {
    return sharedTestEnv.getConnection();
  }

  protected Table getDefaultTable() throws IOException {
    return sharedTestEnv.getDefaultTable();
  }

  protected static class QualifierValue implements Comparable<QualifierValue> {

    protected final byte[] qualifier;
    protected final byte[] value;

    public QualifierValue(byte[] qualifier, byte[] value) {
      this.qualifier = qualifier;
      this.value = value;
    }

    @Override
    public int compareTo(QualifierValue qualifierValue) {
      return Bytes.compareTo(this.qualifier, qualifierValue.qualifier);
    }
  }
}
