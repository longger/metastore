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

package org.apache.hadoop.hive.metastore.model;


public class MBusiTypeSchemaColumn {

  private MBusiType busiType; // partitionname ==>  (key=value/)*(key=value)
  private MSchema schema;
  private String column;

  public MBusiTypeSchemaColumn() {}

  public MBusiTypeSchemaColumn(MBusiType busiType, MSchema schema, String column) {
    super();
    this.busiType = busiType;
    this.schema = schema;
    this.column = column;
  }

  public MBusiType getBusiType() {
    return busiType;
  }

  public void setBusiType(MBusiType busiType) {
    this.busiType = busiType;
  }

  public MSchema getSchema() {
    return schema;
  }

  public void setSchema(MSchema schema) {
    this.schema = schema;
  }

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }


}