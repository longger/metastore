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

package org.apache.hadoop.hive.ql.plan;

import java.util.HashMap;

@Explain(displayName="privilege subject")
public class PrivilegeObjectDesc {

  private boolean table;
  private boolean schema;
  private String object;

  private HashMap<String, String> partSpec;

  public PrivilegeObjectDesc(boolean isTable, String object,
      HashMap<String, String> partSpec) {
    super();
    this.table = isTable;
    this.object = object;
    this.partSpec = partSpec;
  }

  public PrivilegeObjectDesc(boolean isTable, boolean isSchema, String object,
      HashMap<String, String> partSpec) {
    super();
    this.table = isTable;
    this.schema = isSchema;
    this.object = object;
    this.partSpec = partSpec;
  }

  public PrivilegeObjectDesc() {
  }

  @Explain(displayName="is table")
  public boolean getTable() {
    return table;
  }

  public void setTable(boolean isTable) {
    this.table = isTable;
  }

  @Explain(displayName="is schema")
  public boolean getSchema() {
    return schema;
  }

  public void setSchema(boolean isSchema) {
    this.schema = isSchema;
  }

  @Explain(displayName="object")
  public String getObject() {
    return object;
  }

  public void setObject(String object) {
    this.object = object;
  }

  @Explain(displayName="partition spec")
  public HashMap<String, String> getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(HashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

}
