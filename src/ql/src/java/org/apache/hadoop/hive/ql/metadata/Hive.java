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

package org.apache.hadoop.hive.ql.metadata;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.serde.serdeConstants.COLLECTION_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.ESCAPE_CHAR;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.LINE_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.MAPKEY_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Busitype;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EquipRoom;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GeoLocation;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SplitValue;
import org.apache.hadoop.hive.metastore.api.Subpartition;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.HiveIndex.IndexType;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.CreateTableAutomaticGrant;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

/**
 * The Hive class contains information about this instance of Hive. An instances
 * of Hive represents a set of data in a file system (usually HDFS) organized
 * for easy query processing
 *
 */

public class Hive {

  static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Hive");

  private HiveConf conf = null;
  private IMetaStoreClient metaStoreClient;

  private String currentDatabase;
  //added by liulichao
  private static String rootName = "root";//root
  private static String rootPWD = "111111";//root

  private static ThreadLocal<Hive> hiveDB = new ThreadLocal() {
    @Override
    protected synchronized Object initialValue() {
      return null;
    }

    @Override
    public synchronized void remove() {
      if (this.get() != null) {
        ((Hive) this.get()).close();
      }
      super.remove();
    }
  };

  /**
   * Gets hive object for the current thread. If one is not initialized then a
   * new one is created If the new configuration is different in metadata conf
   * vars then a new one is created.
   *
   * @param c
   *          new Hive Configuration
   * @return Hive object for current thread
   * @throws HiveException
   *
   */
  public static Hive get(HiveConf c) throws HiveException {
    boolean needsRefresh = false;
    Hive db = hiveDB.get();
    if (db != null) {
      for (HiveConf.ConfVars oneVar : HiveConf.metaVars) {
        // Since metaVars are all of different types, use string for comparison
        String oldVar = db.getConf().get(oneVar.varname, "");
        String newVar = c.get(oneVar.varname, "");
        if (oldVar.compareToIgnoreCase(newVar) != 0) {
          needsRefresh = true;
          break;
        }
      }
    }
    return get(c, needsRefresh);
  }

  /**
   * get a connection to metastore. see get(HiveConf) function for comments
   *
   * @param c
   *          new conf
   * @param needsRefresh
   *          if true then creates a new one
   * @return The connection to the metastore
   * @throws HiveException
   */
  public static Hive get(HiveConf c, boolean needsRefresh) throws HiveException {
    Hive db = hiveDB.get();
    if (db == null || needsRefresh) {
      closeCurrent();
      c.set("fs.scheme.class", "dfs");
      Hive newdb = new Hive(c);
      if (db != null && db.getCurrentDatabase() != null){
        newdb.setCurrentDatabase(db.getCurrentDatabase());
      }
      hiveDB.set(newdb);
      return newdb;
    }
    db.conf = c;
    return db;
  }

  public static Hive get() throws HiveException {
    Hive db = hiveDB.get();
    if (db == null) {
      db = new Hive(new HiveConf(Hive.class));
      hiveDB.set(db);
    }
    return db;
  }

  public static void closeCurrent() {
    hiveDB.remove();
  }

  /**
   * Hive
   *
   * @param argFsRoot
   * @param c
   *
   */
  private Hive(HiveConf c) throws HiveException {
    conf = c;
  }

  /**
   * closes the connection to metastore for the calling thread
   */
  private void close() {
    LOG.debug("Closing current thread's connection to Hive Metastore.");
    if (metaStoreClient != null) {
      metaStoreClient.close();
      metaStoreClient = null;
    }
  }

  /**
   * Create a database
   * @param db
   * @param ifNotExist if true, will ignore AlreadyExistsException exception
   * @throws AlreadyExistsException
   * @throws HiveException
   */
  public void createDatabase(Database db, boolean ifNotExist)
      throws AlreadyExistsException, HiveException {
    try {
      getMSC().createDatabase(db);
    } catch (AlreadyExistsException e) {
      if (!ifNotExist) {
        throw e;
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Create a Database. Raise an error if a database with the same name already exists.
   * @param db
   * @throws AlreadyExistsException
   * @throws HiveException
   */
  public void createDatabase(Database db) throws AlreadyExistsException, HiveException {
    createDatabase(db, false);
  }

  /**
   * Drop a database.
   * @param name
   * @throws NoSuchObjectException
   * @throws HiveException
   * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#dropDatabase(java.lang.String)
   */
  public void dropDatabase(String name) throws HiveException, NoSuchObjectException {
    dropDatabase(name, true, false, false);
  }

  /**
   * Drop a database
   * @param name
   * @param deleteData
   * @param ignoreUnknownDb if true, will ignore NoSuchObjectException
   * @throws HiveException
   * @throws NoSuchObjectException
   */
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws HiveException, NoSuchObjectException {
    dropDatabase(name, deleteData, ignoreUnknownDb, false);
  }

  /**
   * Drop a database
   * @param name
   * @param deleteData
   * @param ignoreUnknownDb if true, will ignore NoSuchObjectException
   * @param cascade           if true, delete all tables on the DB if exists. Othewise, the query
   *                        will fail if table still exists.
   * @throws HiveException
   * @throws NoSuchObjectException
   */
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws HiveException, NoSuchObjectException {
    try {
      getMSC().dropDatabase(name, deleteData, ignoreUnknownDb, cascade);
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  /**
   * Creates a table metdata and the directory for the table data
   *
   * @param tableName
   *          name of the table
   * @param columns
   *          list of fields of the table
   * @param partCols
   *          partition keys of the table
   * @param fileInputFormat
   *          Class of the input format of the table data file
   * @param fileOutputFormat
   *          Class of the output format of the table data file
   * @throws HiveException
   *           thrown if the args are invalid or if the metadata or the data
   *           directory couldn't be created
   */
  public void createTable(String tableName, List<String> columns,
      List<String> partCols, Class<? extends InputFormat> fileInputFormat,
      Class<?> fileOutputFormat) throws HiveException {
    this.createTable(tableName, columns, partCols, fileInputFormat,
        fileOutputFormat, -1, null);
  }

  /**
   * Creates a table metdata and the directory for the table data
   *
   * @param tableName
   *          name of the table
   * @param columns
   *          list of fields of the table
   * @param partCols
   *          partition keys of the table
   * @param fileInputFormat
   *          Class of the input format of the table data file
   * @param fileOutputFormat
   *          Class of the output format of the table data file
   * @param bucketCount
   *          number of buckets that each partition (or the table itself) should
   *          be divided into
   * @throws HiveException
   *           thrown if the args are invalid or if the metadata or the data
   *           directory couldn't be created
   */
  public void createTable(String tableName, List<String> columns,
      List<String> partCols, Class<? extends InputFormat> fileInputFormat,
      Class<?> fileOutputFormat, int bucketCount, List<String> bucketCols)
      throws HiveException {
    if (columns == null) {
      throw new HiveException("columns not specified for table " + tableName);
    }

    Table tbl = newTable(tableName);
    tbl.setInputFormatClass(fileInputFormat.getName());
    tbl.setOutputFormatClass(fileOutputFormat.getName());

    for (String col : columns) {
      FieldSchema field = new FieldSchema(col, STRING_TYPE_NAME, "default");
      tbl.getCols().add(field);
    }

    if (partCols != null) {
      for (String partCol : partCols) {
        FieldSchema part = new FieldSchema();
        part.setName(partCol);
        part.setType(STRING_TYPE_NAME); // default partition key
        tbl.getPartCols().add(part);
      }
    }
    tbl.setSerializationLib(LazySimpleSerDe.class.getName());
    tbl.setNumBuckets(bucketCount);
    tbl.setBucketCols(bucketCols);
    createTable(tbl);
  }

  /**
   * Updates the existing table metadata with the new metadata.
   *
   * @param tblName
   *          name of the existing table
   * @param newTbl
   *          new name of the table. could be the old name
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterTable(String tblName, Table newTbl)
      throws InvalidOperationException, HiveException {
    Table t = newTable(tblName);
    try {
      // Remove the DDL_TIME so it gets refreshed
      if (newTbl.getParameters() != null) {
        newTbl.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      getMSC().alter_table(t.getDbName(), t.getTableName(), newTbl.getTTable());
    } catch (MetaException e) {
      throw new HiveException("Unable to alter table.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter table.", e);
    }
  }

  /**
   * Updates the existing index metadata with the new metadata.
   *
   * @param idxName
   *          name of the existing index
   * @param newIdx
   *          new name of the index. could be the old name
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterIndex(String dbName, String baseTblName, String idxName, Index newIdx)
      throws InvalidOperationException, HiveException {
    try {
      getMSC().alter_index(dbName, baseTblName, idxName, newIdx);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter index.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter index.", e);
    }
  }

  /**
   * Updates the existing table metadata with the new metadata.
   *
   * @param tblName
   *          name of the existing table
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterPartition(String tblName, Partition newPart)
      throws InvalidOperationException, HiveException {
    Table t = newTable(tblName);
    try {
      // Remove the DDL time so that it gets refreshed
      if (newPart.getParameters() != null) {
        newPart.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      getMSC().alter_partition(t.getDbName(), t.getTableName(),
          newPart.getTPartition());

    } catch (MetaException e) {
      throw new HiveException("Unable to alter partition.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter partition.", e);
    }
  }

  /**
   * Updates the existing table metadata with the new metadata.
   *
   * @param tblName
   *          name of the existing table
   * @param newParts
   *          new partitions
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterPartitions(String tblName, List<Partition> newParts)
      throws InvalidOperationException, HiveException {
    Table t = newTable(tblName);
    List<org.apache.hadoop.hive.metastore.api.Partition> newTParts =
      new ArrayList<org.apache.hadoop.hive.metastore.api.Partition>();
    try {
      // Remove the DDL time so that it gets refreshed
      for (Partition tmpPart: newParts) {
        if (tmpPart.getParameters() != null) {
          tmpPart.getParameters().remove(hive_metastoreConstants.DDL_TIME);
        }
        newTParts.add(tmpPart.getTPartition());
      }
      getMSC().alter_partitions(t.getDbName(), t.getTableName(), newTParts);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter partition.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter partition.", e);
    }
  }
  /**
   * Rename a old partition to new partition
   *
   * @param tbl
   *          existing table
   * @param oldPartSpec
   *          spec of old partition
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void renamePartition(Table tbl, Map<String, String> oldPartSpec, Partition newPart)
      throws HiveException {
    try {
      Map<String, String> newPartSpec = newPart.getSpec();
      if (oldPartSpec.keySet().size() != tbl.getPartCols().size()
          || newPartSpec.keySet().size() != tbl.getPartCols().size()) {
        throw new HiveException("Unable to rename partition to the same name: number of partition cols don't match. ");
      }
      if (!oldPartSpec.keySet().equals(newPartSpec.keySet())){
        throw new HiveException("Unable to rename partition to the same name: old and new partition cols don't match. ");
      }
      List<String> pvals = new ArrayList<String>();

      for (FieldSchema field : tbl.getPartCols()) {
        String val = oldPartSpec.get(field.getName());
        if (val == null || val.length() == 0) {
          throw new HiveException("get partition: Value for key "
              + field.getName() + " is null or empty");
        } else if (val != null){
          pvals.add(val);
        }
      }
      getMSC().renamePartition(tbl.getDbName(), tbl.getTableName(), pvals,
          newPart.getTPartition());

    } catch (InvalidOperationException e){
      throw new HiveException("Unable to rename partition.", e);
    } catch (MetaException e) {
      throw new HiveException("Unable to rename partition.", e);
    } catch (TException e) {
      throw new HiveException("Unable to rename partition.", e);
    }
  }

  public void alterDatabase(String dbName, Database db)
      throws HiveException {
    try {
      getMSC().alterDatabase(dbName, db);
    } catch (MetaException e) {
      throw new HiveException("Unable to alter database " + dbName, e);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Database " + dbName + " does not exists.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter database " + dbName, e);
    }
  }
  /**
   * Creates the table with the give objects
   *
   * @param tbl
   *          a table object
   * @throws HiveException
   */
  public void createTable(Table tbl) throws HiveException {
    createTable(tbl, false);
  }

  /**
   * Creates the table with the give objects
   *
   * @param tbl
   *          a table object
   * @param ifNotExists
   *          if true, ignore AlreadyExistsException
   * @throws HiveException
   */
  public void createTable(Table tbl, boolean ifNotExists) throws HiveException {
    try {
      if (tbl.getDbName() == null || "".equals(tbl.getDbName().trim())) {
        tbl.setDbName(getCurrentDatabase());
      }
      if (tbl.getCols().size() == 0 && !tbl.isHeterView()) {
        tbl.setFields(MetaStoreUtils.getFieldsFromDeserializer(tbl.getTableName(),
            tbl.getDeserializer()));
      }
      tbl.checkValidity();
      if (tbl.getParameters() != null) {
        tbl.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      org.apache.hadoop.hive.metastore.api.Table tTbl = tbl.getTTable();
      PrincipalPrivilegeSet principalPrivs = new PrincipalPrivilegeSet();
      SessionState ss = SessionState.get();
      if (ss != null) {
        CreateTableAutomaticGrant grants = ss.getCreateTableGrants();
        if (grants != null) {
          principalPrivs.setUserPrivileges(grants.getUserGrants());
          principalPrivs.setGroupPrivileges(grants.getGroupGrants());
          principalPrivs.setRolePrivileges(grants.getRoleGrants());
          tTbl.setPrivileges(principalPrivs);
        }
      }

/*      // FIXME: add user info here
      String userName;

      if (ss != null) {
        userName = ss.getUser();
      } else {
        userName = conf.get(HiveConf.ConfVars.HIVE_USER.varname, "root");
      }
      User user = new User(userName, "invalid_passwd", System.currentTimeMillis(), "root");
      LOG.info("GOT USER: " + userName);
      getMSC().createTableByUser(tTbl, user);*/
      getMSC().createTable(tTbl);
    } catch (AlreadyExistsException e) {
      if (!ifNotExists) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   *
   * @param tableName
   *          table name
   * @param indexName
   *          index name
   * @param indexHandlerClass
   *          index handler class
   * @param indexedCols
   *          index columns
   * @param indexTblName
   *          index table's name
   * @param deferredRebuild
   *          referred build index table's data
   * @param inputFormat
   *          input format
   * @param outputFormat
   *          output format
   * @param serde
   * @param storageHandler
   *          index table's storage handler
   * @param location
   *          location
   * @param idxProps
   *          idx
   * @param serdeProps
   *          serde properties
   * @param collItemDelim
   * @param fieldDelim
   * @param fieldEscape
   * @param lineDelim
   * @param mapKeyDelim
   * @throws HiveException
   */
  public void createIndex(String tableName, String indexName, String indexHandlerClass,
      List<String> indexedCols, String indexTblName, boolean deferredRebuild,
      String inputFormat, String outputFormat, String serde,
      String storageHandler, String location,
      Map<String, String> idxProps, Map<String, String> tblProps, Map<String, String> serdeProps,
      String collItemDelim, String fieldDelim, String fieldEscape,
      String lineDelim, String mapKeyDelim, String indexComment)
      throws HiveException {

    try {
      String dbName = getCurrentDatabase();
      Index old_index = null;
      try {
        old_index = getIndex(dbName, tableName, indexName);
      } catch (Exception e) {
      }
      if (old_index != null) {
        throw new HiveException("Index " + indexName + " already exists on table " + tableName + ", db=" + dbName);
      }

      org.apache.hadoop.hive.metastore.api.Table baseTbl = getMSC().getTable(dbName, tableName);
      if (baseTbl.getTableType() == TableType.VIRTUAL_VIEW.toString()) {
        throw new HiveException("tableName="+ tableName +" is a VIRTUAL VIEW. Index on VIRTUAL VIEW is not supported.");
      }


      //removed by zjw , do not store index as an additional talbe
//      if (indexTblName == null) {
//        indexTblName = MetaStoreUtils.getIndexTableName(dbName, tableName, indexName);
//      } else {
//        org.apache.hadoop.hive.metastore.api.Table temp = null;
//        try {
//          temp = getMSC().getTable(dbName, indexTblName);
//        } catch (Exception e) {
//        }
//        if (temp != null) {
//          throw new HiveException("Table name " + indexTblName + " already exists. Choose another name.");
//        }
//      }

      org.apache.hadoop.hive.metastore.api.StorageDescriptor storageDescriptor = baseTbl.getSd().deepCopy();
      SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
      if(serde != null) {
        serdeInfo.setSerializationLib(serde);
      } else {
        if (storageHandler == null) {
          serdeInfo.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
        } else {
          HiveStorageHandler sh = HiveUtils.getStorageHandler(getConf(), storageHandler);
          String serDeClassName = sh.getSerDeClass().getName();
          serdeInfo.setSerializationLib(serDeClassName);
        }
      }

      if (fieldDelim != null) {
        serdeInfo.getParameters().put(FIELD_DELIM, fieldDelim);
        serdeInfo.getParameters().put(SERIALIZATION_FORMAT, fieldDelim);
      }
      if (fieldEscape != null) {
        serdeInfo.getParameters().put(ESCAPE_CHAR, fieldEscape);
      }
      if (collItemDelim != null) {
        serdeInfo.getParameters().put(COLLECTION_DELIM, collItemDelim);
      }
      if (mapKeyDelim != null) {
        serdeInfo.getParameters().put(MAPKEY_DELIM, mapKeyDelim);
      }
      if (lineDelim != null) {
        serdeInfo.getParameters().put(LINE_DELIM, lineDelim);
      }

      if (serdeProps != null) {
        Iterator<Entry<String, String>> iter = serdeProps.entrySet()
          .iterator();
        while (iter.hasNext()) {
          Entry<String, String> m = iter.next();
          serdeInfo.getParameters().put(m.getKey(), m.getValue());
        }
      }

      storageDescriptor.setLocation(null);
      if (location != null) {
        storageDescriptor.setLocation(location);
      }
      storageDescriptor.setInputFormat(inputFormat);
      storageDescriptor.setOutputFormat(outputFormat);

      Map<String, String> params = new HashMap<String,String>();

      List<FieldSchema> indexTblCols = new ArrayList<FieldSchema>();
      List<Order> sortCols = new ArrayList<Order>();
      storageDescriptor.setBucketCols(null);
      int k = 0;
      for (int i = 0; i < storageDescriptor.getCols().size(); i++) {
        FieldSchema col = storageDescriptor.getCols().get(i);
        if (indexedCols.contains(col.getName())) {
          indexTblCols.add(col);
          sortCols.add(new Order(col.getName(), 1));
          k++;
        }
      }
      if (k != indexedCols.size()) {
        throw new RuntimeException(
            "Check the index columns, they should appear in the table being indexed.");
      }

      storageDescriptor.setCols(indexTblCols);
      storageDescriptor.setSortCols(sortCols);

      int time = (int) (System.currentTimeMillis() / 1000);
      org.apache.hadoop.hive.metastore.api.Table tt = null;
    //removed by zjw , do not store index as an additional talbe

//      HiveIndexHandler indexHandler = HiveUtils.getIndexHandler(this.getConf(), indexHandlerClass);
//      if (indexHandler.usesIndexTable()) {
//        tt = new org.apache.hadoop.hive.ql.metadata.Table(dbName, indexTblName).getTTable();
//        List<FieldSchema> partKeys = baseTbl.getPartitionKeys();
//        tt.setPartitionKeys(partKeys);
//        tt.setTableType(TableType.INDEX_TABLE.toString());
//        if (tblProps != null) {
//          for (Entry<String, String> prop : tblProps.entrySet()) {
//            tt.putToParameters(prop.getKey(), prop.getValue());
//          }
//        }
//      }

      if(!deferredRebuild) {
        throw new RuntimeException("Please specify deferred rebuild using \" WITH DEFERRED REBUILD \".");
      }

      Index indexDesc = new Index(indexName, indexHandlerClass, dbName, tableName, time, time, indexTblName,
          storageDescriptor, params, deferredRebuild);
      if (indexComment != null) {
        indexDesc.getParameters().put("comment", indexComment);
      }

      if (idxProps != null)
      {
        indexDesc.getParameters().putAll(idxProps);
      }

//      indexHandler.analyzeIndexDefinition(baseTbl, indexDesc, tt);
      //added by zjw
      analyzeValidIndexDefinition(baseTbl, indexDesc, tt);

      this.getMSC().createIndex(indexDesc, tt);

    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private void analyzeValidIndexDefinition(org.apache.hadoop.hive.metastore.api.Table baseTable,
      org.apache.hadoop.hive.metastore.api.Index index,
      org.apache.hadoop.hive.metastore.api.Table indexTable)
      throws HiveException{
      if(IndexType.LUCENE_TABLE.getHandlerClsName()
          .equals(index.getIndexHandlerClass())){
        Map<String, String> params = index.getParameters();
        for(String key:params.keySet()){
          LOG.warn("---jzw--lucene.prop.name:"+key);
        }

        if( !params.containsKey(Constants.META_LUCENE_ANALYZE)
            || !params.containsKey(Constants.META_LUCENE_ANALYZE)
            || !params.containsKey(Constants.META_LUCENE_ANALYZE)){
          throw new HiveException("Lucene properties have to be setted.");
        }
      }
  }

  public Index getIndex(String qualifiedIndexName) throws HiveException {
    String[] names = getQualifiedNames(qualifiedIndexName);
    switch (names.length) {
    case 3:
      return getIndex(names[0], names[1], names[2]);
    case 2:
      return getIndex(getCurrentDatabase(), names[0], names[1]);
    default:
      throw new HiveException("Invalid index name:" + qualifiedIndexName);
    }
  }

  public Index getIndex(String baseTableName, String indexName) throws HiveException {
    Table t = newTable(baseTableName);
    return this.getIndex(t.getDbName(), t.getTableName(), indexName);
  }

  public Index getIndex(String dbName, String baseTableName,
      String indexName) throws HiveException {
    try {
      return this.getMSC().getIndex(dbName, baseTableName, indexName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean dropIndex(String db_name, String tbl_name, String index_name, boolean deleteData) throws HiveException {
    try {
      return getMSC().dropIndex(db_name, tbl_name, index_name, deleteData);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Partition or table doesn't exist.", e);
    } catch (Exception e) {
      throw new HiveException("Unknown error. Please check logs.", e);
    }
  }

  /**
   * Drops table along with the data in it. If the table doesn't exist then it
   * is a no-op
   *
   * @param tableName
   *          table to drop
   * @throws HiveException
   *           thrown if the drop fails
   */
  public void dropTable(String tableName) throws HiveException {
    Table t = newTable(tableName);
    dropTable(t.getDbName(), t.getTableName(), true, true);
  }

  /**
   * Drops table along with the data in it. If the table doesn't exist then it
   * is a no-op
   *
   * @param dbName
   *          database where the table lives
   * @param tableName
   *          table to drop
   * @throws HiveException
   *           thrown if the drop fails
   */
  public void dropTable(String dbName, String tableName) throws HiveException {
    dropTable(dbName, tableName, true, true);
  }

  /**
   * Drops the table.
   *
   * @param dbName
   * @param tableName
   * @param deleteData
   *          deletes the underlying data along with metadata
   * @param ignoreUnknownTab
   *          an exception if thrown if this is falser and table doesn't exist
   * @throws HiveException
   */
  public void dropTable(String dbName, String tableName, boolean deleteData,
      boolean ignoreUnknownTab) throws HiveException {

    try {
      getMSC().dropTable(dbName, tableName, deleteData, ignoreUnknownTab);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public HiveConf getConf() {
    return (conf);
  }

  /**
   * Returns metadata for the table named tableName
   * @param tableName the name of the table
   * @return the table metadata
   * @throws HiveException if there's an internal error or if the
   * table doesn't exist
   */
  public Table getTable(final String tableName) throws HiveException {
    Table t = newTable(tableName);
    LOG.info("---zjw--get table db:"+t.getDbName()+"--table:"+t.getTableName());
    return this.getTable(t.getDbName(), t.getTableName(), true);
  }

  /**
   * Returns metadata for the table named tableName
   * @param tableName the name of the table
   * @param throwException controls whether an exception is thrown or a returns a null
   * @return the table metadata
   * @throws HiveException if there's an internal error or if the
   * table doesn't exist
   */
  public Table getTable(final String tableName, boolean throwException) throws HiveException {
    Table t = newTable(tableName);
    return this.getTable(t.getDbName(), t.getTableName(), throwException);
  }

  /**
   * Returns metadata of the table
   *
   * @param dbName
   *          the name of the database
   * @param tableName
   *          the name of the table
   * @return the table
   * @exception HiveException
   *              if there's an internal error or if the table doesn't exist
   */
  public Table getTable(final String dbName, final String tableName) throws HiveException {
    if (tableName.contains(".")) {
      Table t = newTable(tableName);
      return this.getTable(t.getDbName(), t.getTableName(), true);
    } else {
      return this.getTable(dbName, tableName, true);
    }
  }

  public Table getTable(final String dcName,final String dbName, final String tableName) throws HiveException {
    if (tableName.contains(".")) {
      Table t = newTable(tableName);
      return this.getTable(t.getDbName(), t.getTableName(), true);
    } else {
      return this.getTable(dbName, tableName, true);
    }
  }


  /**
   * Returns metadata of the table
   *
   * @param dbName
   *          the name of the database
   * @param tableName
   *          the name of the table
   * @param throwException
   *          controls whether an exception is thrown or a returns a null
   * @return the table or if throwException is false a null value.
   * @throws HiveException
   */
  public Table getTable(final String dbName, final String tableName,
      boolean throwException) throws HiveException {

    if (tableName == null || tableName.equals("")) {
      throw new HiveException("empty table creation??");
    }

    // Get the table from metastore
    org.apache.hadoop.hive.metastore.api.Table tTable = null;
    try {
      String[] db_names = this.getQualifiedNames(dbName);
      if(db_names.length >1){
        tTable = getRemoteDbMSC(db_names[0]).getTable(db_names[1], tableName);
      }
      else{
        tTable = getMSC().getTable(dbName, tableName);
      }
    } catch (NoSuchObjectException e) {
      if (throwException) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidTableException("Table " + tableName + " not found ", tableName);
      }
      return null;
    } catch (Exception e) {
      throw new HiveException("Unable to fetch table " + tableName, e);
    }

    // For non-views, we need to do some extra fixes
    if (!TableType.VIRTUAL_VIEW.toString().equals(tTable.getTableType())) {
      // Fix the non-printable chars
      Map<String, String> parameters = tTable.getSd().getParameters();
      String sf = parameters.get(SERIALIZATION_FORMAT);
      if (sf != null) {
        char[] b = sf.toCharArray();
        if ((b.length == 1) && (b[0] < 10)) { // ^A, ^B, ^C, ^D, \t
          parameters.put(SERIALIZATION_FORMAT, Integer.toString(b[0]));
        }
      }

      // Use LazySimpleSerDe for MetadataTypedColumnsetSerDe.
      // NOTE: LazySimpleSerDe does not support tables with a single column of
      // col
      // of type "array<string>". This happens when the table is created using
      // an
      // earlier version of Hive.
      if (org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class
          .getName().equals(
            tTable.getSd().getSerdeInfo().getSerializationLib())
          && tTable.getSd().getColsSize() > 0
          && tTable.getSd().getCols().get(0).getType().indexOf('<') == -1) {
        tTable.getSd().getSerdeInfo().setSerializationLib(
            org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      }
    }

    Table table = new Table(tTable);

    table.checkValidity();
    return table;
  }

  public org.apache.hadoop.hive.ql.metadata.GlobalSchema getSchema(final String schemaName,
      boolean throwException) throws HiveException {

    if (schemaName == null || schemaName.equals("")) {
      throw new HiveException("empty table creation??");
    }

    // Get the table from metastore
    org.apache.hadoop.hive.metastore.api.GlobalSchema schema = null;
    try {

      schema = getMSC().getSchemaByName(schemaName);
    } catch (NoSuchObjectException e) {
      if (throwException) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidTableException("schema" + schemaName + " not found ", schemaName);
      }
      return null;
    } catch (Exception e) {
      throw new HiveException("Unable to fetch schema " + schemaName, e);
    }

    // For non-views, we need to do some extra fixes
    if (!TableType.VIRTUAL_VIEW.toString().equals(schema.getSchemaType())) {
      // Fix the non-printable chars
      Map<String, String> parameters = schema.getSd().getParameters();
      String sf = parameters.get(SERIALIZATION_FORMAT);
      if (sf != null) {
        char[] b = sf.toCharArray();
        if ((b.length == 1) && (b[0] < 10)) { // ^A, ^B, ^C, ^D, \t
          parameters.put(SERIALIZATION_FORMAT, Integer.toString(b[0]));
        }
      }

      // Use LazySimpleSerDe for MetadataTypedColumnsetSerDe.
      // NOTE: LazySimpleSerDe does not support tables with a single column of
      // col
      // of type "array<string>". This happens when the table is created using
      // an
      // earlier version of Hive.
      if (org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class
          .getName().equals(
            schema.getSd().getSerdeInfo().getSerializationLib())
          && schema.getSd().getColsSize() > 0
          && schema.getSd().getCols().get(0).getType().indexOf('<') == -1) {
        schema.getSd().getSerdeInfo().setSerializationLib(
            org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      }
    }

    org.apache.hadoop.hive.ql.metadata.GlobalSchema schema2 = new org.apache.hadoop.hive.ql.metadata.GlobalSchema(schema);

    schema2.checkValidity();
    return schema2;
  }

  /**
   * Get all table names for the current database.
   * @return List of table names
   * @throws HiveException
   */
  public List<String> getAllTables() throws HiveException {
    return getAllTables(getCurrentDatabase());
  }

  /**
   * Get all table names for the specified database.
   * @param dbName
   * @return List of table names
   * @throws HiveException
   */
  public List<String> getAllTables(String dbName) throws HiveException {
    return getTablesByPattern(dbName, ".*");
  }

  /**
   * Returns all existing tables from default database which match the given
   * pattern. The matching occurs as per Java regular expressions
   *
   * @param tablePattern
   *          java re pattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesByPattern(String tablePattern) throws HiveException {
    return getTablesByPattern(getCurrentDatabase(), tablePattern);
  }

  /**
   * Returns all existing tables from the specified database which match the given
   * pattern. The matching occurs as per Java regular expressions.
   * @param dbName
   * @param tablePattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesByPattern(String dbName, String tablePattern) throws HiveException {
    try {
    //added by zjw for dc.db
      String[] db_names = this.getQualifiedNames(dbName);
      if(db_names.length == 2){
        return getRemoteDbMSC(db_names[0]).getTables((db_names[1]), tablePattern);
      } else {
        return getMSC().getTables(dbName, tablePattern);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Returns all existing tables from the given database which match the given
   * pattern. The matching occurs as per Java regular expressions
   *
   * @param database
   *          the database name
   * @param tablePattern
   *          java re pattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesForDb(String database, String tablePattern)
      throws HiveException {
    try {
      return getMSC().getTables(database, tablePattern);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all existing database names.
   *
   * @return List of database names.
   * @throws HiveException
   */
  public List<String> getAllDatabases() throws HiveException {
    try {
      return getMSC().getAllDatabases();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all existing databases that match the given
   * pattern. The matching occurs as per Java regular expressions
   *
   * @param databasePattern
   *          java re pattern
   * @return list of database names
   * @throws HiveException
   */
  public List<String> getDatabasesByPattern(String databasePattern) throws HiveException {
    try {
    //added by zjw for dc.db
      String[] dc_names = this.getQualifiedNames(databasePattern);
      if(dc_names.length == 2){
        return getRemoteDbMSC(dc_names[0]).getDatabases(dc_names[1]) ;
      } else {
        return getMSC().getDatabases(databasePattern);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean grantPrivileges(PrivilegeBag privileges)
      throws HiveException {
    try {
      return getMSC().grant_privileges(privileges);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * @param privileges
   *          a bag of privileges
   * @return true on success
   * @throws HiveException
   */
  public boolean revokePrivileges(PrivilegeBag privileges)
      throws HiveException {
    try {
      return getMSC().revoke_privileges(privileges);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Query metadata to see if a database with the given name already exists.
   *
   * @param dbName
   * @return true if a database with the given name already exists, false if
   *         does not exist.
   * @throws HiveException
   */
  public boolean databaseExists(String dbName) throws HiveException {
    return getDatabase(dbName) != null;
  }

  /**
   * Get the database by name.
   * @param dbName the name of the database.
   * @return a Database object if this database exists, null otherwise.
   * @throws HiveException
   */
  public Database getDatabase(String dbName) throws HiveException {
    try {
      //added by zjw for dc.db
      String[] db_names = this.getQualifiedNames(dbName);
      if(db_names.length >1){
        return getRemoteDbMSC(db_names[0]).getDatabase(db_names[1]);
      } else {
        return getMSC().getDatabase(dbName);
      }
    } catch (NoSuchObjectException e) {
      return null;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Load a directory into a Hive Table Partition - Alters existing content of
   * the partition with the contents of loadPath. - If he partition does not
   * exist - one is created - files in loadPath are moved into Hive. But the
   * directory itself is not removed.
   *
   * @param loadPath
   *          Directory containing files to load into Table
   * @param tableName
   *          name of table to be loaded.
   * @param partSpec
   *          defines which partition needs to be loaded
   * @param replace
   *          if true - replace files in the partition, otherwise add files to
   *          the partition
   * @param holdDDLTime if true, force [re]create the partition
   * @param inheritTableSpecs if true, on [re]creating the partition, take the
   *          location/inputformat/outputformat/serde details from table spec
   */
  public void loadPartition(Path loadPath, String tableName,
      Map<String, String> partSpec, boolean replace, boolean holdDDLTime,
      boolean inheritTableSpecs)
      throws HiveException {
    Table tbl = getTable(tableName);
    try {
      /**
       * Move files before creating the partition since down stream processes
       * check for existence of partition in metadata before accessing the data.
       * If partition is created before data is moved, downstream waiting
       * processes might move forward with partial data
       */

      Partition oldPart = getPartition(tbl, partSpec, false);
      Path oldPartPath = null;
      if(oldPart != null) {
        oldPartPath = oldPart.getPartitionPath();
      }

      Path newPartPath = null;

      if (inheritTableSpecs) {
        Path partPath = new Path(tbl.getDataLocation().getPath(),
            Warehouse.makePartPath(partSpec));
        newPartPath = new Path(loadPath.toUri().getScheme(), loadPath.toUri().getAuthority(),
            partPath.toUri().getPath());

        if(oldPart != null) {
          /*
           * If we are moving the partition across filesystem boundaries
           * inherit from the table properties. Otherwise (same filesystem) use the
           * original partition location.
           *
           * See: HIVE-1707 and HIVE-2117 for background
           */
          FileSystem oldPartPathFS = oldPartPath.getFileSystem(getConf());
          FileSystem loadPathFS = loadPath.getFileSystem(getConf());
          if (oldPartPathFS.equals(loadPathFS)) {
            newPartPath = oldPartPath;
          }
        }
      } else {
        newPartPath = oldPartPath;
      }

      if (replace) {
        Hive.replaceFiles(loadPath, newPartPath, oldPartPath, getConf());
      } else {
        FileSystem fs = FileSystem.get(tbl.getDataLocation(), getConf());
        Hive.copyFiles(conf, loadPath, newPartPath, fs);
      }

      // recreate the partition if it existed before
      if (!holdDDLTime) {
        getPartition(tbl, partSpec, true, newPartPath.toString(), inheritTableSpecs);
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    } catch (MetaException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }

  }

  /**
   * Given a source directory name of the load path, load all dynamically generated partitions
   * into the specified table and return a list of strings that represent the dynamic partition
   * paths.
   * @param loadPath
   * @param tableName
   * @param partSpec
   * @param replace
   * @param numDP number of dynamic partitions
   * @param holdDDLTime
   * @return a list of strings with the dynamic partition paths
   * @throws HiveException
   */
  public ArrayList<LinkedHashMap<String, String>> loadDynamicPartitions(Path loadPath,
      String tableName, Map<String, String> partSpec, boolean replace,
      int numDP, boolean holdDDLTime)
      throws HiveException {

    Set<Path> validPartitions = new HashSet<Path>();
    try {
      ArrayList<LinkedHashMap<String, String>> fullPartSpecs =
        new ArrayList<LinkedHashMap<String, String>>();

      FileSystem fs = loadPath.getFileSystem(conf);
      FileStatus[] leafStatus = Utilities.getFileStatusRecurse(loadPath, numDP+1, fs);
      // Check for empty partitions
      for (FileStatus s : leafStatus) {
        // Check if the hadoop version supports sub-directories for tables/partitions
        if (s.isDir() &&
          !conf.getBoolVar(HiveConf.ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES)) {
          // No leaves in this directory
          LOG.info("NOT moving empty directory: " + s.getPath());
        } else {
          validPartitions.add(s.getPath().getParent());
        }
      }

      if (validPartitions.size() == 0) {
        LOG.warn("No partition is generated by dynamic partitioning");
      }

      if (validPartitions.size() > conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS)) {
        throw new HiveException("Number of dynamic partitions created is " + validPartitions.size()
            + ", which is more than "
            + conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS)
            +". To solve this try to set " + HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname
            + " to at least " + validPartitions.size() + '.');
      }

      // for each dynamically created DP directory, construct a full partition spec
      // and load the partition based on that
      Iterator<Path> iter = validPartitions.iterator();
      while (iter.hasNext()) {
        // get the dynamically created directory
        Path partPath = iter.next();
        assert fs.getFileStatus(partPath).isDir():
          "partitions " + partPath + " is not a directory !";

        // generate a full partition specification
        LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<String, String>(partSpec);
        Warehouse.makeSpecFromName(fullPartSpec, partPath);
        fullPartSpecs.add(fullPartSpec);

        // finally load the partition -- move the file to the final table address
        loadPartition(partPath, tableName, fullPartSpec, replace, holdDDLTime, true);
        LOG.info("New loading path = " + partPath + " with partSpec " + fullPartSpec);
      }
      return fullPartSpecs;
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Load a directory into a Hive Table. - Alters existing content of table with
   * the contents of loadPath. - If table does not exist - an exception is
   * thrown - files in loadPath are moved into Hive. But the directory itself is
   * not removed.
   *
   * @param loadPath
   *          Directory containing files to load into Table
   * @param tableName
   *          name of table to be loaded.
   * @param replace
   *          if true - replace files in the table, otherwise add files to table
   * @param holdDDLTime
   */
  public void loadTable(Path loadPath, String tableName, boolean replace,
      boolean holdDDLTime) throws HiveException {
    Table tbl = getTable(tableName);

    if (replace) {
      tbl.replaceFiles(loadPath);
    } else {
      tbl.copyFiles(loadPath);
    }

    if (!holdDDLTime) {
      try {
        alterTable(tableName, tbl);
      } catch (InvalidOperationException e) {
        throw new HiveException(e);
      }
    }
  }

 /**
   * Creates a partition.
   *
   * @param tbl
   *          table for which partition needs to be created
   * @param partSpec
   *          partition keys and their values
   * @return created partition object
   * @throws HiveException
   *           if table doesn't exist or partition already exists
   */
  public Partition createPartition(Table tbl, Map<String, String> partSpec)
      throws HiveException {
    return createPartition(tbl, null ,partSpec);
  }

  /**
   * Creates a partition.
   *
   * @param tbl
   *          table for which partition needs to be created
   * @param partSpec
   *          partition keys and their values
   * @return created partition object
   * @throws HiveException
   *           if table doesn't exist or partition already exists
   */
  public Partition createPartition(Table tbl, String partitionName ,Map<String, String> partSpec)
      throws HiveException {
    return createPartition(tbl, partitionName,partSpec, null, null, null, null, -1,
        null, null, null, null, null);
  }

  /**
   * Creates a partition
   *
   * @param tbl
   *          table for which partition needs to be created
   * @param partSpec
   *          partition keys and their values
   * @param location
   *          location of this partition
   * @param partParams
   *          partition parameters
   * @param inputFormat the inputformat class
   * @param outputFormat the outputformat class
   * @param numBuckets the number of buckets
   * @param cols the column schema
   * @param serializationLib the serde class
   * @param serdeParams the serde parameters
   * @param bucketCols the bucketing columns
   * @param sortCols sort columns and order
   *
   * @return created partition object
   * @throws HiveException
   *           if table doesn't exist or partition already exists
   */
  public Partition createPartition(Table tbl, String partitionName, Map<String, String> partSpec,
      Path location, Map<String, String> partParams, String inputFormat, String outputFormat,
      int numBuckets, List<FieldSchema> cols,
      String serializationLib, Map<String, String> serdeParams,
      List<String> bucketCols, List<Order> sortCols) throws HiveException {

    org.apache.hadoop.hive.metastore.api.Partition partition = null;

//    for (FieldSchema field : tbl.getPartCols()) {
//      String val = partSpec.get(field.getName());
//      if (val == null || val.length() == 0) {
//        throw new HiveException("add partition: Value for key "
//            + field.getName() + " is null or empty");
//      }
//    }

    try {
      Partition tmpPart = new Partition(tbl, partSpec, location);
      // No need to clear DDL_TIME in parameters since we know it's
      // not populated on construction.
      org.apache.hadoop.hive.metastore.api.Partition inPart
        = tmpPart.getTPartition();
      if(partitionName != null){
        inPart.setPartitionName(partitionName);
      }

      if (partParams != null) {
        inPart.setParameters(partParams);
      }
      if (inputFormat != null) {
        inPart.getSd().setInputFormat(inputFormat);
      }
      if (outputFormat != null) {
        inPart.getSd().setOutputFormat(outputFormat);
      }
      if (numBuckets != -1) {
        inPart.getSd().setNumBuckets(numBuckets);
      }
      if (cols != null) {
        inPart.getSd().setCols(cols);
      }
      if (serializationLib != null) {
          inPart.getSd().getSerdeInfo().setSerializationLib(serializationLib);
      }
      if (serdeParams != null) {
        inPart.getSd().getSerdeInfo().setParameters(serdeParams);
      }
      if (bucketCols != null) {
        inPart.getSd().setBucketCols(bucketCols);
      }
      if (sortCols != null) {
        inPart.getSd().setSortCols(sortCols);
      }
      LOG.warn("---zjw-- before hive add_partition.");
      partition = getMSC().add_partition(inPart);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }

    return new Partition(tbl, partition);
  }

  public Partition getPartition(Table tbl, Map<String, String> partSpec,
      boolean forceCreate) throws HiveException {
    return getPartition(tbl, partSpec, forceCreate, null, true);
  }

  /**
   * Returns partition metadata
   *
   * @param tbl
   *          the partition's table
   * @param partSpec
   *          partition keys and values
   * @param forceCreate
   *          if this is true and partition doesn't exist then a partition is
   *          created
   * @param partPath the path where the partition data is located
   * @param inheritTableSpecs whether to copy over the table specs for if/of/serde
   * @return result partition object or null if there is no partition
   * @throws HiveException
   */
  public Partition getPartition(Table tbl, Map<String, String> partSpec,
      boolean forceCreate, String partPath, boolean inheritTableSpecs) throws HiveException {
    if (!tbl.isValidSpec(partSpec)) {
      throw new HiveException("Invalid partition: " + partSpec);
    }
    List<String> pvals = new ArrayList<String>();
//    for (FieldSchema field : tbl.getPartCols()) {
//      String val = partSpec.get(field.getName());
//      // enable dynamic partitioning
//      if (val == null && !HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING)
//          || val.length() == 0) {
//        throw new HiveException("get partition: Value for key "
//            + field.getName() + " is null or empty");
//      } else if (val != null){
//        pvals.add(val);
//      }
//    }

    //modified by zjw
    for (String val : partSpec.values()) {
        pvals.add(val);
    }

    org.apache.hadoop.hive.metastore.api.Partition tpart = null;
    try {
      tpart = getMSC().getPartitionWithAuthInfo(tbl.getDbName(),
          tbl.getTableName(), pvals, getUserName(), getGroupNames());
    } catch (NoSuchObjectException nsoe) {
      // this means no partition exists for the given partition
      // key value pairs - thrift cannot handle null return values, hence
      // getPartition() throws NoSuchObjectException to indicate null partition
      tpart = null;
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    try {
      if (forceCreate) {
        if (tpart == null) {
          LOG.debug("creating partition for table " + tbl.getTableName()
                    + " with partition spec : " + partSpec);
          tpart = getMSC().appendPartition(tbl.getDbName(), tbl.getTableName(), pvals);
        }
        else {
          LOG.debug("altering partition for table " + tbl.getTableName()
                    + " with partition spec : " + partSpec);
          if (inheritTableSpecs) {
            tpart.getSd().setOutputFormat(tbl.getTTable().getSd().getOutputFormat());
            tpart.getSd().setInputFormat(tbl.getTTable().getSd().getInputFormat());
            tpart.getSd().getSerdeInfo().setSerializationLib(tbl.getSerializationLib());
            tpart.getSd().getSerdeInfo().setParameters(
                tbl.getTTable().getSd().getSerdeInfo().getParameters());
            tpart.getSd().setBucketCols(tbl.getBucketCols());
            tpart.getSd().setNumBuckets(tbl.getNumBuckets());
            tpart.getSd().setSortCols(tbl.getSortCols());
          }
          if (partPath == null || partPath.trim().equals("")) {
            throw new HiveException("new partition path should not be null or empty.");
          }
          tpart.getSd().setLocation(partPath);
          String fullName = tbl.getTableName();
          if (!org.apache.commons.lang.StringUtils.isEmpty(tbl.getDbName())) {
            fullName = tbl.getDbName() + "." + tbl.getTableName();
          }
          alterPartition(fullName, new Partition(tbl, tpart));
        }
      }
      if (tpart == null) {
        return null;
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return new Partition(tbl, tpart);
  }

  public boolean dropPartition(String tblName, List<String> part_vals, boolean deleteData)
  throws HiveException {
    Table t = newTable(tblName);
    return dropPartition(t.getDbName(), t.getTableName(), part_vals, deleteData);
  }

  public boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws HiveException {
    try {
      return getMSC().dropPartition(db_name, tbl_name, part_vals, deleteData);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Partition or table doesn't exist.", e);
    } catch (Exception e) {
      throw new HiveException("Unknown error. Please check logs.", e);
    }
  }

  public List<String> getPartitionNames(String tblName, short max) throws HiveException {
    Table t = newTable(tblName);
    return getPartitionNames(t.getDbName(), t.getTableName(), max);
  }

  public List<String> getPartitionNames(String dbName, String tblName, short max)
      throws HiveException {
    List<String> names = null;
    try {
      names = getMSC().listPartitionNames(dbName, tblName, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return names;
  }

  public List<String> getPartitionNames(String dbName, String tblName,
      Map<String, String> partSpec, short max) throws HiveException {
    List<String> names = null;
    Table t = getTable(dbName, tblName);

    List<String> pvals = getPvals(t.getPartCols(), partSpec);

    try {
      names = getMSC().listPartitionNames(dbName, tblName, pvals, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return names;
  }

  /**
   * get all the partitions that the table has
   *
   * @param tbl
   *          object for which partition is needed
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl) throws HiveException {
    if (tbl.isPartitioned()) {
      List<org.apache.hadoop.hive.metastore.api.Partition> tParts;
      try {
        tParts = getMSC().listPartitionsWithAuthInfo(tbl.getDbName(), tbl.getTableName(),
            (short) -1, getUserName(), getGroupNames());
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new HiveException(e);
      }
      List<Partition> parts = new ArrayList<Partition>(tParts.size());
      for (org.apache.hadoop.hive.metastore.api.Partition tpart : tParts) {
        parts.add(new Partition(tbl, tpart));
      }
      return parts;
    } else {
      Partition part = new Partition(tbl);
      ArrayList<Partition> parts = new ArrayList<Partition>(1);
      parts.add(part);
      return parts;
    }
  }

  private static List<String> getPvals(List<FieldSchema> partCols,
      Map<String, String> partSpec) {
    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : partCols) {
      String val = partSpec.get(field.getName());
      if (val == null) {
        val = "";
      }
      pvals.add(val);
    }
    return pvals;
  }

  /**
   * get all the partitions of the table that matches the given partial
   * specification. partition columns whose value is can be anything should be
   * an empty string.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @param limit number of partitions to return
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl, Map<String, String> partialPartSpec,
      short limit)
  throws HiveException {
    if (!tbl.isPartitioned()) {
      throw new HiveException("Partition spec should only be supplied for a " +
          "partitioned table");
    }

    List<String> partialPvals = getPvals(tbl.getPartCols(), partialPartSpec);

    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = null;
    try {
      partitions = getMSC().listPartitionsWithAuthInfo(tbl.getDbName(), tbl.getTableName(),
          partialPvals, limit, getUserName(), getGroupNames());
    } catch (Exception e) {
      throw new HiveException(e);
    }

    List<Partition> qlPartitions = new ArrayList<Partition>();
    for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
      qlPartitions.add( new Partition(tbl, p));
    }

    return qlPartitions;
  }

  /**
   * get all the partitions of the table that matches the given partial
   * specification. partition columns whose value is can be anything should be
   * an empty string.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl, Map<String, String> partialPartSpec)
  throws HiveException {
    return getPartitions(tbl, partialPartSpec, (short)-1);
  }

  /**
   * get all the partitions of the table that matches the given partial
   * specification. partition columns whose value is can be anything should be
   * an empty string.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @param partialPartSpec
   *          partial partition specification (some subpartitions can be empty).
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitionsByNames(Table tbl,
      Map<String, String> partialPartSpec)
      throws HiveException {

    if (!tbl.isPartitioned()) {
      throw new HiveException("Partition spec should only be supplied for a " +
                "partitioned table");
    }

    List<String> names = getPartitionNames(tbl.getDbName(), tbl.getTableName(),
        partialPartSpec, (short)-1);

    List<Partition> partitions = getPartitionsByNames(tbl, names);
    return partitions;
  }

  /**
   * Get all partitions of the table that matches the list of given partition names.
   *
   * @param tbl
   *          object for which partition is needed. Must be partitioned.
   * @param partNames
   *          list of partition names
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitionsByNames(Table tbl, List<String> partNames)
      throws HiveException {

    if (!tbl.isPartitioned()) {
      throw new HiveException("Partition spec should only be supplied for a " +
          "partitioned table");
    }
    List<Partition> partitions = new ArrayList<Partition>(partNames.size());

    int batchSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_MAX);
    int nParts = partNames.size();
    int nBatches = nParts / batchSize;

    try {
      for (int i = 0; i < nBatches; ++i) {
        List<org.apache.hadoop.hive.metastore.api.Partition> tParts =
          getMSC().getPartitionsByNames(tbl.getDbName(), tbl.getTableName(),
          partNames.subList(i*batchSize, (i+1)*batchSize));
        if (tParts != null) {
          for (org.apache.hadoop.hive.metastore.api.Partition tpart: tParts) {
            partitions.add(new Partition(tbl, tpart));
          }
        }
      }

      if (nParts > nBatches * batchSize) {
        List<org.apache.hadoop.hive.metastore.api.Partition> tParts =
          getMSC().getPartitionsByNames(tbl.getDbName(), tbl.getTableName(),
          partNames.subList(nBatches*batchSize, nParts));
        if (tParts != null) {
          for (org.apache.hadoop.hive.metastore.api.Partition tpart: tParts) {
            partitions.add(new Partition(tbl, tpart));
          }
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return partitions;
  }

  /**
   * Get a list of Partitions by filter.
   * @param tbl The table containing the partitions.
   * @param filter A string represent partition predicates.
   * @return a list of partitions satisfying the partition predicates.
   * @throws HiveException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  public List<Partition> getPartitionsByFilter(Table tbl, String filter)
      throws HiveException, MetaException, NoSuchObjectException, TException {

    if (!tbl.isPartitioned()) {
      throw new HiveException("Partition spec should only be supplied for a " +
          "partitioned table");
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> tParts = getMSC().listPartitionsByFilter(
        tbl.getDbName(), tbl.getTableName(), filter, (short)-1);
    List<Partition> results = new ArrayList<Partition>(tParts.size());

    for (org.apache.hadoop.hive.metastore.api.Partition tPart: tParts) {
      Partition part = new Partition(tbl, tPart);
      results.add(part);
    }
    return results;
  }

  /**
   * Get the name of the current database
   * @return the current database name
   */
  public String getCurrentDatabase() {
    if (null == currentDatabase) {
      currentDatabase = DEFAULT_DATABASE_NAME;
    }
    return currentDatabase;
  }

  /**
   * Set the name of the current database
   * @param currentDatabase
   */
  public void setCurrentDatabase(String currentDatabase) {
    this.currentDatabase = currentDatabase;
  }

  //added by liulichao for authentication, begin
  public int createUser(String userName, String passwd, String ownerName)
      throws HiveException {
    int ret = -100;

    try {
      userName = userName.trim();
      ownerName= ownerName.trim();
      LOG.info("username-passed:"+userName);
      LOG.info("ownername-passed:"+ownerName);
      if (!ownerName.equalsIgnoreCase(rootName)) {
        LOG.info("User " + ownerName + " has no privilege to create user.");
        return -1;
      }
//      if (userName.equalsIgnoreCase(rootName) ) {  //the userName can't be root.
////        throw new MetaException("User " + userName
////            + " is root user, please change the name.");
//        LOG.info("User " + userName + " already exists.");
//        return -2;
//      }

      if(userName.length()>16 || userName.length()<1) {
//        throw new HiveException("Length of the new user's name must be between 1 and 16 !");
        LOG.info("Length of the new user's name must be between 1 and 16 !");
        return -3;
      }
      if(passwd.length()>41 || passwd.length()<8) {//passwd with '' outside
//        throw new HiveException("Length of the passwd must be between 6 and 41 !");
        LOG.info("Length of the passwd must be between 6 and 41 !");
        return -4;
      }

      int now = (int)(System.currentTimeMillis()/1000);
      boolean success = false;
      success = getMSC().create_user(new User(userName, passwd, now, ownerName));
      if (success) {
        ret = 0;
      } else {
        LOG.info("User " + userName + " already exists.");
        ret = -2;
      }

      return ret;
    } catch (Exception e) {
//      throw new HiveException(e);
      LOG.info("Create user error!"+e.getMessage());
      return -5;
    }
  }

  public int dropUser(String userName) throws HiveException {
    int ret = -100;

    try {
      //privilege check here...
      SessionState ss = SessionState.get();
      String ownerName = ss.getUser();
      LOG.info(ownerName);
      if (!ownerName.equalsIgnoreCase(rootName) && userName.equalsIgnoreCase(rootName)) {
        //LOG.debug("没有删除用户的权限！");
        LOG.debug("Not root, no priv to drop users.！");
        return -1;
      }

      boolean success = false;
      success = getMSC().drop_user(userName);
      if (success) {
        ret = 0;
      } else {
        LOG.info("user doesnt exist！");
        ret = -2;
      }

      return ret;
    } catch (Exception e) {
      //throw new HiveException(e);
      LOG.info("drop user error！");
      return -3;
    }
  }

  public int setPasswd(String userName, String passwd) throws HiveException {
    int ret = -100;

    try {
      //privilege check here...
      SessionState ss = SessionState.get();
      String localName = ss.getAuthenticator().getUserName();
      if (!userName.equals(localName) && !localName.equalsIgnoreCase(rootName)) {
        //LOG.debug("没有修改其他用户密码的权限！");
        LOG.info("Not root, has no priv to change password for other users. ！");
        //throw new NoSuchObjectException("没有修改其他用户密码的权限！");
        return -1;
      }

      if(passwd.length()>41 || passwd.length()<8) {//passwd with '' outside
        //throw new HiveException("Length of the passwd must be between 6 and 41 !");
        LOG.info("Length of the passwd must be between 6 and 41 !");
        return -2;
      }

      boolean success = false;
      User u = new User();
      u.setUserName(userName);
      u.setPassword(passwd);

      success = getMSC().modify_user(u);
      if (success) {
        ret = 0;
      } else {
        LOG.info("user doesnt exist！");
        ret = -3;
      }

      return ret;
    } catch (Exception e) {
      //throw new HiveException(e);
      LOG.info("setpasswd error！");
      return -4;
    }
  }

  public int authUser(String userName, String passwd) throws HiveException {
    int ret = -100;
    try {
      LOG.debug("user:"+userName+",pwd:"+passwd);

//      if (userName.equals("root")) {
//        MUser nameCheck = getMSC().getMUser(userName);//IMetaStorClient.java
//        if (nameCheck == null ) {
//          long now = System.currentTimeMillis() / 1000;//change .thrift, User(string,string, long, string)//liulichao
//          User user = new User("root", passwd, now, "root");
//          getMSC().create_user(user);
//        }
//      }
//      if (userName.equalsIgnoreCase(rootName)) {
//        if (passwd.equals("'"+rootPWD+"'")) {
//          return 1;
//        } else {
//          LOG.debug("user or pwd error！");
//          return -1;
//        }
//      }

      boolean success = false;
      success = getMSC().authentication(userName, passwd.substring(1, passwd.lastIndexOf("'")));
      if (success) {
        ret = 0;
      } else {
        LOG.debug("user or pwd error！");
        ret = -1;
      }

      if (ret >= 0 ) {
        SessionState ss = SessionState.get();
        //ss.setUser("root");     //added by liulichao, authenticated user, set it in SessionState.
        ss.setUser(userName);
      }

      return ret;
    } catch (Exception e) {
//      throw new HiveException(e);
      LOG.info("authentication error!"+e.getMessage());//?
      return -2;
    }
  }

  public List<String> listUserNames() throws HiveException {
    try {
      SessionState ss = SessionState.get();
      String localName = ss.getAuthenticator().getUserName();
      LOG.debug("localName:"+localName);
      if (!localName.equalsIgnoreCase(rootName)) {
          LOG.debug("Not root, has no priv to list userNames!");
          return new ArrayList<String>(0);
      }

      List<String> ret = getMSC().list_users_names();
      for (int i = 0; i< ret.size(); i++) {
        LOG.info("user(i)=" + ret.get(i));
      }

      return ret;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.debug("list userNames error:"+e.getMessage().toString());
      return new ArrayList<String>(0);
    }
  }

  //added by liulichao for authentication, end

  public void createRole(String roleName, String ownerName)
      throws HiveException {
    try {
      getMSC().create_role(new Role(roleName, -1, ownerName));
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropRole(String roleName) throws HiveException {
    try {
      getMSC().drop_role(roleName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Get all existing role names.
   *
   * @return List of role names.
   * @throws HiveException
   */
  public List<String> getAllRoleNames() throws HiveException {
    try {
      return getMSC().listRoleNames();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<Role> showRoleGrant(String principalName, PrincipalType principalType) throws HiveException {
    try {
      return getMSC().list_roles(principalName, principalType);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean grantRole(String roleName, String userName,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws HiveException {
    try {
      return getMSC().grant_role(roleName, userName, principalType, grantor,
          grantorType, grantOption);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean revokeRole(String roleName, String userName,
      PrincipalType principalType)  throws HiveException {
    try {
      return getMSC().revoke_role(roleName, userName, principalType);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<Role> listRoles(String userName,  PrincipalType principalType)
      throws HiveException {
    try {
      return getMSC().list_roles(userName, principalType);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * @param objectType
   *          hive object type
   * @param db_name
   *          database name
   * @param table_name
   *          table name
   * @param part_values
   *          partition values
   * @param column_name
   *          column name
   * @param user_name
   *          user name
   * @param group_names
   *          group names
   * @return the privilege set
   * @throws HiveException
   */
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectType objectType,
      String db_name, String table_name, List<String> part_values,
      String column_name, String user_name, List<String> group_names)
      throws HiveException {
    try {
      HiveObjectRef hiveObj = new HiveObjectRef(objectType, db_name,
          table_name, part_values, column_name);
      return getMSC().get_privilege_set(hiveObj, user_name, group_names);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * @param objectType
   *          hive object type
   * @param principalName
   * @param principalType
   * @param dbName
   * @param tableName
   * @param partValues
   * @param columnName
   * @return list of privileges
   * @throws HiveException
   */
  public List<HiveObjectPrivilege> showPrivilegeGrant(
      HiveObjectType objectType, String principalName,
      PrincipalType principalType, String dbName, String tableName,
      List<String> partValues, String columnName) throws HiveException {
    try {
      HiveObjectRef hiveObj = new HiveObjectRef(objectType, dbName, tableName,
          partValues, columnName);
      return getMSC().list_privileges(principalName, principalType, hiveObj);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  static private void checkPaths(HiveConf conf,
    FileSystem fs, FileStatus[] srcs, Path destf,
    boolean replace) throws HiveException {
    try {
      for (FileStatus src : srcs) {
        FileStatus[] items = fs.listStatus(src.getPath());
        for (FileStatus item : items) {
          Path itemStaging = item.getPath();

          if (Utilities.isTempPath(item)) {
            // This check is redundant because temp files are removed by
            // execution layer before
            // calling loadTable/Partition. But leaving it in just in case.
            fs.delete(itemStaging, true);
            continue;
          }

          if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES) &&
            item.isDir()) {
            throw new HiveException("checkPaths: " + src.getPath()
                + " has nested directory" + itemStaging);
          }
          if (!replace) {
            // It's possible that the file we're copying may have the same
            // relative name as an existing file in the "destf" directory.
            // So let's make a quick check to see if we can rename any
            // potential offenders so as to allow them to move into the
            // "destf" directory. The scheme is dead simple: simply tack
            // on "_copy_N" where N starts at 1 and works its way up until
            // we find a free space.

            // Note: there are race conditions here, but I don't believe
            // they're worse than what was already present.
            int counter = 1;

            // Strip off the file type, if any so we don't make:
            // 000000_0.gz -> 000000_0.gz_copy_1
            String name = itemStaging.getName();
            String filetype;
            int index = name.lastIndexOf('.');
            if (index >= 0) {
              filetype = name.substring(index);
              name = name.substring(0, index);
            } else {
              filetype = "";
            }

            Path itemDest = new Path(destf, itemStaging.getName());
            Path itemStagingBase = new Path(itemStaging.getParent(), name);

            while (fs.exists(itemDest)) {
              Path proposedStaging = itemStagingBase
                  .suffix("_copy_" + counter++).suffix(filetype);
              Path proposedDest = new Path(destf, proposedStaging.getName());

              if (fs.exists(proposedDest)) {
                // There's already a file in our destination directory with our
                // _copy_N suffix. We've been here before...
                LOG.trace(proposedDest + " already exists");
                continue;
              }

              if (!fs.rename(itemStaging, proposedStaging)) {
                LOG.debug("Unsuccessfully in attempt to rename " + itemStaging + " to " + proposedStaging + "...");
                continue;
              }

              LOG.debug("Successfully renamed " + itemStaging + " to " + proposedStaging);
              itemDest = proposedDest;
            }
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException("checkPaths: filesystem error in check phase", e);
    }
  }

  static protected void copyFiles(HiveConf conf, Path srcf, Path destf, FileSystem fs)
      throws HiveException {
    try {
      // create the destination if it does not exist
      if (!fs.exists(destf)) {
        fs.mkdirs(destf);
      }
    } catch (IOException e) {
      throw new HiveException(
          "copyFiles: error while checking/creating destination directory!!!",
          e);
    }

    FileStatus[] srcs;
    try {
      srcs = fs.globStatus(srcf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
    if (srcs == null) {
      LOG.info("No sources specified to move: " + srcf);
      return;
      // srcs = new FileStatus[0]; Why is this needed?
    }
    // check that source and target paths exist
    checkPaths(conf, fs, srcs, destf, false);

    // move it, move it
    try {
      for (FileStatus src : srcs) {
        FileStatus[] items = fs.listStatus(src.getPath());
        for (FileStatus item : items) {
          Path source = item.getPath();
          Path target = new Path(destf, item.getPath().getName());
          if (!fs.rename(source, target)) {
            throw new IOException("Cannot move " + source + " to " + target);
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException("copyFiles: error while moving files!!!", e);
    }
  }

  /**
   * Replaces files in the partition with new data set specified by srcf. Works
   * by renaming directory of srcf to the destination file.
   * srcf, destf, and tmppath should resident in the same DFS, but the oldPath can be in a
   * different DFS.
   *
   * @param srcf
   *          Source directory to be renamed to tmppath. It should be a
   *          leaf directory where the final data files reside. However it
   *          could potentially contain subdirectories as well.
   * @param destf
   *          The directory where the final data needs to go
   * @param oldPath
   *          The directory where the old data location, need to be cleaned up.
   */
  static protected void replaceFiles(Path srcf, Path destf, Path oldPath,
      HiveConf conf) throws HiveException {

    try {
      FileSystem fs = srcf.getFileSystem(conf);

      // check if srcf contains nested sub-directories
      FileStatus[] srcs;
      try {
        srcs = fs.globStatus(srcf);
      } catch (IOException e) {
        throw new HiveException("Getting globStatus " + srcf.toString(), e);
      }
      if (srcs == null) {
        LOG.info("No sources specified to move: " + srcf);
        return;
      }
      checkPaths(conf, fs, srcs, destf, true);

      // point of no return -- delete oldPath
      if (oldPath != null) {
        try {
          FileSystem fs2 = oldPath.getFileSystem(conf);
          if (fs2.exists(oldPath)) {
            // use FsShell to move data to .Trash first rather than delete permanently
            FsShell fshell = new FsShell();
            fshell.setConf(conf);
            fshell.run(new String[]{"-rmr", oldPath.toString()});
          }
        } catch (Exception e) {
          //swallow the exception
          LOG.warn("Directory " + oldPath.toString() + " canot be removed.");
        }
      }

      // rename src directory to destf
      if (srcs.length == 1 && srcs[0].isDir()) {
        // rename can fail if the parent doesn't exist
        if (!fs.exists(destf.getParent())) {
          fs.mkdirs(destf.getParent());
        }
        if (fs.exists(destf)) {
          fs.delete(destf, true);
        }

        boolean b = fs.rename(srcs[0].getPath(), destf);
        if (!b) {
          throw new HiveException("Unable to move results from " + srcs[0].getPath()
              + " to destination directory: " + destf);
        }
        LOG.debug("Renaming:" + srcf.toString() + " to " + destf.toString()  + ",Status:" + b);
      } else { // srcf is a file or pattern containing wildcards
        if (!fs.exists(destf)) {
          fs.mkdirs(destf);
        }
        // srcs must be a list of files -- ensured by LoadSemanticAnalyzer
        for (FileStatus src : srcs) {
          Path destPath = new Path(destf, src.getPath().getName());
          if (!fs.rename(src.getPath(), destPath)) {
            throw new HiveException("Error moving: " + src.getPath()
                + " into: " + destf);
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

  /**
   * Creates a metastore client. Currently it creates only JDBC based client as
   * File based store support is removed
   *
   * @returns a Meta Store Client
   * @throws HiveMetaException
   *           if a working client can't be created
   */
  private IMetaStoreClient createMetaStoreClient() throws MetaException {

    HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
        public HiveMetaHook getHook(
          org.apache.hadoop.hive.metastore.api.Table tbl)
          throws MetaException {

          try {
            if (tbl == null) {
              return null;
            }
            HiveStorageHandler storageHandler =
              HiveUtils.getStorageHandler(conf,
                tbl.getParameters().get(META_TABLE_STORAGE));
            if (storageHandler == null) {
              return null;
            }
            return storageHandler.getMetaHook();
          } catch (HiveException ex) {
            LOG.error(StringUtils.stringifyException(ex));
            throw new MetaException(
              "Failed to load storage handler:  " + ex.getMessage());
          }
        }
      };
    return RetryingMetaStoreClient.getProxy(conf, hookLoader,
        HiveMetaStoreClient.class.getName());
  }

  /**
   *
   * @return the metastore client for the current thread
   * @throws MetaException
   */
  private IMetaStoreClient getMSC() throws MetaException {
    if (metaStoreClient == null) {
      metaStoreClient = createMetaStoreClient();
      // do authentication here
      String user_name = conf.getVar(HiveConf.ConfVars.HIVE_USER);
      String passwd = conf.getVar(HiveConf.ConfVars.HIVE_USERPWD);
      try {
        metaStoreClient.authentication(user_name, passwd);
      } catch (NoSuchObjectException e) {
        e.printStackTrace();
        throw new MetaException(e.getMessage());
      } catch (TException e) {
        e.printStackTrace();
        throw new MetaException(e.getMessage());
      }
    }
    return metaStoreClient;
  }

  private IMetaStoreClient getRemoteDbMSC(String db_name) throws HiveException {
    IMetaStoreClient r = null;

    try{
      Database db = getMSC().get_attribution(db_name);
      if(db == null){
        throw new MetaException("There is no Attribution named:"+db_name+",or top attribution is not reachable.");
      }
      r = getMSC().getRemoteDbMSC(db_name);
      // do authentication here
      String user_name = conf.getVar(HiveConf.ConfVars.HIVE_USER);
      String passwd = conf.getVar(HiveConf.ConfVars.HIVE_USERPWD);
      try {
        r.authentication(user_name, passwd);
      } catch (NoSuchObjectException e) {
        e.printStackTrace();
        throw new MetaException(e.getMessage());
      } catch (TException e) {
        e.printStackTrace();
        throw new MetaException(e.getMessage());
      }
    } catch (TException e) {
      throw new HiveException("Unable to alter getRemoteDcMSC,metastore service unreachable.", e);
    }
    return r;
  }

  private String getUserName() {
    SessionState ss = SessionState.get();
    if (ss != null && ss.getAuthenticator() != null) {
      return ss.getAuthenticator().getUserName();
    }
    return null;
  }

  private List<String> getGroupNames() {
    SessionState ss = SessionState.get();
    if (ss != null && ss.getAuthenticator() != null) {
      return ss.getAuthenticator().getGroupNames();
    }
    return null;
  }

  public static List<FieldSchema> getFieldsFromDeserializer(String name,
      Deserializer serde) throws HiveException {
    try {
      return MetaStoreUtils.getFieldsFromDeserializer(name, serde);
    } catch (SerDeException e) {
      throw new HiveException("Error in getting fields from serde. "
          + e.getMessage(), e);
    } catch (MetaException e) {
      throw new HiveException("Error in getting fields from serde."
          + e.getMessage(), e);
    }
  }

  public List<Index> getIndexes(String dbName, String tblName, short max) throws HiveException {
    List<Index> indexes = null;
    try {
      indexes = getMSC().listIndexes(dbName, tblName, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return indexes;
  }

  public boolean updateTableColumnStatistics(ColumnStatistics statsObj) throws HiveException {
    try {
      return getMSC().updateTableColumnStatistics(statsObj);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj) throws HiveException {
    try {
      return getMSC().updatePartitionColumnStatistics(statsObj);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName, String colName)
    throws HiveException {
    try {
      return getMSC().getTableColumnStatistics(dbName, tableName, colName);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }

  }

  public ColumnStatistics getPartitionColumnStatistics(String dbName, String tableName,
    String partName, String colName) throws HiveException {
      try {
        return getMSC().getPartitionColumnStatistics(dbName, tableName, partName, colName);
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new HiveException(e);
      }
    }

  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
    throws HiveException {
    try {
      return getMSC().deleteTableColumnStatistics(dbName, tableName, colName);
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
  }

  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
    String colName) throws HiveException {
      try {
        return getMSC().deletePartitionColumnStatistics(dbName, tableName, partName, colName);
      } catch(Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new HiveException(e);
      }
    }

  public Table newTable(String tableName) throws HiveException {
    String[] names = getQualifiedNames(tableName);
    switch (names.length) {
    case 3:
      Table t = new Table(names[0]+"."+names[1], names[2]);
      return t;
    case 2:
      return new Table(names[0], names[1]);
    case 1:
      return new Table(getCurrentDatabase(), names[0]);
    default:
      try{
        throw new HiveException("Invalid table name: " + tableName);
      }catch(Exception e) {
        e.printStackTrace();
      }
      throw new HiveException("Invalid table name: " + tableName);
    }
  }

  public org.apache.hadoop.hive.ql.metadata.GlobalSchema newSchema(String SchemaName) throws HiveException {
    String[] names = getQualifiedNames(SchemaName);
    switch (names.length) {

    case 1:
      return new org.apache.hadoop.hive.ql.metadata.GlobalSchema(names[0]);
    default:
      throw new HiveException("Invalid table name: " + SchemaName);
    }
  }

  private static String[] getQualifiedNames(String qualifiedName) {
    return qualifiedName.split("\\.");
  }

  /*******************************added by zjw
   * @throws HiveException ***************************************/
  public void dropPartition(String db_name,String tbl_name,String part_name) throws HiveException{
    try{
      Table t = this.getTable(db_name, tbl_name);
      getMSC().dropPartition(t.getDbName(), tbl_name, part_name, false);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean addDatawareHouseSql(Integer dwNum, String sql) throws HiveException{
    try{
      getMSC().addDatawareHouseSql(dwNum, sql);

      return true;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<String> getSubPartitions(String dbName, String tabName, String partName) throws HiveException {
    try{
//      Table t = this.getTable(db_name, tbl_name);
      return getMSC().getSubPartitions(dbName, tabName, partName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<String> getAllDatabases(String dc_name)throws HiveException {
    try{
      return getRemoteDbMSC(dc_name).getAllDatabases();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<String> getDatabases(String dc_name,String tablePattern) throws HiveException {
    try{
      return getRemoteDbMSC(dc_name).getDatabases(tablePattern);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<Busitype> showBusitypes() throws HiveException {
    try{
      return getMSC().showBusitypes();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public int createBusitype(Busitype bt) throws HiveException {
    try{
      return getMSC().createBusitype( bt);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<Node> getNodes() throws HiveException {
    try{
      return getMSC().get_all_nodes();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<SFile> getFiles(Table tbl,String part_name) throws HiveException {
    try{
      List<String> part_names = new ArrayList<String>();
      List<Long> fids = new ArrayList<Long>();
      part_names.add(part_name);
      if(tbl == null){
        LOG.info("--tbl is null--part:"+part_name);
      }
      LOG.info("--zjw--getfile:"+tbl.getTableName()+"--part:"+part_name);
      List<org.apache.hadoop.hive.metastore.api.Partition> parts =getMSC().getPartitionsByNames(tbl.getDbName(), tbl.getTableName(), part_names);
      if(parts == null || parts.isEmpty()){
        throw new HiveException("NO partition for:"+part_name);
      }
      org.apache.hadoop.hive.metastore.api.Partition part = parts.get(0);
      if(tbl.getPartitionKeys().size() == 1){

        fids.addAll(part.getFiles());
      }else if (tbl.getPartitionKeys().size() == 2){
        for(Subpartition sp : part.getSubpartitions()){
          fids.addAll(sp.getFiles());
        }
      }
      List<SFile>files = getMSC().get_files_by_ids(fids);
      return files;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
  public List<SFileLocation> getFileLocations(Table tbl,String part_name) throws HiveException {
    List<SFileLocation> fls = new ArrayList<SFileLocation>();
    try{

      List<SFile> files =  getFiles(tbl,part_name);
      for(SFile file : files){
        fls.addAll(file.getLocations());
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return fls;
  }


  public void addGeoLoc(GeoLoc gd) throws HiveException {
      GeoLocation geoLocation = new GeoLocation();
      geoLocation.setGeoLocName(gd.getGeoLocName());
      geoLocation.setNation(gd.getNation());
      geoLocation.setProvince(gd.getProvince());
      geoLocation.setCity(gd.getCity());
      geoLocation.setDist(gd.getDist());
      try{
        getMSC().addGeoLocation(geoLocation);
      } catch (Exception e) {
        throw new HiveException(e);
      }
  }

  public void dropGeoLoc(GeoLoc gd) throws HiveException {
      try {
        GeoLocation geoLocation = getMSC().getGeoLocationByName(gd.getGeoLocName());
        getMSC().deleteGeoLocation(geoLocation);
      } catch (Exception e) {
        throw new HiveException(e);
      }

  }

  public void modifyGeoLoc(GeoLoc gd) throws HiveException {
    GeoLocation geoLocation = new GeoLocation();
    geoLocation.setGeoLocName(gd.getGeoLocName());
    geoLocation.setNation(gd.getNation());
    geoLocation.setProvince(gd.getProvince());
    geoLocation.setCity(gd.getCity());
    geoLocation.setDist(gd.getDist());
    try {
      getMSC().modifyGeoLocation(geoLocation);
    }catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<GeoLoc> showGeoLoc() throws HiveException {
    List<GeoLoc> gls = new ArrayList<GeoLoc>();
    try{

      List<GeoLocation> geoLocations = getMSC().listGeoLocation();
      for(GeoLocation glc : geoLocations){
        GeoLoc gl = new GeoLoc(glc.getGeoLocName(),glc.getNation(),glc.getProvince(),glc.getCity(),glc.getDist());
        gls.add(gl);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
    return gls;

  }

  public void addEqRoom(EqRoom gd) throws HiveException,SemanticException {
    GeoLocation geoLocation;
    try {
      geoLocation = getMSC().getGeoLocationByName(gd.geoLoc.getGeoLocName());
      EquipRoom equipRoom = new EquipRoom();
      equipRoom.setEqRoomName(gd.getEqRoomName());
      String eqStatus = gd.getStatus();
      int status = -1;
      if(eqStatus.equals("ONLINE")){
        status = 0;
      }else if (eqStatus.equals("OFFLINE")){
        status = 1;
      }else if (eqStatus.equals("SUSPECT")){
        status = 2;
      }else{
        throw new SemanticException("Not valid Status for adding equipment room.");
      }
      equipRoom.setStatus(status);
      equipRoom.setComment(gd.getComment());
      equipRoom.setGeolocation(geoLocation);
      getMSC().addEquipRoom(equipRoom);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropEqRoom(EqRoom gd) throws HiveException {
    try {
      EquipRoom equipRoom = new EquipRoom(); //= getMSC().getEquipRoomByName(gd.getEqRoomName());
      getMSC().deleteEquipRoom(equipRoom);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void modifyEqRoom(EqRoom gd) throws HiveException {
      try {
        EquipRoom equipRoom = new EquipRoom();
        equipRoom.setEqRoomName(gd.getEqRoomName());
        String eqStatus = gd.getStatus();
        int status = -1;
        if(eqStatus.equals("ONLINE")){
          status = 0;
        }else if (eqStatus.equals("OFFLINE")){
          status = 1;
        }else if (eqStatus.equals("SUSPECT")){
          status = 2;
        }else{
          throw new SemanticException("Not valid Status for modify equipment room.");
        }
        equipRoom.setStatus(status);
        equipRoom.setComment(gd.getComment());
        GeoLocation geoLocation = getMSC().getGeoLocationByName(gd.geoLoc.getGeoLocName());
        equipRoom.setGeolocation(geoLocation);
        getMSC().modifyEquipRoom(equipRoom);
      } catch (Exception e) {
        throw new HiveException(e);
      }

  }

  public List<EqRoom> showEqRoom() throws HiveException {
      List<EqRoom> ers = new ArrayList<EqRoom>();
      try{
        List<EquipRoom> equipRooms = getMSC().listEquipRoom();
        for(EquipRoom eqr : equipRooms){
          GeoLoc geoLoc = new GeoLoc(eqr.getGeolocation().getGeoLocName(),eqr.getGeolocation().getNation(),
              eqr.getGeolocation().getProvince(),eqr.getGeolocation().getCity(),eqr.getGeolocation().getDist());
          String eqStatus = "";
          if(eqr.getStatus() == 0){
            eqStatus = "ONLINE";
          }else if(eqr.getStatus() == 1){
            eqStatus = "OFFLINE";
          }else if(eqr.getStatus() == 2){
            eqStatus = "SUSPECT";
          }else{
            throw new SemanticException("Not valid Status for equipment room.");
          }
          EqRoom er = new EqRoom(eqr.getEqRoomName(),eqStatus,eqr.getComment(),geoLoc);
          ers.add(er);
        }
      } catch (Exception e) {
        throw new HiveException(e);
      }
      return ers;
  }

  public void addNodeAssignment(NodeAssignment gd) throws HiveException {
    try {
      getMSC().addNodeAssignment(gd.getNodeName(), gd.getDbName());
    } catch (Exception e) {
      throw new HiveException(e);
    }
   }

  public void dropNodeAssignment(NodeAssignment gd) throws HiveException {
    try {
      getMSC().deleteNodeAssignment(gd.getNodeName(), gd.getDbName());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  public boolean createSchema(GlobalSchema schema) throws HiveException {
    try {
      LOG.info("before createSchema");
    return getMSC().createSchema(schema);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }



  public boolean modifySchema(String schemaName, GlobalSchema schema) throws HiveException {
    try {
    return getMSC().modifySchema(schemaName, schema);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }



  public boolean deleteSchema(String schemaName) throws HiveException {
    try {
    return getMSC().deleteSchema(schemaName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }



  public List<GlobalSchema> listSchemas() throws HiveException {
    try {
    return getMSC().listSchemas();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }



  public GlobalSchema getSchemaByName(String schemaName) throws HiveException {
    try {
    return getMSC().getSchemaByName(schemaName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<NodeGroup> getTableNodeGroups(String dbName, String tabName) throws HiveException {
    try {
    return getMSC().getTableNodeGroups(dbName, tabName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }



  public List<Long> listTableFiles(String dbName, String tabName, int begin, int end) throws HiveException {
    try {
    return getMSC().listTableFiles(dbName, tabName, begin, end);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<NodeAssignment> showNodeAssignment() throws HiveException {
    List<NodeAssignment> nats = new ArrayList<NodeAssignment>();
    /*try{
      List<NodeAssignment> nodeAssignments = getMSC().
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }*/
    return nats;
  }

  public void createNodeGroup(NodeGroups ngs) throws HiveException {
    try{
      String ngStatus = ngs.getStatus();
      int status = -1;
      if(ngStatus.equals("ONLINE")){
        status = 0;
      }else if (ngStatus.equals("OFFLINE")){
        status = 1;
      }else if (ngStatus.equals("SUSPECT")){
        status = 2;
      }else{
        throw new SemanticException("Not valid Status for adding Node Group.");
      }
      HashSet<Node> nodes = new HashSet<Node>();
      for(String nodeName : ngs.getNodes()){
        Node node = getMSC().get_node(nodeName);
        if(node == null){
          throw new HiveException("Not valid node:["+nodeName+"]");
        }
        nodes.add(node);
      }
      NodeGroup ng = new NodeGroup(ngs.getNode_group_name(), ngs.getComment(), status, nodes);
      getMSC().addNodeGroup(ng);
      } catch (Exception e) {
        throw new HiveException(e);
      }
  }

  public void alterNodeGroup(NodeGroup ngs, Set<String> nodeNames) throws HiveException {
    try{
      Set<Node> nds = new HashSet<Node>();
      for(String nodeName : nodeNames){
        Node node = getMSC().get_node(nodeName);
        if(node == null){
          throw new HiveException("Not valid node:["+nodeName+"]");
        }
        nds.add(node);
      }
      ngs.setNodes(nds);
      getMSC().alterNodeGroup(ngs);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropNodeGroup(NodeGroups ngs) throws HiveException {
    List<String> ngNames = new ArrayList<String>();
    ngNames.add(ngs.getNode_group_name());
    try {
      List<NodeGroup> ng = getMSC().listNodeGroups(ngNames);
      if(ng == null || ng.isEmpty()){
        throw new HiveException("No NgName :["+ngs.getNode_group_name()+"]");
      }
      getMSC().deleteNodeGroup(ng.get(0));
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  public List<SFile> filterTableFiles(String dbName, String tabName, List<SplitValue> values) throws HiveException {
    try {
    return getMSC().filterTableFiles(dbName, tabName, values);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  public void modifyNodeGroup(NodeGroups ngs) throws HiveException {
    List<String> ngNames = new ArrayList<String>();
    ngNames.add(ngs.getNode_group_name());
    try {
      List<NodeGroup> ng = getMSC().listNodeGroups(ngNames);
      getMSC().modifyNodeGroup(ngs.getNode_group_name(), ng.get(0));
    } catch (Exception e) {
      throw new HiveException(e);
    }

  }


  public List<NodeGroups> getAllNodeGroups(String ng_name)throws HiveException {
    try{
      List<String> ngNames = new ArrayList<String>();
      ngNames.add(ng_name);
      List<NodeGroup> nodeGroup = getMSC().listNodeGroups(ngNames);
      List<NodeGroups> nodeGroups = new ArrayList<NodeGroups>();
      for(NodeGroup ng : nodeGroup){
        NodeGroups ngs = new NodeGroups();
        ngs.setNode_group_name(ng.getNode_group_name());
        ngs.setComment(ng.getComment());
        String status = "";
        if(ng.getStatus() == 0){
          status = "ONLINE";
        }else if(ng.getStatus() == 1){
          status = "OFFLINE";
        }else if(ng.getStatus() == 2){
          status = "SUSPECT";
        }else{
          throw new SemanticException("Not valid Status for adding equipment room.");
        }
        ngs.setStatus(status);
        HashSet<String> ngName= new HashSet<String>();
        for(Node nd : ng.getNodes()){
          String name = nd.getNode_name();
          ngName.add(name);
        }
        ngs.setNodes(ngName);
        nodeGroups.add(ngs);
      }
      return nodeGroups;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public static enum NodeGroupStatus{
    ONLINE(0),OFFLINE(1),SUSPECT(2);
    int status = 0;
    NodeGroupStatus(int status){
      this.status=status;
    }
    public static NodeGroupStatus getNodeGroupStatus(int status){
      if(status == 0){
        return ONLINE;
      }else if (status == 1){
        return OFFLINE;
      }else {
        return SUSPECT;
      }
    }
  }
  public List<NodeGroups> getAllNodeGroups() throws HiveException {
    try {
      List<NodeGroups> ngs = new ArrayList<NodeGroups>();
      List<NodeGroup> ng = getMSC().listNodeGroups();
      for(NodeGroup nodeGroup : ng){
        NodeGroups nodeGroups = new NodeGroups();
        nodeGroups.setNode_group_name(nodeGroup.getNode_group_name());
        nodeGroups.setComment(nodeGroup.getComment());
        nodeGroups.setStatus(NodeGroupStatus.getNodeGroupStatus(nodeGroup.getStatus()).name());
        HashSet<String> ngName= new HashSet<String>();
        if(nodeGroup.getNodes() == null || nodeGroup.getNodes().isEmpty()){
          LOG.info("---zjw --nodes is null");
        }else{
          LOG.info("---zjw --nodes size:" + nodeGroup.getNodesSize());
        }
        for(Node nd : nodeGroup.getNodes()){
          String name = nd.getNode_name();
          ngName.add(name);
        }
        nodeGroups.setNodes(ngName);
        ngs.add(nodeGroups);
      }
      return ngs;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
  public List<NodeGroup> listNodeGroups(List<String> ngNames)  throws HiveException {
    try {
    return getMSC().listNodeGroups(ngNames);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
  public List<NodeGroup> listNodeGroups()  throws HiveException {
    try {
    return getMSC().listNodeGroups();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
/*public static void main(String args[]){
  (NodeGroupStatus.ONLINE.name());
  (NodeGroupStatus.ONLINE.status);
}*/

  public List<NodeGroup> listDBNodeGroups(String dbName)   throws HiveException {
    try {
    return getMSC().listDBNodeGroups(dbName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  public boolean addTableNodeDist(String db, String tab, List<String> ng)   throws HiveException {
    try {
    return getMSC().addTableNodeDist(db, tab, ng);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  public boolean deleteTableNodeDist(String db, String tab, List<String> ng)  throws HiveException {
    try {
    return getMSC().deleteTableNodeDist(db, tab, ng);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  public List<NodeGroup> listTableNodeDists(String dbName, String tabName)  throws HiveException {
    try {
    return getMSC().listTableNodeDists(dbName, tabName);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }


  public boolean assiginSchematoDB(String dbName, String schemaName,
      List<FieldSchema> fileSplitKeys, List<FieldSchema> part_keys, List<NodeGroup> ngs) throws HiveException {
    try {
    return getMSC().assiginSchematoDB(dbName, schemaName, fileSplitKeys, part_keys, ngs);
    } catch (Exception e) {
      throw new HiveException(e);
    }
   }

  public void addNodeGroupAssignment(NodeGroupAssignment nga) throws HiveException {
      try {
        NodeGroup ng = new NodeGroup();//getMSC().getNodeGroupByName(nga.getNodeGroupName());
        getMSC().addNodeGroupAssignment(ng, nga.getDbName());
      } catch (Exception e) {
        throw new HiveException(e);
      }
  }

  public void dropNodeGroupAssignment(NodeGroupAssignment nga) throws HiveException {
    try {
      NodeGroup ng = new NodeGroup();//getMSC().getNodeGroupByName(nga.getNodeGroupName());
      getMSC().deleteNodeGroupAssignment(ng, nga.getDbName());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<NodeGroupAssignment> showNodeGroupAssignment() throws HiveException {
    List<NodeGroupAssignment> ngas = new ArrayList<NodeGroupAssignment>();
    /*try{
      List<NodeGroupAssignment> nodeGroupAssignments = getMSC().
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }*/
    return ngas;
  }

  public void addRoleAssignment(RoleAssignment ra) throws HiveException {
    try {
      getMSC().addRoleAssignment(ra.getRoleName(),ra.getDbName());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropRoleAssignment(RoleAssignment ra) throws HiveException {
    try {
      getMSC().deleteRoleAssignment(ra.getRoleName(),ra.getDbName());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public List<RoleAssignment> showRoleAssignment() throws HiveException {
    List<RoleAssignment> ras = new ArrayList<RoleAssignment>();
    /*try{
      List<RoleAssignment> roleAssignments = getMSC().
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }*/
    return ras;
  }

  public void addUserAssignment(UserAssignment ua) throws HiveException {
    try {
      getMSC().addUserAssignment(ua.getUserName(),ua.getDbName());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropUserAssignment(UserAssignment ua) throws HiveException {
      try {
        getMSC().deleteUserAssignment(ua.getUserName(),ua.getDbName());
      } catch (Exception e) {
        throw new HiveException(e);
      }
  }

  public List<UserAssignment> showUserAssignment() throws HiveException {
    List<UserAssignment> uas = new ArrayList<UserAssignment>();
    /*try{
      List<UserAssignment> userAssignments = getMSC().
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }*/
    return uas;
  }


  public void dropSchema(String schemaName) throws HiveException {

    try {
      GlobalSchema gls = getMSC().getSchemaByName(schemaName);
      getMSC().deleteSchema(gls.getSchemaName());
    } catch (Exception e) {
      throw new HiveException(e);
    }

  }

  public List<org.apache.hadoop.hive.ql.metadata.GlobalSchema> showGlobalSchema() throws HiveException {
    List<org.apache.hadoop.hive.ql.metadata.GlobalSchema> gl =
        new ArrayList<org.apache.hadoop.hive.ql.metadata.GlobalSchema>();
    try {
      List<GlobalSchema> gls = getMSC().listSchemas();
      if(gls != null){
        LOG.info("---zjw-- in showGlobalSchema:"+gls.size());
      }else{
        LOG.info("---zjw-- in showGlobalSchema is null");
      }
      for(GlobalSchema serverg : gls){
        org.apache.hadoop.hive.ql.metadata.GlobalSchema gsa =
            new org.apache.hadoop.hive.ql.metadata.GlobalSchema(serverg);
        gl.add(gsa);
      }
    } catch (Exception e) {
      LOG.error(e,e);
      throw new HiveException(e);
    }
    return gl;
  }
  /**
   * Updates the existing table metadata with the new metadata.
   *
   * @param glsName
   *          name of the existing schema
   * @param newSch
   *          new name of the schema. could be the old name
   * @throws InvalidOperationException
   *           if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterSchema(String schName, org.apache.hadoop.hive.ql.metadata.GlobalSchema newSch)
      throws InvalidOperationException, HiveException, Exception {
    org.apache.hadoop.hive.ql.metadata.GlobalSchema gls = new org.apache.hadoop.hive.ql.metadata.GlobalSchema(schName);
    try {
      // Remove the DDL_TIME so it gets refreshed
      if (newSch.getParameters() != null) {
        newSch.getParameters().remove(hive_metastoreConstants.DDL_TIME);
      }
      getMSC().modifySchema(gls.getSchemaName(), newSch.getTSchema());
    } catch (MetaException e) {
      LOG.error("Unable to modify schema MetaException:" + e.getMessage());
      throw new HiveException("Unable to alter schema.", e);
    } catch (TException e) {
      LOG.error("Unable to modify schema TException:" + e.getMessage());
      throw new HiveException("Unable to alter schema.", e);
    } catch (Exception e) {
      LOG.error("Unable to modify schema Exception:" + e.getMessage());
      throw new Exception("Unable to alter schema.", e);
    }
  }

  public void addNode(AddNode an) throws HiveException {
    try {
      getMSC().add_node(an.getName(), an.getIp());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropNode(DropNode dn) throws HiveException {
    try {
      getMSC().del_node(dn.getName());
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
};
