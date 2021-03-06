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

package org.apache.hadoop.hive.metastore;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.DiskManager.DeviceInfo;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.BusiTypeColumn;
import org.apache.hadoop.hive.metastore.api.BusiTypeDatacenter;
import org.apache.hadoop.hive.metastore.api.Busitype;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.EquipRoom;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GeoLocation;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SFileRef;
import org.apache.hadoop.hive.metastore.api.SplitValue;
import org.apache.hadoop.hive.metastore.api.Subpartition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.statfs;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.metastore.model.MUser;
import org.apache.thrift.TException;

public interface RawStore extends Configurable {

  public abstract void shutdown();

  /**
   * Opens a new one or the one already created Every call of this function must
   * have corresponding commit or rollback function call
   *
   * @return an active transaction
   */

  public abstract boolean openTransaction();

  /**
   * if this is the commit of the first open call then an actual commit is
   * called.
   *
   * @return true or false
   */
  public abstract boolean commitTransaction();

  /**
   * Rolls back the current transaction if it is active
   */
  public abstract void rollbackTransaction();

  public abstract void createDatabase(Database db)
      throws InvalidObjectException, MetaException;

  public abstract Database getDatabase(String name)
      throws NoSuchObjectException;

  public abstract boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException;

  public abstract boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException, MetaException;

  public abstract List<String> getDatabases(String pattern) throws MetaException;

  public abstract List<String> getAllDatabases() throws MetaException;

  public abstract boolean createType(Type type);

  public abstract Type getType(String typeName);

  public abstract boolean dropType(String typeName);

  public abstract void createTable(Table tbl) throws InvalidObjectException,
      MetaException;

  public abstract void createOrUpdateDevice(DeviceInfo di, Node node, NodeGroup ng) throws
      InvalidObjectException, MetaException;

  public Device modifyDevice(Device dev, Node node) throws MetaException, NoSuchObjectException, InvalidObjectException;

  public abstract void offlineDevice(String devid) throws InvalidObjectException, MetaException;

  public abstract void createNode(Node node) throws InvalidObjectException, MetaException;

  public boolean updateNode(Node node) throws MetaException;

  public boolean delNode(String node_name) throws MetaException;

  public abstract Node getNode(String node_name) throws MetaException;

  public List<Node> getAllNodes() throws MetaException;

  public long countNode() throws MetaException;

  public long countFiles() throws MetaException;

  public SFile createFile(SFile file) throws InvalidObjectException, MetaException;

  public SFile getSFile(long fid) throws MetaException;

  public SFile getSFile(String devid, String location) throws MetaException;

  public boolean delSFile(long fid) throws MetaException;

  public SFile updateSFile(SFile newfile) throws MetaException;

  public boolean createFileLocation(SFileLocation location) throws InvalidObjectException, MetaException;

  public List<SFileLocation> getSFileLocations(long fid) throws MetaException;

  public List<SFileLocation> getSFileLocations(int status) throws MetaException;

  public List<SFileLocation> getSFileLocations(String devid, long curts, long timeout) throws MetaException;

  public SFileLocation getSFileLocation(String devid, String location) throws MetaException;

  public SFileLocation updateSFileLocation(SFileLocation newsfl) throws MetaException;

  public boolean delSFileLocation(String devid, String location) throws MetaException;

  public abstract boolean dropTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  public abstract Table getTable(String dbName, String tableName)
      throws MetaException;

  public abstract Table getTableByID(long id) throws MetaException;

  public long getTableOID(String dbName, String tableName) throws MetaException;

  public abstract boolean addPartition(Partition part)
      throws InvalidObjectException, MetaException;

  public abstract Partition getPartition(String dbName, String tableName,
      String partName) throws MetaException, NoSuchObjectException;

  public abstract Subpartition getSubpartition(String dbName, String tableName,
      String partName) throws MetaException, NoSuchObjectException;

  public abstract Partition getPartition(String db_name, String tbl_name,
      List<String> part_vals) throws MetaException, NoSuchObjectException;

  public void updatePartition(Partition newPart) throws InvalidObjectException, MetaException;

  public void updateSubpartition(Subpartition newPart) throws InvalidObjectException, MetaException;

  public abstract boolean dropPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException;

  public boolean dropPartition(String dbName, String tableName,
    String part_name) throws MetaException, NoSuchObjectException, InvalidObjectException,
    InvalidInputException;

  public abstract List<Partition> getPartitions(String dbName,
      String tableName, int max) throws MetaException;

  public abstract void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException;

  public List<String> getTables(String dbName, String pattern)
      throws MetaException;

  /**
   * @param dbname
   *        The name of the database from which to retrieve the tables
   * @param tableNames
   *        The names of the tables to retrieve.
   * @return A list of the tables retrievable from the database
   *          whose names are in the list tableNames.
   *         If there are duplicate names, only one instance of the table will be returned
   * @throws MetaException
   */
  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException;

  public List<String> getAllTables(String dbName) throws MetaException;

  /**
   * Gets a list of tables based on a filter string and filter type.
   * @param dbName
   *          The name of the database from which you will retrieve the table names
   * @param filter
   *          The filter string
   * @param max_tables
   *          The maximum number of tables returned
   * @return  A list of table names that match the desired filter
   * @throws MetaException
   * @throws UnknownDBException
   */
  public abstract List<String> listTableNamesByFilter(String dbName,
      String filter, short max_tables) throws MetaException, UnknownDBException;

  public abstract List<String> listPartitionNames(String db_name,
      String tbl_name, short max_parts) throws MetaException;

  public abstract List<String> listPartitionNamesByFilter(String db_name,
      String tbl_name, String filter, short max_parts) throws MetaException;

  public abstract void alterPartition(String db_name, String tbl_name, String partName, List<String> part_vals,
      Partition new_part) throws InvalidObjectException, MetaException;

  public abstract void alterPartitions(String db_name, String tbl_name, List<String> partNames,
      List<List<String>> part_vals_list, List<Partition> new_parts)
      throws InvalidObjectException, MetaException;

  public abstract boolean addIndex(Index index)
      throws InvalidObjectException, MetaException;

  public abstract Index getIndex(String dbName, String origTableName, String indexName) throws MetaException;

  public abstract boolean dropIndex(String dbName, String origTableName, String indexName) throws MetaException;

  public abstract List<Index> getIndexes(String dbName,
      String origTableName, int max) throws MetaException;

  public abstract List<String> listIndexNames(String dbName,
      String origTableName, short max) throws MetaException;

  public abstract void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
      throws InvalidObjectException, MetaException;

  public abstract List<Partition> getPartitionsByFilter(
      String dbName, String tblName, String filter, short maxParts)
      throws MetaException, NoSuchObjectException;

  public abstract List<Partition> getPartitionsByNames(
      String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException;

  public abstract Table markPartitionForEvent(String dbName, String tblName, Map<String,String> partVals, PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException;

  public abstract boolean isPartitionMarkedForEvent(String dbName, String tblName, Map<String, String> partName, PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException;

  public abstract boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  public abstract boolean removeRole(String roleName) throws MetaException, NoSuchObjectException;

  public abstract boolean grantRole(Role role, String userName, PrincipalType principalType,
      String grantor, PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  public abstract boolean revokeRole(Role role, String userName, PrincipalType principalType)
      throws MetaException, NoSuchObjectException;

  public abstract PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException;

  public abstract PrincipalPrivilegeSet getDBPrivilegeSet (String dbName, String userName,
      List<String> groupNames)  throws InvalidObjectException, MetaException;

  public abstract PrincipalPrivilegeSet getTablePrivilegeSet (String dbName, String tableName,
      String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  public abstract PrincipalPrivilegeSet getPartitionPrivilegeSet (String dbName, String tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  public abstract PrincipalPrivilegeSet getColumnPrivilegeSet (String dbName, String tableName, String partitionName,
      String columnName, String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  public abstract List<MGlobalPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType);

  public abstract List<MDBPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String dbName);

  public abstract List<MTablePrivilege> listAllTableGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName);

  public abstract List<MPartitionPrivilege> listPrincipalPartitionGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String partName);

  public abstract List<MTableColumnPrivilege> listPrincipalTableColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String columnName);

  public abstract List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String partName, String columnName);

  public abstract boolean grantPrivileges (PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  public abstract boolean revokePrivileges  (PrivilegeBag privileges)
  throws InvalidObjectException, MetaException, NoSuchObjectException;

  public abstract org.apache.hadoop.hive.metastore.api.Role getRole(
      String roleName) throws NoSuchObjectException;

  public List<String> listRoleNames();

  public List<MRoleMap> listRoles(String principalName,
      PrincipalType principalType);

  public abstract Partition getPartitionWithAuth(String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  public abstract List<Partition> getPartitionsWithAuth(String dbName,
      String tblName, short maxParts, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  /**
   * Lists partition names that match a given partial specification
   * @param db_name
   *          The name of the database which has the partitions
   * @param tbl_name
   *          The name of the table which has the partitions
   * @param part_vals
   *          A partial list of values for partitions in order of the table's partition keys.
   *          Entries can be empty if you only want to specify latter partitions.
   * @param max_parts
   *          The maximum number of partitions to return
   * @return A list of partition names that match the partial spec.
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  public abstract List<String> listPartitionNamesPs(String db_name, String tbl_name,
      List<String> part_vals, short max_parts)
      throws MetaException, NoSuchObjectException;

  /**
   * Lists partitions that match a given partial specification and sets their auth privileges.
   *   If userName and groupNames null, then no auth privileges are set.
   * @param db_name
   *          The name of the database which has the partitions
   * @param tbl_name
   *          The name of the table which has the partitions
   * @param part_vals
   *          A partial list of values for partitions in order of the table's partition keys
   *          Entries can be empty if you need to specify latter partitions.
   * @param max_parts
   *          The maximum number of partitions to return
   * @param userName
   *          The user name for the partition for authentication privileges
   * @param groupNames
   *          The groupNames for the partition for authentication privileges
   * @return A list of partitions that match the partial spec.
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws InvalidObjectException
   */
  public abstract List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException;

  /** Persists the given column statistics object to the metastore
   * @param partVals
   *
   * @param ColumnStats object to persist
   * @param List of partVals
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */
  public abstract boolean updateTableColumnStatistics(ColumnStatistics colStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /** Persists the given column statistics object to the metastore
   * @param partVals
   *
   * @param ColumnStats object to persist
   * @param List of partVals
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */
  public abstract boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,
     List<String> partVals)
     throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Returns the relevant column statistics for a given column in a given table in a given database
   * if such statistics exist.
   *
   * @param The name of the database, defaults to current database
   * @param The name of the table
   * @param The name of the column for which statistics is requested
   * @return Relevant column statistics for the column for the given table
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidInputException
   *
   */
  public abstract ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
    String colName) throws MetaException, NoSuchObjectException, InvalidInputException,
    InvalidObjectException;

  /**
   * Returns the relevant column statistics for a given column in a given partition in a given
   * table in a given database if such statistics exist.
   * @param partName
   *
   * @param The name of the database, defaults to current database
   * @param The name of the table
   * @param The name of the partition
   * @param List of partVals for the partition
   * @param The name of the column for which statistics is requested
   * @return Relevant column statistics for the column for the given partition in a given table
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidInputException
   * @throws InvalidObjectException
   *
   */

  public abstract ColumnStatistics getPartitionColumnStatistics(String dbName, String tableName,
    String partName, List<String> partVals, String colName)
    throws MetaException, NoSuchObjectException, InvalidInputException, InvalidObjectException;

  /**
   * Deletes column statistics if present associated with a given db, table, partition and col. If
   * null is passed instead of a colName, stats when present for all columns associated
   * with a given db, table and partition are deleted.
   *
   * @param dbName
   * @param tableName
   * @param partName
   * @param partVals
   * @param colName
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */

  public abstract boolean deletePartitionColumnStatistics(String dbName, String tableName,
      String partName, List<String> partVals, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Deletes column statistics if present associated with a given db, table and col. If
   * null is passed instead of a colName, stats when present for all columns associated
   * with a given db and table are deleted.
   *
   * @param dbName
   * @param tableName
   * @param colName
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */

  public abstract boolean deleteTableColumnStatistics(String dbName, String tableName,
    String colName)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  public abstract long cleanupEvents();

  public abstract Node findNode(String ip) throws MetaException;

//authentication  with user by liulichao, begin
  public abstract boolean addUser(String userName, String passwd, String ownerName)
        throws InvalidObjectException, MetaException;

  public abstract boolean removeUser(String userName) throws MetaException, NoSuchObjectException;

  public List<String> listUsersNames() throws MetaException;

  public abstract boolean authentication(String userName, String passwd) throws MetaException, NoSuchObjectException;
  //authentication and authorization with user by liulichao, end

  public abstract MUser getMUser(String user);  //added by liulichao

  public List<SFile> findUnderReplicatedFiles() throws MetaException;

  public List<SFile> findOverReplicatedFiles() throws MetaException;

  public List<SFile> findLingeringFiles(long node_nr) throws MetaException;

  public void findFiles(List<SFile> underReplicated, List<SFile> overReplicated, List<SFile> lingering, long from, long to) throws MetaException;

  public void findVoidFiles(List<SFile> voidFiles) throws MetaException;

  public void createPartitionIndex(Index index, Partition part) throws InvalidObjectException, MetaException, AlreadyExistsException;

  public void createPartitionIndex(Index index, Subpartition part) throws InvalidObjectException, MetaException, AlreadyExistsException;

  public boolean dropPartitionIndex(Index index, Partition part) throws InvalidObjectException, NoSuchObjectException, MetaException;

  public boolean dropPartitionIndex(Index index, Subpartition part) throws InvalidObjectException, NoSuchObjectException, MetaException;

  public void createPartitionIndexStores(Index index, Partition part, List<SFile> store, List<Long> originFid) throws InvalidObjectException, MetaException;

  public void createPartitionIndexStores(Index index, Subpartition part, List<SFile> store, List<Long> originFid) throws InvalidObjectException, MetaException;

  public boolean dropPartitionIndexStores(Index index, Partition part, List<SFile> store) throws InvalidObjectException, NoSuchObjectException, MetaException;

  public boolean dropPartitionIndexStores(Index index, Subpartition part, List<SFile> store) throws InvalidObjectException, NoSuchObjectException, MetaException;

  public List<SFileRef> getPartitionIndexFiles(Index index, Partition part) throws InvalidObjectException, NoSuchObjectException, MetaException;

  public boolean add_datawarehouse_sql(int dwNum, String sql)throws InvalidObjectException, MetaException;

  public abstract List<SFileRef> getSubpartitionIndexFiles(Index index, Subpartition subpart) throws InvalidObjectException, MetaException ;

  public abstract List<Subpartition> getSubpartitions(String dbname, String tbl_name, Partition part) throws InvalidObjectException, NoSuchObjectException, MetaException;


  public abstract List<BusiTypeColumn> getAllBusiTypeCols()throws MetaException;

  public Partition getParentPartition(String dbName, String tableName, String subpart_name) throws NoSuchObjectException, MetaException;

  public abstract List<BusiTypeDatacenter> get_all_busi_type_datacenters()throws  MetaException, TException ;

  public abstract void append_busi_type_datacenter(BusiTypeDatacenter busiTypeDatacenter)throws InvalidObjectException, MetaException, TException;

  public abstract List<Busitype> showBusitypes()throws  MetaException, TException;

  public abstract int createBusitype(Busitype busitype)throws InvalidObjectException, MetaException, TException;

  public Device getDevice(String devid) throws MetaException, NoSuchObjectException;

  public boolean delDevice(String devid) throws MetaException;

  boolean modifyUser(User user) throws MetaException, NoSuchObjectException;

  public abstract boolean addEquipRoom(EquipRoom er) throws MetaException;

  public abstract boolean modifyEquipRoom(EquipRoom er) throws MetaException;

  public abstract boolean deleteEquipRoom(EquipRoom er) throws MetaException;

  public abstract List<EquipRoom> listEquipRoom() throws MetaException;

  public abstract boolean addGeoLocation(GeoLocation gl) throws MetaException;

  public abstract boolean modifyGeoLocation(GeoLocation gl) throws MetaException;

  public abstract boolean deleteGeoLocation(GeoLocation gl) throws MetaException;

  public abstract List<GeoLocation> listGeoLocation() throws MetaException;

  public abstract List<String> listUsersNames(String dbName) throws MetaException;

  public abstract GlobalSchema getSchema(String schema_name)throws NoSuchObjectException,MetaException;

  public abstract  boolean modifySchema(String schemaName,GlobalSchema schema) throws InvalidObjectException, MetaException;
  //修改表模式

  public abstract  boolean deleteSchema(String schemaName) throws InvalidObjectException, InvalidInputException,NoSuchObjectException,  MetaException;
  //删除模式

  public abstract  List<GlobalSchema> listSchemas() throws  MetaException;
  //获取所有模式

  public abstract  boolean addNodeGroup(NodeGroup ng) throws InvalidObjectException, MetaException;
  //新增节点组

  public abstract  boolean alterNodeGroup(NodeGroup ng) throws InvalidObjectException, MetaException;
  //往节点组新增节点

  public abstract  boolean modifyNodeGroup(String ngName,NodeGroup ng) throws InvalidObjectException, MetaException;
  //修改节点组

  public abstract  boolean deleteNodeGroup(NodeGroup ng) throws  MetaException;
  //删除节点组

  public abstract  List<NodeGroup> listNodeGroups() throws  MetaException;
  //获取所有节点组

  public abstract  List<NodeGroup> listDBNodeGroups(String dbName) throws  MetaException;
  //获取某归属地所有节点组

  public abstract  boolean addTableNodeDist(String db, String tab, List<String> ng) throws  MetaException;
  //新增某归属地表的存储节点组

  public abstract  boolean deleteTableNodeDist(String db, String tab, List<String> ng) throws  MetaException;
  //删除某归属地表的存储节点组

  public abstract  List<NodeGroup> listTableNodeDists(String dbName, String tabName) throws  MetaException;
  //获取某归属地的表分布节点组


  public abstract void createSchema(GlobalSchema schema)throws InvalidObjectException, MetaException;

  public abstract List<Long> listTableFiles(String dbName, String tableName, int begin, int end) throws MetaException;

  public abstract List<Long> findSpecificDigestFiles(String digest) throws MetaException;

  public abstract List<SFile> filterTableFiles(String dbName, String tableName, List<SplitValue> values)
      throws MetaException;

  public abstract boolean assiginSchematoDB(String dbName, String schemaName, List<FieldSchema> fileSplitKeys, List<FieldSchema> part_keys,
      List<NodeGroup> ngs) throws InvalidObjectException,NoSuchObjectException, MetaException;

  public abstract List<NodeGroup> listNodeGroupByNames(List<String> ngNames) throws  MetaException;

  /**
   * 根据名找对象,节点归属，用户归属，角色归属的增删查！！！
   * @author
   * @param geoLocName
   * @return
   * @throws MetaException
   */

  public abstract GeoLocation getGeoLocationByName(String geoLocName) throws MetaException;

  public abstract List<GeoLocation> getGeoLocationByNames(List<String> geoLocNames) throws MetaException;

  public abstract boolean addNodeAssignment(String nodename, String dbname) throws MetaException, NoSuchObjectException;

  public boolean deleteNodeAssignment(String nodeName, String dbName) throws MetaException, NoSuchObjectException;

  public abstract List<Node> listNodes() throws  MetaException;

  public abstract boolean addUserAssignment(String userName, String dbName) throws MetaException, NoSuchObjectException;

  public abstract boolean deleteUserAssignment(String userName, String dbName) throws MetaException, NoSuchObjectException;

  public abstract List<User> listUsers() throws  MetaException;

  public abstract boolean addRoleAssignment(String roleName, String dbName) throws MetaException, NoSuchObjectException;

  public abstract boolean deleteRoleAssignment(String roleName, String dbName) throws MetaException, NoSuchObjectException;

  public abstract List<Role> listRoles() throws  MetaException;

  public abstract boolean addNodeGroupAssignment(NodeGroup ng, String dbName) throws MetaException, NoSuchObjectException;

  public abstract boolean deleteNodeGroupAssignment(NodeGroup ng, String dbName) throws MetaException, NoSuchObjectException;

  public abstract void truncTableFiles(String dbName, String tableName) throws MetaException, NoSuchObjectException;

  public abstract boolean reopenSFile(SFile file) throws MetaException;

  public abstract long getCurrentFID();

  public abstract long getMinFID() throws MetaException;

  public abstract List<Device> listDevice() throws MetaException;

  public abstract List<String> listDevsByNode(String nodeName) throws MetaException;

  public abstract List<Long> listFilesByDevs(List<String> devids) throws MetaException, TException;

  public abstract statfs statFileSystem(long from, long to) throws MetaException;

  public abstract long countDevice() throws MetaException;

}
