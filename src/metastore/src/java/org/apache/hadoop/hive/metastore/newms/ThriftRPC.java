package org.apache.hadoop.hive.metastore.newms;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DiskManager;
import org.apache.hadoop.hive.metastore.DiskManager.DMProfile;
import org.apache.hadoop.hive.metastore.DiskManager.DMRequest;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.BusiTypeColumn;
import org.apache.hadoop.hive.metastore.api.BusiTypeDatacenter;
import org.apache.hadoop.hive.metastore.api.Busitype;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreatePolicy;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.EquipRoom;
import org.apache.hadoop.hive.metastore.api.FOFailReason;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.FindNodePolicy;
import org.apache.hadoop.hive.metastore.api.GeoLocation;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MSOperation;
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
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.thrift.TException;

import com.facebook.fb303.fb_status;
import com.google.common.collect.Lists;

/*
 * 没缓存的对象，但是rpc里要得到的
 * BusiTypeColumn，Device,ColumnStatistics,Type,role,User,HiveObjectPrivilege
 */

public class ThriftRPC implements org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface {


  private final NewMSConf conf;
  private final RawStore rs;
  private IMetaStoreClient client;

  private static final Log LOG = NewMS.LOG;
  private DiskManager dm;
  private final long startTimeMillis;

  public ThriftRPC(NewMSConf conf)
  {
    this.conf = conf;
    rs = new RawStoreImp(conf);
    startTimeMillis = System.currentTimeMillis();
    try {
      client = MsgProcessing.createMetaStoreClient();
      dm = new DiskManager(new HiveConf(DiskManager.class), LOG);
    } catch (MetaException e) {
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * return the alive timemillis since last set up
   * the {@code startTimeMillis} is inited in the constructor method {@link #ThriftRPC(NewMSConf)}
   *
   * @author mzy
   * @return {@code System.currentTimeMillis - startTimeMillis}
   */
  @Override
  public long aliveSince() throws TException {
    return System.currentTimeMillis() - startTimeMillis;
  }

  @Override
  public long getCounter(String arg0) throws TException {
    if (isNullOrEmpty(arg0)) {
      return -1;
    }
    return 0;
  }

  @Override
  public Map<String, Long> getCounters() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCpuProfile(int profileDurationInSec) throws TException {

    return "";
  }

  @Override
  public String getName() throws TException {

    return null;
  }

  @Override
  public String getOption(String arg0) throws TException {

    return null;
  }

  @Override
  public Map<String, String> getOptions() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public fb_status getStatus() throws TException {
    return null;
  }

  @Override
  public String getStatusDetails() throws TException {

    return null;
  }

  @Override
  public String getVersion() throws TException {
    return "0.1";
  }

  @Override
  public void reinitialize() throws TException {

  }

  @Override
  public void setOption(String arg0, String arg1) throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void shutdown() throws TException {

  }

  @Override
  public boolean addEquipRoom(EquipRoom arg0) throws MetaException,
      TException {
    return arg0 != null && client.addEquipRoom(arg0);
  }

  @Override
  public boolean addGeoLocation(GeoLocation arg0) throws MetaException,
      TException {
    return arg0 != null && client.addGeoLocation(arg0);
  }

  @Override
  public boolean addNodeAssignment(String nodeName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    final boolean expr = isNullOrEmpty(nodeName) || isNullOrEmpty(dbName);
    return !expr && client.addNodeAssignment(nodeName, dbName);
  }

  @Override
  public boolean addNodeGroup(NodeGroup nodeGroup) throws AlreadyExistsException,
      MetaException, TException {

    return nodeGroup != null && client.addNodeGroup(nodeGroup);
  }

  @Override
  public boolean addNodeGroupAssignment(NodeGroup nodeGroup, String dbName)
      throws MetaException, TException {
    final boolean expr = nodeGroup == null || isNullOrEmpty(dbName);
    return !expr && client.addNodeGroupAssignment(nodeGroup, dbName);
  }

  @Override
  public boolean addRoleAssignment(String roleName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    final boolean expr = isNullOrEmpty(roleName) || isNullOrEmpty(dbName);
    return !expr && client.addRoleAssignment(roleName, dbName);
  }

  @Override
  public boolean addTableNodeDist(String dbName, String tableName, List<String> nodeGroupList)
      throws MetaException, TException {
    final boolean expr = isNullOrEmpty(dbName) || isNullOrEmpty(tableName) || nodeGroupList == null;
    return !expr && client.addTableNodeDist(dbName, tableName, nodeGroupList);
  }

  @Override
  public boolean addUserAssignment(String userName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    final boolean expr = isNullOrEmpty(userName) || isNullOrEmpty(dbName);
    return !expr && client.addUserAssignment(userName, dbName);
  }

  @Override
  public boolean add_datawarehouse_sql(int dwNum, String sql)
      throws InvalidObjectException, MetaException, TException {
    return client.addDatawareHouseSql(dwNum, sql);
  }

  @Override
  public Index add_index(Index index, Table indexTable)
      throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    client.createIndex(index, indexTable);
    return index;
  }

  @Override
  public Node add_node(String nodeName, List<String> ipl) throws MetaException,
      TException {
    final boolean expr = isNullOrEmpty(nodeName) || ipl == null;
    checkArgument(expr, "nodeName and ipl shuldn't be null or empty");
    final Node node = client.add_node(nodeName, ipl);
    return node;
  }

  @Override
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    return client.add_partition(checkNotNull(partition, "partation shuldn't be null"));
  }

  @Override
  public int add_partition_files(Partition partition, List<SFile> sfiles)
      throws TException {
    return client.add_partition_files(checkNotNull(partition), checkNotNull(sfiles));
  }

  @Override
  public boolean add_partition_index(Index index, Partition partition)
      throws MetaException, AlreadyExistsException, TException {
    return client.add_partition_index(checkNotNull(index), checkNotNull(partition));
  }

  @Override
  public boolean add_partition_index_files(Index arg0, Partition arg1,
      List<SFile> arg2, List<Long> arg3) throws MetaException, TException {
    // TODO zy
    return false;
  }

  @Override
  public Partition add_partition_with_environment_context(Partition arg0,
      EnvironmentContext arg1) throws InvalidObjectException,
      AlreadyExistsException, MetaException, TException {
    // TODO mzy
    return null;
  }

  @Override
  public int add_partitions(List<Partition> partitions)
      throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    return client.add_partitions(partitions);
  }

  /**
   * just return true like HiveMetaStore
   *
   * @author mzy
   */
  @Override
  public boolean add_subpartition(String arg0, String arg1,
      List<String> arg2, Subpartition arg3) throws TException {
    return true;
  }

  @Override
  public int add_subpartition_files(Subpartition arg0, List<SFile> arg1)
      throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean add_subpartition_index(Index index, Subpartition subpart)
      throws MetaException, AlreadyExistsException, TException {
    // TODO Auto-generated method stub
    return client.add_subpartition_index(index, subpart);
  }

  @Override
  public boolean add_subpartition_index_files(Index arg0, Subpartition arg1,
      List<SFile> arg2, List<Long> arg3) throws MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean alterNodeGroup(NodeGroup ng)
      throws AlreadyExistsException, MetaException, TException {
    return client.alterNodeGroup(ng);
  }

  @Override
  public void alter_database(String name, Database db)
      throws MetaException, NoSuchObjectException, TException {
    client.alterDatabase(name, db);
  }

  @Override
  public void alter_index(String dbName, String tblName, String indexName, Index index)
      throws InvalidOperationException, MetaException, TException {
    client.alter_index(dbName, tblName, indexName, index);
  }

  @Override
  public Node alter_node(String nodeName, List<String> ipl, int status)
      throws MetaException, TException {
    return client.alter_node(nodeName, ipl, status);
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partition(dbName, tblName, newPart);
  }

  @Override
  public void alter_partition_with_environment_context(String dbName,
      String name, Partition newPart, EnvironmentContext arg3)
      throws InvalidOperationException, MetaException, TException {
    client.renamePartition(dbName, name, null, newPart);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partitions(dbName, tblName, newParts);
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table)
      throws InvalidOperationException, MetaException, TException {
    client.alter_table(defaultDatabaseName, tblName, table);
  }

  @Override
  public void alter_table_with_environment_context(String arg0, String arg1,
      Table arg2, EnvironmentContext arg3)
      throws InvalidOperationException, MetaException, TException {
    // TODO implement this method
  }

  @Override
  public void append_busi_type_datacenter(BusiTypeDatacenter busiTypeDatacenter)
      throws InvalidObjectException, MetaException, TException {
    client.append_busi_type_datacenter(busiTypeDatacenter);
  }

  @Override
  public Partition append_partition(String tableName, String dbName,
      List<String> partVals) throws InvalidObjectException,
      AlreadyExistsException, MetaException, TException {
    return client.appendPartition(tableName, dbName, partVals);
  }

  @Override
  public Partition append_partition_by_name(String tableName, String dbName,
      String name) throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    return client.appendPartition(tableName, dbName, name);
  }

  @Override
  public boolean assiginSchematoDB(String dbName, String schemaName,
      List<FieldSchema> fileSplitKeys, List<FieldSchema> partKeys, List<NodeGroup> ngs)
      throws InvalidObjectException, NoSuchObjectException,
      MetaException, TException {
    return client.assiginSchematoDB(dbName, schemaName, fileSplitKeys, partKeys, ngs);
  }

  @Override
  public boolean authentication(String userName, String passwd)
      throws NoSuchObjectException, MetaException, TException {
    return client.authentication(userName, passwd);
  }

  @Override
  public void cancel_delegation_token(String tokenStrForm) throws MetaException,
      TException {
    client.cancelDelegationToken(tokenStrForm);
  }

  @Override
  public int close_file(SFile arg0) throws FileOperationException,
      MetaException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int createBusitype(Busitype bt) throws InvalidObjectException,
      MetaException, TException {
    return client.createBusitype(bt);
  }

  @Override
  public boolean createSchema(GlobalSchema schema)
      throws AlreadyExistsException, InvalidObjectException,
      MetaException, TException {
    // TODO Auto-generated method stub
    return client.createSchema(schema);
  }

  @Override
  public void create_attribution(Database db)
      throws AlreadyExistsException, InvalidObjectException,
      MetaException, TException {
    client.create_attribution(db);
  }

  @Override
  public void create_database(Database db) throws AlreadyExistsException,
      InvalidObjectException, MetaException, TException {
    client.createDatabase(db);
  }

  @Override
  public Device create_device(String devId, int prop, String nodeName)
      throws MetaException, TException {
    return client.createDevice(devId, prop, nodeName);
  }

  @Override
  public SFile create_file(String node_name, int repnr, String db_name, String table_name,
      List<SplitValue> values)
      throws FileOperationException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SFile create_file_by_policy(CreatePolicy arg0, int arg1,
      String arg2, String arg3, List<SplitValue> arg4)
      throws FileOperationException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean create_role(Role role) throws MetaException, TException {
    return client.create_role(role);
  }

  @Override
  public void create_table(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException,
      TException {
    client.createTable(tbl);
  }

  @Override
  public void create_table_by_user(Table tbl, User user)
      throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {
    client.createTableByUser(tbl, user);
  }

  @Override
  public void create_table_with_environment_context(Table arg0,
      EnvironmentContext arg1) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException,
      TException {
    // TODO implement this method
  }

  @Override
  public boolean create_type(Type type) throws AlreadyExistsException,
      InvalidObjectException, MetaException, TException {
    return rs.createType(type);
  }

  @Override
  public boolean create_user(User user) throws InvalidObjectException,
      MetaException, TException {
    return client.create_user(user);
  }

  @Override
  public boolean del_device(String devId) throws MetaException, TException {
    return client.delDevice(devId);
  }

  @Override
  public int del_node(String nodeName) throws MetaException, TException {
    return (!isNullOrEmpty(nodeName) && client.del_node(nodeName)) ? 1 : 0;
  }

  @Override
  public boolean deleteEquipRoom(EquipRoom er) throws MetaException,
      TException {
    return er != null && client.deleteEquipRoom(er);
  }

  @Override
  public boolean deleteGeoLocation(GeoLocation gl) throws MetaException,
      TException {
    return gl != null && client.deleteGeoLocation(gl);
  }

  @Override
  public boolean deleteNodeAssignment(String nodeName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    boolean expr = isNullOrEmpty(nodeName) || isNullOrEmpty(dbName);
    return !expr && client.deleteNodeAssignment(nodeName, dbName);
  }

  @Override
  public boolean deleteNodeGroup(NodeGroup nodeGroup) throws MetaException,
      TException {
    return nodeGroup != null && client.deleteNodeGroup(nodeGroup);
  }

  @Override
  public boolean deleteNodeGroupAssignment(NodeGroup nodeGroup, String dbName)
      throws MetaException, TException {
    return client.deleteNodeGroupAssignment(nodeGroup, dbName);
  }

  @Override
  public boolean deleteRoleAssignment(String roleName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    return client.deleteRoleAssignment(roleName, dbName);
  }

  /**
   * Delete a schema with the assigned param
   *
   * @param arg0
   *          is schema name
   * @return if the param is {@code NULL} or Empty
   *         return false
   *         else return {@link IMetaStoreClient#deleteSchema(String)}
   * @author mzy
   */
  @Override
  public boolean deleteSchema(String arg0) throws MetaException, TException {
    return !isNullOrEmpty(arg0) && client.deleteSchema(arg0);
  }

  /**
   * Delete Table node dist
   *
   * @param arg0
   *          db name
   * @param arg1
   *          table name
   * @param arg2
   *          node group
   * @return if the param is valid and
   *         {@link IMetaStoreClient#deleteTableNodeDist(String, String, List)} is success then
   *         return true
   *         else return false;
   * @author mzy
   */
  @Override
  public boolean deleteTableNodeDist(String arg0, String arg1,
      List<String> arg2) throws MetaException, TException {
    final boolean expr = isNullOrEmpty(arg0) || isNullOrEmpty(arg1) || arg2 == null;
    return !expr && client.deleteTableNodeDist(arg0, arg1, arg2);
  }

  /**
   * Delete User from db by userName and dbName
   *
   * @param userName
   * @param dbName
   * @return if userName and dbName is not null or empty and
   *         {@link IMetaStoreClient#deleteUserAssignment(String, String)} is success then return
   *         true
   *         else return false;
   * @author mzy
   */
  @Override
  public boolean deleteUserAssignment(String userName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    final boolean expr = isNullOrEmpty(userName) || isNullOrEmpty(dbName);
    return !expr && client.deleteUserAssignment(userName, dbName);
  }

  @Override
  public boolean delete_partition_column_statistics(String dbName, String tableName,
      String partName, String colName) throws NoSuchObjectException,
      MetaException, InvalidObjectException, InvalidInputException,
      TException {
    return client.deletePartitionColumnStatistics(dbName, tableName, partName, colName);
  }

  @Override
  public boolean delete_table_column_statistics(String dbName, String tableName,
      String colName) throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException, TException {
    return client.deleteTableColumnStatistics(dbName, tableName, colName);
  }

  @Override
  public void drop_attribution(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException,
      MetaException, TException {
    client.dropDatabase(name, deleteData, ignoreUnknownDb);
  }

  @Override
  public void drop_database(String name, boolean ifDeleteData, boolean ifIgnoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException,
      MetaException, TException {
    client.dropDatabase(name, ifDeleteData, ifIgnoreUnknownDb);
  }

  @Override
  public boolean drop_index_by_name(String dbName, String tableName, String indexName,
      boolean ifDeleteData) throws NoSuchObjectException, MetaException,
      TException {
    return client.dropIndex(dbName, tableName, indexName, ifDeleteData);
  }

  @Override
  public boolean drop_partition(String dbName, String tableName, List<String> partValues,
      boolean ifDeleteData) throws NoSuchObjectException, MetaException,
      TException {
    return client.dropPartition(dbName, tableName, partValues, ifDeleteData);
  }

  @Override
  public boolean drop_partition_by_name(String dbName, String tableName,
      String partName, boolean ifDelData) throws NoSuchObjectException,
      MetaException, TException {
    return client.dropPartition(dbName, tableName, partName, ifDelData);
  }

  @Override
  public int drop_partition_files(Partition part, List<SFile> files)
      throws TException {
    // TODO zy
    return 0;// client.drop_partition_files(part, files);
  }

  @Override
  public boolean drop_partition_index(Index index, Partition part)
      throws MetaException, AlreadyExistsException, TException {
    return client.drop_partition_index(index, part);
  }

  @Override
  public boolean drop_partition_index_files(Index index, Partition part,
      List<SFile> file) throws MetaException, TException {
    // TODO zy
    return false;// client.drop_partition_index_files(index, part, file);
  }

  @Override
  public boolean drop_role(String roleName) throws MetaException, TException {
    return client.drop_role(roleName);
  }

  @Override
  public int drop_subpartition_files(Subpartition subpart, List<SFile> files)
      throws TException {
    return client.drop_subpartition_files(subpart, files);
  }

  @Override
  public boolean drop_subpartition_index(Index index, Subpartition subpart)
      throws MetaException, AlreadyExistsException, TException {
    return client.drop_subpartition_index(index, subpart);
  }

  @Override
  public boolean drop_subpartition_index_files(Index index, Subpartition subpart,
      List<SFile> file) throws MetaException, TException {
    // TODO zy
    return false;// client.drop_subpartition_index_files(index, subpart, file);
  }

  @Override
  public void drop_table(String dbName, String tableName, boolean ifDelData)
      throws NoSuchObjectException, MetaException, TException {
    client.dropTable(dbName, tableName, ifDelData, true);
  }

  @Override
  public boolean drop_type(String type) throws MetaException,
      NoSuchObjectException, TException {
    // TODO mzy
    return rs.dropType(type);
  }

  @Override
  public boolean drop_user(String userName) throws NoSuchObjectException,
      MetaException, TException {
    return client.drop_user(userName);
  }

  @Override
  public List<SFile> filterTableFiles(String dbName, String tabName, List<SplitValue> values)
      throws MetaException, TException {
    return rs.filterTableFiles(dbName, tabName, values);
  }

  @Override
  public List<Node> find_best_nodes(int nr) throws MetaException,
      TException {
    try {
      List<Node> list = dm.findBestNodes(nr);
      return list;
    } catch (IOException e) {
      return Lists.newArrayList();
    }
  }

  @Override
  public List<Node> find_best_nodes_in_groups(String arg0, String arg1,
      int arg2, FindNodePolicy arg3) throws MetaException, TException {
    // return Lists.newArrayList(d);
    // TODO to know how to find the best nodes in grops
    return Lists.newArrayList();
  }

  @Override
  public String getDMStatus() throws MetaException, TException {

    return dm.getDMStatus();
  }

  @Override
  public GeoLocation getGeoLocationByName(String geoLocName) throws MetaException,
      NoSuchObjectException, TException {
    return rs.getGeoLocationByName(geoLocName);
  }

  @Override
  public List<GeoLocation> getGeoLocationByNames(List<String> geoLocNames)
      throws MetaException, TException {
    return rs.getGeoLocationByNames(geoLocNames);
  }

  @Override
  public String getMP(String node_name, String devid) throws MetaException,
      TException {
    return dm.getMP(node_name, devid);
  }

  @Override
  public long getMaxFid() throws MetaException, TException {
    return rs.getCurrentFID();
  }

  @Override
  public String getNodeInfo() throws MetaException, TException {
    // TODO rs doesn't implement this method
    return client.getNodeInfo();
  }

  @Override
  public GlobalSchema getSchemaByName(String schName)
      throws NoSuchObjectException, MetaException, TException {
    GlobalSchema gs = rs.getSchema(schName);
    return gs;
  }

  @Override
  public long getSessionId() throws MetaException, TException {
    return 0L;
  }

  @Override
  public List<SFile> getTableNodeFiles(String dbName, String tabName, String nodeName)
      throws MetaException, TException {
    throw new MetaException("Not implemented yet!");
  }

  @Override
  public List<NodeGroup> getTableNodeGroups(String dbName, String tabName)
      throws MetaException, TException {
    Table tbl = get_table(dbName, tabName);
    return tbl.getNodeGroups();
  }

  // 把所有本地缓存的database返回
  @Override
  public List<Database> get_all_attributions() throws MetaException,
      TException {
    List<Database> dbs = new ArrayList<Database>();
    dbs.addAll(CacheStore.getDatabaseHm().values());
    return dbs;
  }

  @Override
  public List<BusiTypeColumn> get_all_busi_type_cols() throws MetaException,
      TException {
    return rs.getAllBusiTypeCols();
  }

  @Override
  public List<BusiTypeDatacenter> get_all_busi_type_datacenters()
      throws MetaException, TException {
    return rs.get_all_busi_type_datacenters();
  }

  @Override
  public List<String> get_all_databases() throws MetaException, TException {
    List<String> dbNames = new ArrayList<String>();
    dbNames.addAll(CacheStore.getDatabaseHm().keySet());
    return dbNames;
  }

  @Override
  public List<Node> get_all_nodes() throws MetaException, TException {
    List<Node> ns = new ArrayList<Node>();
    ns.addAll(CacheStore.getNodeHm().values());
    return ns;
  }

  @Override
  public List<String> get_all_tables(String dbName) throws MetaException,
      TException {
    return rs.getAllTables(dbName);
  }

  @Override
  public Database get_attribution(String name) throws NoSuchObjectException,
      MetaException, TException {
    // zy
    return get_database(name);
  }

  @Override
  public String get_config_value(String name, String defaultValue)
      throws ConfigValSecurityException, TException {
    // TODO rs doesn't implement this method
    return client.getConfigValue(name, defaultValue);
  }

  @Override
  public Database get_database(String dbName) throws NoSuchObjectException,
      MetaException, TException {
    Database db = rs.getDatabase(dbName);
    return db;
  }

  @Override
  public List<String> get_databases(String pattern) throws MetaException,
      TException {
    return rs.getDatabases(pattern);
  }

  @Override
  public String get_delegation_token(String owner, String renewerKerberosPrincipalName)
      throws MetaException, TException {
    // TODO rs doesn't impelement this method
    return client.getDelegationToken(owner, renewerKerberosPrincipalName);
  }

  @Override
  public Device get_device(String devid) throws MetaException, NoSuchObjectException, TException {
    return rs.getDevice(devid);
  }

  @Override
  public List<FieldSchema> get_fields(String dbname, String tablename)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    Table t = rs.getTable(dbname, tablename);
    if (t == null) {
      throw new UnknownTableException("Table not found by name:" + dbname + "." + tablename);
    }
    return t.getSd().getCols();
  }

  @Override
  public SFile get_file_by_id(long fid) throws FileOperationException,
      MetaException, TException {
    // TODO Auto-generated method stub
    try {
      SFile f = rs.getSFile(fid);
      if (f == null) {
        throw new FileOperationException("File not found by id:" + fid, FOFailReason.INVALID_FILE);
      }
      return f;
    } catch (Exception e) {
      e.printStackTrace();
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public SFile get_file_by_name(String node, String devid, String location)
      throws FileOperationException, MetaException, TException {
    SFile f = rs.getSFile(devid, location);
    if (f == null) {
      throw new FileOperationException("Can not find SFile by name: " + node + ":" + devid + ":"
          + location, FOFailReason.INVALID_FILE);
    }
    return f;
  }

  @Override
  public Index get_index_by_name(String dbName, String tableName, String indexName)
      throws MetaException, NoSuchObjectException, TException {
    String key = dbName + "." + tableName + "." + indexName;
    Index ind = rs.getIndex(dbName, tableName, indexName);

    if (ind == null) {
      throw new NoSuchObjectException("Index not found by name:" + key);
    }
    return ind;
  }

  @Override
  public List<String> get_index_names(String dbName, String tblName, short arg2)
      throws MetaException, TException {
    List<String> indNames = new ArrayList<String>();
    for (String key : CacheStore.getIndexHm().keySet()) {
      String[] keys = key.split("\\.");
      if (dbName.equalsIgnoreCase(keys[0]) && tblName.equalsIgnoreCase(keys[1])) {
        indNames.add(keys[2]);
      }
    }
    return indNames;
  }

  @Override
  public List<Index> get_indexes(String dbName, String tblName, short arg2)
      throws NoSuchObjectException, MetaException, TException {
    List<Index> inds = new ArrayList<Index>();
    for (String key : CacheStore.getIndexHm().keySet()) {
      String[] keys = key.split("\\.");
      if (dbName.equalsIgnoreCase(keys[0]) && tblName.equalsIgnoreCase(keys[1])) {
        inds.add(CacheStore.getIndexHm().get(key));
      }
    }
    return inds;
  }

  @Override
  // MetaStoreClient 初始化时会调这个rpc
  public Database get_local_attribution() throws MetaException, TException {
    // TODO Auto-generated method stub
    String dbname = conf.getLocalDbName();
    try {
      Database db = rs.getDatabase(dbname);
      return db;
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
    // 如果返回null会抛异常
    // org.apache.thrift.TApplicationException: get_local_attribution failed: unknown result
  }

  @Override
  public List<String> get_lucene_index_names(String arg0, String arg1,
      short arg2) throws MetaException, TException {
    // TODO HiveMetaStore doesn't implement this method
    return Lists.newArrayList();
  }

  @Override
  public Node get_node(String nodeName) throws MetaException, TException {
    return rs.getNode(nodeName);
  }

  @Override
  public Partition get_partition(final String dbName, final String tableName,
      final List<String> partVals)
      throws MetaException, NoSuchObjectException, TException {
    return rs.getPartition(dbName, tableName, partVals);
  }

  @Override
  public Partition get_partition_by_name(String dbName, String tableName, String partName)
      throws MetaException, NoSuchObjectException, TException {
    return rs.getPartition(dbName, tableName, partName);
  }

  @Override
  public ColumnStatistics get_partition_column_statistics(String dbName,
      String tableName, String partitionName, String colName)
      throws NoSuchObjectException, MetaException, InvalidInputException,
      InvalidObjectException, TException {
    // TODO copy from HiveMetaStore
    return rs.getPartitionColumnStatistics(dbName, tableName, partitionName, null, colName);
  }

  @Override
  public List<SFileRef> get_partition_index_files(Index index, Partition part)
      throws MetaException, TException {
    return rs.getPartitionIndexFiles(index, part);
  }

  @Override
  public List<String> get_partition_names(String dbName, String tableName, short maxPart)
      throws MetaException, TException {
    return rs.listPartitionNames(dbName, tableName, maxPart);
  }

  @Override
  public List<String> get_partition_names_ps(String dbName, String tableName,
      List<String> partVals, short maxParts) throws MetaException,
      NoSuchObjectException, TException {
    return rs.listPartitionNamesPs(dbName, tableName, partVals, maxParts);
  }

  @Override
  public Partition get_partition_with_auth(String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, TException {
    return rs.getPartitionWithAuth(dbName, tableName, pvals, userName, groupNames);
  }

  @Override
  public List<Partition> get_partitions(String dbName, String tableName, short maxParts)
      throws NoSuchObjectException, MetaException, TException {
    return rs.getPartitions(dbName, tableName, maxParts);
  }

  @Override
  public List<Partition> get_partitions_by_filter(String dbName, String tblName,
      String filter, short maxParts) throws MetaException,
      NoSuchObjectException, TException {
    return rs.getPartitionsByFilter(dbName, tblName, filter, maxParts);
  }

  @Override
  public List<Partition> get_partitions_by_names(String tblName, String dbName,
      List<String> partVals) throws MetaException, NoSuchObjectException,
      TException {
    return rs.getPartitionsByNames(dbName, tblName, partVals);
  }

  @Override
  public List<Partition> get_partitions_ps(String dbName, String tableName,
      List<String> partVals, short maxParts) throws MetaException,
      NoSuchObjectException, TException {
    return rs.listPartitionsPsWithAuth(dbName, tableName, partVals, maxParts, null, null);
  }

  @Override
  public List<Partition> get_partitions_ps_with_auth(String dbName,
      String tblName, List<String> partVals, short maxParts, String userName,
      List<String> groupNames) throws NoSuchObjectException, MetaException,
      TException {
    return rs.listPartitionsPsWithAuth(dbName, tblName, partVals, maxParts, userName, groupNames);
  }

  @Override
  public List<Partition> get_partitions_with_auth(String dbName, String tblName,
      short maxParts, String userName, List<String> groupNames)
      throws NoSuchObjectException, MetaException, TException {
    return rs.getPartitionsWithAuth(dbName, tblName, maxParts, userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
      String userName, List<String> groupNames) throws MetaException, TException {
    if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
      String partName = getPartName(hiveObject);
      return rs.getColumnPrivilegeSet(hiveObject.getDbName(), hiveObject
          .getObjectName(), partName, hiveObject.getColumnName(), userName,
          groupNames);
    } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
      String partName = getPartName(hiveObject);
      return rs.getPartitionPrivilegeSet(hiveObject.getDbName(),
          hiveObject.getObjectName(), partName, userName, groupNames);
    } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
      return rs.getDBPrivilegeSet(hiveObject.getDbName(), userName,
          groupNames);
    } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
      return rs.getTablePrivilegeSet(hiveObject.getDbName(), hiveObject
          .getObjectName(), userName, groupNames);
    } else if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
      return rs.getUserPrivilegeSet(userName, groupNames);
    }
    return null;
  }
  private String getPartName(HiveObjectRef hiveObject) throws MetaException {
    String partName = null;
    List<String> partValue = hiveObject.getPartValues();
    if (partValue != null && partValue.size() > 0) {
      Table table = rs.getTable(hiveObject.getDbName(), hiveObject
          .getObjectName());
      partName = Warehouse
          .makePartName(table.getPartitionKeys(), partValue);
    }
    return partName;
  }
  @Override
  public List<String> get_role_names() throws MetaException, TException {
    return rs.listRoleNames();
  }

  @Override
  // 模仿HiveMetaStore中的方法写的
  public List<FieldSchema> get_schema(String dbname, String tablename)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    try {
      String baseTableName = tablename.split("\\.")[0];
      // Table baseTable = (Table) ms.readObject(ObjectType.TABLE, dbname+"."+baseTableName);
      Table baseTable = rs.getTable(dbname, baseTableName);
      if (baseTable == null) {
        throw new UnknownTableException("Table not found by name:" + baseTableName);
      }
      List<FieldSchema> fss = baseTable.getSd().getCols();
      if (baseTable.getPartitionKeys() != null) {
        fss.addAll(baseTable.getPartitionKeys());
      }

      return fss;
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public List<SFileRef> get_subpartition_index_files(Index index,
      Subpartition subpart) throws MetaException, TException {
    return rs.getSubpartitionIndexFiles(index, subpart);
  }

  @Override
  public List<Subpartition> get_subpartitions(String dbName, String tabName,
      Partition part) throws TException {
    return rs.getSubpartitions(dbName, tabName, part);
  }

  @Override
  public Table get_table(String dbname, String tablename) throws MetaException,
      NoSuchObjectException, TException {
    Table t = rs.getTable(dbname, tablename);
    return t;
  }

  @Override
  public ColumnStatistics get_table_column_statistics(String dbName,
      String tableName, String colName) throws NoSuchObjectException,
      MetaException, InvalidInputException, InvalidObjectException,
      TException {
    return rs.getTableColumnStatistics(dbName, tableName, colName);
  }

  @Override
  public List<String> get_table_names_by_filter(String dbName, String filter, short maxTables)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return rs.listTableNamesByFilter(dbName, filter, maxTables);
  }

  @Override
  public List<Table> get_table_objects_by_name(String dbname, List<String> names)
      throws MetaException, InvalidOperationException, UnknownDBException,
      TException {
    if (dbname == null || dbname.isEmpty()) {
      throw new UnknownDBException("DB name is null or empty");
    }
    if (names == null) {
      throw new InvalidOperationException("table names are null");
    }
    return rs.getTableObjectsByName(dbname, names);
  }

  @Override
  public List<String> get_tables(String dbName, String tablePattern)
      throws MetaException, TException {
    return rs.getTables(dbName, tablePattern);
  }

  @Override
  public Type get_type(String typeName) throws MetaException,
      NoSuchObjectException, TException {
    return rs.getType(typeName);
  }

  @Override
  public Map<String, Type> get_type_all(String arg0) throws MetaException,
      TException {
    //FIXME HiveMetaStore just do this
    throw new MetaException("not yet implemented");
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges) throws MetaException,
      TException {
    return rs.grantPrivileges(privileges);
  }

  @Override
  public boolean grant_role(String role, String userName, PrincipalType principalType,
      String grantor, PrincipalType grantorType, boolean grantOption)
      throws MetaException, TException {
    return rs.grantRole(rs.getRole(role), userName, principalType, grantor, grantorType, grantOption);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType)
      throws MetaException, NoSuchObjectException, UnknownDBException,
      UnknownTableException, UnknownPartitionException,
      InvalidPartitionException, TException {
    return rs.isPartitionMarkedForEvent(dbName, tblName, partName, evtType);
  }

  @Override
  public List<NodeGroup> listDBNodeGroups(String dbName) throws MetaException,
      TException {
    return rs.listDBNodeGroups(dbName);
  }

  @Override
  public List<EquipRoom> listEquipRoom() throws MetaException, TException {
    return rs.listEquipRoom();
  }

  @Override
  public List<Long> listFilesByDigest(String digest) throws MetaException,
      TException {

    return rs.findSpecificDigestFiles(digest);
  }

  @Override
  public List<GeoLocation> listGeoLocation() throws MetaException, TException {
    return rs.listGeoLocation();
  }

  @Override
  public List<NodeGroup> listNodeGroupByNames(List<String> ngNames)
      throws MetaException, TException {
    return rs.listNodeGroupByNames(ngNames);
  }

  @Override
  public List<NodeGroup> listNodeGroups() throws MetaException, TException {
    List<NodeGroup> ngs = new ArrayList<NodeGroup>();
    ngs.addAll(CacheStore.getNodeGroupHm().values());
    return ngs;
  }

  @Override
  public List<Node> listNodes() throws MetaException, TException {
    List<Node> ng = new ArrayList<Node>();
    ng.addAll(CacheStore.getNodeHm().values());
    return ng;
  }

  @Override
  public List<Role> listRoles() throws MetaException, TException {
    return rs.listRoles();
  }

  @Override
  public List<GlobalSchema> listSchemas() throws MetaException, TException {
    List<GlobalSchema> gss = new ArrayList<GlobalSchema>();
    gss.addAll(CacheStore.getGlobalSchemaHm().values());
    return gss;
  }

  /*
   * void setRange(long fromIncl, long toExcl)
   *
   * Set the range of results to return. The execution of the query is modified to return only a
   * subset of results. If the filter would normally return 100 instances, and fromIncl is set to
   * 50,
   * and toExcl is set to 70, then the first 50 results that would have been returned are skipped,
   * the next 20 results are returned and the remaining 30 results are ignored. An implementation
   * should
   * execute the query such that the range algorithm is done at the data store.
   *
   * Parameters:
   * fromIncl - 0-based inclusive start index
   * toExcl - 0-based exclusive end index, or Long.MAX_VALUE for no limit.
   * Since:
   * 2.0
   *
   * 返回的结果集中包含from，不包含to，一共返回from-to个元素
   * 而zrange的两边是inclusive
   */
  @Override
  public List<Long> listTableFiles(String dbName, String tabName, int from, int to)
      throws MetaException, TException {

    return rs.listTableFiles(dbName, tabName, from, to - 1);
  }

  @Override
  public List<NodeGroup> listTableNodeDists(String dbName, String tabName)
      throws MetaException, TException {
    Table t = get_table(dbName, tabName);
    if (t == null) {
      throw new MetaException("No table found by dbname:" + dbName + ", tableName:" + tabName);
    }
    return t.getNodeGroups();
  }

  @Override
  public List<User> listUsers() throws MetaException, TException {
    return rs.listUsers();
  }

  @Override
  public List<Device> list_device() throws MetaException, TException {
    return rs.listDevice();
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String arg0,
      PrincipalType arg1, HiveObjectRef arg2) throws MetaException,
      TException {
    //TODO implement this method
    return null;
  }

  @Override
  public List<Role> list_roles(String arg0, PrincipalType arg1)
      throws MetaException, TException {
    return rs.listRoles();
  }

  @Override
  public List<String> list_users(Database dbName) throws MetaException,
      TException {
    return rs.listUsersNames(dbName.getName());
  }

  @Override
  public List<String> list_users_names() throws MetaException, TException {
    return rs.listUsersNames();
  }

  @Override
  public void markPartitionForEvent(String dbName, String tblName,
      Map<String, String> partVals, PartitionEventType evtType)
      throws MetaException, NoSuchObjectException, UnknownDBException,
      UnknownTableException, UnknownPartitionException,
      InvalidPartitionException, TException {
    rs.markPartitionForEvent(dbName, tblName, partVals, evtType);
  }

  @Override
  public boolean migrate2_in(Table tbl, List<Partition> parts,
      List<Index> idxs, String from_dc, String to_nas_devid,
      Map<Long, SFileLocation> fileMap) throws MetaException, TException {
    return client.migrate2_in(tbl, parts, idxs, from_dc, to_nas_devid, fileMap);
  }

  @Override
  public List<SFileLocation> migrate2_stage1(String dbName, String tableName,
      List<String> partNames, String to_dc) throws MetaException, TException {
    return client.migrate2_stage1(dbName, tableName, partNames, to_dc);
  }

  @Override
  public boolean migrate2_stage2(String dbName, String tableName, List<String> partNames,
      String to_dc, String to_db, String to_nas_devid) throws MetaException,
      TException {
    return client.migrate2_stage2(dbName, tableName, partNames, to_dc, to_db, to_nas_devid);
  }

  @Override
  public boolean migrate_in(Table tbl, Map<Long, SFile> files,
      List<Index> idxs, String from_db, String to_devid,
      Map<Long, SFileLocation> fileMap) throws MetaException, TException {
    return client.migrate_in(tbl, files, idxs, from_db, to_devid, fileMap);
  }

  @Override
  public List<SFileLocation> migrate_stage1(String dbName, String tableName,
      List<Long> partNames, String to_dc) throws MetaException, TException {
    return client.migrate_stage1(dbName, tableName, partNames, to_dc);
  }

  @Override
  public boolean migrate_stage2(String dbName, String tableName, List<Long> files,
      String from_db, String to_db, String to_devid, String user, String password)
      throws MetaException, TException {
    return client.migrate_stage2(dbName, tableName, files, from_db, to_db, to_devid, user, password);
  }

  @Override
  public boolean modifyEquipRoom(EquipRoom er) throws MetaException,
      TException {
    return rs.modifyEquipRoom(er);
  }

  @Override
  public boolean modifyGeoLocation(GeoLocation gl) throws MetaException,
      TException {
    return rs.modifyGeoLocation(gl);
  }

  @Override
  public boolean modifyNodeGroup(String schemaName, NodeGroup ng)
      throws MetaException, TException {
    return rs.modifyNodeGroup(schemaName, ng);
  }

  @Override
  public boolean modifySchema(String schemaName, GlobalSchema schema)
      throws MetaException, TException {
    return rs.modifySchema(schemaName, schema);
  }

  @Override
  public Device modify_device(Device dev, Node node) throws MetaException,
      TException {
    return rs.modifyDevice(dev, node);
  }

  @Override
  public boolean modify_user(User user) throws NoSuchObjectException,
      MetaException, TException {
    return rs.modifyUser(user);
  }

  @Override
  public boolean offline_filelocation(SFileLocation sfl)
      throws MetaException, TException {
    //FIXME add startFunction and endFunction
    sfl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.OFFLINE);
    rs.updateSFileLocation(sfl);
    return true;
  }

  @Override
  public boolean online_filelocation(SFile file) throws MetaException,
      TException {
 // reget the file now
    SFile stored_file = get_file_by_id(file.getFid());

    if (stored_file.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
      throw new MetaException("online filelocation can only do on INCREATE file " + file.getFid() +
          " STATE: " + stored_file.getStore_status());
    }
    if (stored_file.getLocationsSize() != 1) {
      throw new MetaException("Invalid file location in SFile fid: " + stored_file.getFid());
    }
    SFileLocation sfl = stored_file.getLocations().get(0);
    assert sfl != null;
    sfl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
    rs.updateSFileLocation(sfl);
    return false;
  }

  @Override
  public Map<String, String> partition_name_to_spec(String part_name)
      throws MetaException, TException {
    if (part_name.length() == 0) {
      return new HashMap<String, String>();
    }
    return Warehouse.makeSpecFromName(part_name);
  }

  @Override
  public List<String> partition_name_to_vals(String part_name)
      throws MetaException, TException {
    if (part_name.length() == 0) {
      return new ArrayList<String>();
    }
    LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(part_name);
    List<String> part_vals = new ArrayList<String>();
    part_vals.addAll(map.values());
    return part_vals;
  }

  @Override
  public String pingPong(String str) throws MetaException, TException {
    return str;
  }

  @Override
  public void rename_partition(String dbname, String name, List<String> part_vals,
      Partition newPart) throws InvalidOperationException, MetaException,
      TException {
    //FIXME just use client
    client.renamePartition(dbname, name, part_vals, newPart);
  }

  @Override
  public long renew_delegation_token(String tokenStrForm) throws MetaException,
      TException {
    return client.renewDelegationToken(tokenStrForm);
  }

  @Override
  public boolean reopen_file(long arg0) throws FileOperationException,
      MetaException, TException {
    return false;
  }

  @Override
  public int restore_file(SFile arg0) throws FileOperationException,
      MetaException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges) throws MetaException,
      TException {
    return rs.revokePrivileges(privileges);
  }

  @Override
  public boolean revoke_role(String role, String userName, PrincipalType principalType)
      throws MetaException, TException {
        return rs.revokeRole(rs.getRole(role), userName, principalType);
  }

  @Override
  public int rm_file_logical(SFile file) throws FileOperationException,
      MetaException, TException {
    DMProfile.frmlR.incrementAndGet();
    SFile saved = rs.getSFile(file.getFid());
    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID" + file.getFid(), FOFailReason.INVALID_FILE);
    }

    // only in REPLICATED state can step into RM_LOGICAL
    if (saved.getStore_status() != MetaStoreConst.MFileStoreStatus.REPLICATED) {
      throw new FileOperationException("File StoreStatus is not in REPLICATED.", FOFailReason.INVALID_STATE);
    }
    saved.setStore_status(MetaStoreConst.MFileStoreStatus.RM_LOGICAL);
    rs.updateSFile(saved);
    return 0;
  }

  @Override
  public int rm_file_physical(SFile file) throws FileOperationException,
      MetaException, TException {
    DMProfile.frmpR.incrementAndGet();
    SFile saved = rs.getSFile(file.getFid());
    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID " + file.getFid(), FOFailReason.INVALID_FILE);
    }

    if (!(saved.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE ||
        saved.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED ||
        saved.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_LOGICAL)) {
      throw new FileOperationException("File StoreStatus is not in INCREATE/REPLICATED/RM_LOGICAL.", FOFailReason.INVALID_STATE);
    }
    saved.setStore_status(MetaStoreConst.MFileStoreStatus.RM_PHYSICAL);
    file = rs.updateSFile(saved);
    file.setLocations(rs.getSFileLocations(file.getFid()));
    synchronized (dm.cleanQ) {
      dm.cleanQ.add(new DMRequest(file, DMRequest.DMROperation.RM_PHYSICAL, 0));
      dm.cleanQ.notify();
    }
    return 0;
  }

  @Override
  public void set_file_repnr(long arg0, int arg1)
      throws FileOperationException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean set_loadstatus_bad(long arg0) throws MetaException,
      TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> set_ugi(String arg0, List<String> arg1)
      throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Busitype> showBusitypes() throws InvalidObjectException,
      MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public statfs statFileSystem(long arg0, long arg1) throws MetaException,
      TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean toggle_safemode() throws MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void truncTableFiles(String arg0, String arg1) throws MetaException,
      TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void update_attribution(Database arg0) throws NoSuchObjectException,
      InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean update_partition_column_statistics(ColumnStatistics arg0)
      throws NoSuchObjectException, InvalidObjectException,
      MetaException, InvalidInputException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean update_table_column_statistics(ColumnStatistics arg0)
      throws NoSuchObjectException, InvalidObjectException,
      MetaException, InvalidInputException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean user_authority_check(User arg0, Table arg1,
      List<MSOperation> arg2) throws MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean del_filelocation(SFileLocation slf) throws MetaException,
      TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<SFile> get_files_by_ids(List<Long> fids)
      throws FileOperationException, MetaException, TException {
    List<SFile> fl = new ArrayList<SFile>();
    for (Long fid : fids)
    {
      SFile sf = this.get_file_by_id(fid);
      fl.add(sf);
    }
    return fl;
  }

  // @Override
  // public int del_fileLocation(SFileLocation arg0)
  // throws FileOperationException, MetaException, TException {
  // // TODO Auto-generated method stub
  // return 0;
  // }
  //

}
