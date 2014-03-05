package org.apache.hadoop.hive.metastore.newms;

import iie.metastore.MetaStoreClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.RawStore;
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
import org.apache.thrift.TException;

import com.facebook.fb303.fb_status;

/*
 * 没缓存的对象，但是rpc里要得到的
 * BusiTypeColumn，Device,ColumnStatistics,Type,role,User,HiveObjectPrivilege
 */

public class ThriftRPC implements org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface{

	private NewMSConf conf;
	private RawStore ms;
	private MetaStoreClient msClient;
	public ThriftRPC(NewMSConf conf)
	{
		this.conf = conf;
		ms = new RawStoreImp(conf);
//		try {
//			msClient = new MetaStoreClient(conf.getMshost(), conf.getMsport());
//		} catch (MetaException e) {
//			e.printStackTrace();
//		}
	}
	@Override
	public long aliveSince() throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getCounter(String arg0) throws TException {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getOption(String arg0) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getOptions() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public fb_status getStatus() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStatusDetails() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getVersion() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reinitialize() throws TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setOption(String arg0, String arg1) throws TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdown() throws TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean addEquipRoom(EquipRoom arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addGeoLocation(GeoLocation arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addNodeAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addNodeGroup(NodeGroup arg0) throws AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addNodeGroupAssignment(NodeGroup arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addRoleAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addTableNodeDist(String arg0, String arg1, List<String> arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addUserAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean add_datawarehouse_sql(int arg0, String arg1)
			throws InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Index add_index(Index arg0, Table arg1)
			throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Node add_node(String arg0, List<String> arg1) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition add_partition(Partition arg0)
			throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int add_partition_files(Partition arg0, List<SFile> arg1)
			throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean add_partition_index(Index arg0, Partition arg1)
			throws MetaException, AlreadyExistsException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean add_partition_index_files(Index arg0, Partition arg1,
			List<SFile> arg2, List<Long> arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Partition add_partition_with_environment_context(Partition arg0,
			EnvironmentContext arg1) throws InvalidObjectException,
			AlreadyExistsException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int add_partitions(List<Partition> arg0)
			throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean add_subpartition(String arg0, String arg1,
			List<String> arg2, Subpartition arg3) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int add_subpartition_files(Subpartition arg0, List<SFile> arg1)
			throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean add_subpartition_index(Index arg0, Subpartition arg1)
			throws MetaException, AlreadyExistsException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean add_subpartition_index_files(Index arg0, Subpartition arg1,
			List<SFile> arg2, List<Long> arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean alterNodeGroup(NodeGroup arg0)
			throws AlreadyExistsException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void alter_database(String arg0, Database arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_index(String arg0, String arg1, String arg2, Index arg3)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Node alter_node(String arg0, List<String> arg1, int arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void alter_partition(String arg0, String arg1, Partition arg2)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_partition_with_environment_context(String arg0,
			String arg1, Partition arg2, EnvironmentContext arg3)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_partitions(String arg0, String arg1, List<Partition> arg2)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_table(String arg0, String arg1, Table arg2)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_table_with_environment_context(String arg0, String arg1,
			Table arg2, EnvironmentContext arg3)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void append_busi_type_datacenter(BusiTypeDatacenter arg0)
			throws InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Partition append_partition(String arg0, String arg1,
			List<String> arg2) throws InvalidObjectException,
			AlreadyExistsException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition append_partition_by_name(String arg0, String arg1,
			String arg2) throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean assiginSchematoDB(String arg0, String arg1,
			List<FieldSchema> arg2, List<FieldSchema> arg3, List<NodeGroup> arg4)
			throws InvalidObjectException, NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean authentication(String arg0, String arg1)
			throws NoSuchObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void cancel_delegation_token(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int close_file(SFile arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int createBusitype(Busitype arg0) throws InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean createSchema(GlobalSchema arg0)
			throws AlreadyExistsException, InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void create_attribution(Database arg0)
			throws AlreadyExistsException, InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void create_database(Database arg0) throws AlreadyExistsException,
			InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Device create_device(String arg0, int arg1, String arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SFile create_file(String arg0, int arg1, String arg2, String arg3,
			List<SplitValue> arg4) throws FileOperationException, TException {
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
	public boolean create_role(Role arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void create_table(Table arg0) throws AlreadyExistsException,
			InvalidObjectException, MetaException, NoSuchObjectException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void create_table_by_user(Table arg0, User arg1)
			throws AlreadyExistsException, InvalidObjectException,
			MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void create_table_with_environment_context(Table arg0,
			EnvironmentContext arg1) throws AlreadyExistsException,
			InvalidObjectException, MetaException, NoSuchObjectException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean create_type(Type arg0) throws AlreadyExistsException,
			InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean create_user(User arg0) throws InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean del_device(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int del_node(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean deleteEquipRoom(EquipRoom arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteGeoLocation(GeoLocation arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteNodeAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteNodeGroup(NodeGroup arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteNodeGroupAssignment(NodeGroup arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteRoleAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteSchema(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteTableNodeDist(String arg0, String arg1,
			List<String> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteUserAssignment(String arg0, String arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean delete_partition_column_statistics(String arg0, String arg1,
			String arg2, String arg3) throws NoSuchObjectException,
			MetaException, InvalidObjectException, InvalidInputException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean delete_table_column_statistics(String arg0, String arg1,
			String arg2) throws NoSuchObjectException, MetaException,
			InvalidObjectException, InvalidInputException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void drop_attribution(String arg0, boolean arg1, boolean arg2)
			throws NoSuchObjectException, InvalidOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void drop_database(String arg0, boolean arg1, boolean arg2)
			throws NoSuchObjectException, InvalidOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean drop_index_by_name(String arg0, String arg1, String arg2,
			boolean arg3) throws NoSuchObjectException, MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_partition(String arg0, String arg1, List<String> arg2,
			boolean arg3) throws NoSuchObjectException, MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_partition_by_name(String arg0, String arg1,
			String arg2, boolean arg3) throws NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int drop_partition_files(Partition arg0, List<SFile> arg1)
			throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean drop_partition_index(Index arg0, Partition arg1)
			throws MetaException, AlreadyExistsException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_partition_index_files(Index arg0, Partition arg1,
			List<SFile> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_role(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int drop_subpartition_files(Subpartition arg0, List<SFile> arg1)
			throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean drop_subpartition_index(Index arg0, Subpartition arg1)
			throws MetaException, AlreadyExistsException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_subpartition_index_files(Index arg0, Subpartition arg1,
			List<SFile> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void drop_table(String arg0, String arg1, boolean arg2)
			throws NoSuchObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean drop_type(String arg0) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean drop_user(String arg0) throws NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public List<SFile> filterTableFiles(String dbName, String tabName, List<SplitValue> values) throws MetaException, TException {
		return ms.filterTableFiles(dbName, tabName, values);
	}

	@Override
	public List<Node> find_best_nodes(int arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Node> find_best_nodes_in_groups(String arg0, String arg1,
			int arg2, FindNodePolicy arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDMStatus() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GeoLocation getGeoLocationByName(String arg0) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<GeoLocation> getGeoLocationByNames(List<String> arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getMP(String arg0, String arg1) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getMaxFid() throws MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getNodeInfo() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GlobalSchema getSchemaByName(String schName)
			throws NoSuchObjectException, MetaException, TException {
			GlobalSchema gs = ms.getSchema(schName);
			return gs;
	}

	@Override
	public long getSessionId() throws MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
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

	//把所有本地缓存的database返回
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<BusiTypeDatacenter> get_all_busi_type_datacenters()
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_all_databases() throws MetaException, TException {
		List<String> dbNames = new ArrayList<String>();
		dbNames.addAll(CacheStore.getDatabaseHm().keySet());
//		for(String key : MetadataStorage.getDatabaseHm().keySet()){
//			try {
//				Database db = (Database)ms.readObject(ObjectType.DATABASE, key);
//				dbNames.add(db.getName());
//			} catch (JedisConnectionException e) {
//				e.printStackTrace();
//			} catch (IOException e) {
//				e.printStackTrace();
//			} catch (ClassNotFoundException e) {
//				e.printStackTrace();
//			}
//		}
		return dbNames;
	}

	@Override
	public List<Node> get_all_nodes() throws MetaException, TException {
		List<Node> ns = new ArrayList<Node>();
		ns.addAll(CacheStore.getNodeHm().values());
		return ns;
	}

	@Override
	public List<String> get_all_tables(String dbname) throws MetaException,
			TException {
		return ms.getAllTables(dbname);
	}

	@Override
	public Database get_attribution(String name) throws NoSuchObjectException,
			MetaException, TException {
		return get_database(name);
	}

	@Override
	public String get_config_value(String arg0, String arg1)
			throws ConfigValSecurityException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Database get_database(String dbName) throws NoSuchObjectException,
			MetaException, TException {
			Database db = ms.getDatabase(dbName);
			return db;
	}

	@Override
	public List<String> get_databases(String pattern) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String get_delegation_token(String arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Device get_device(String devid) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<FieldSchema> get_fields(String dbname, String tablename)
			throws MetaException, UnknownTableException, UnknownDBException, TException {
			Table t = ms.getTable(dbname, tablename);
			if(t == null)
				throw new UnknownTableException("Table not found by name:"+dbname+"."+tablename);
			return t.getSd().getCols();
	}

	@Override
	public SFile get_file_by_id(long fid) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		try {
			SFile f = ms.getSFile(fid);
			if(f == null)
				throw new FileOperationException("File not found by id:"+fid, FOFailReason.INVALID_FILE);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public SFile get_file_by_name(String node, String devid, String location)
			throws FileOperationException, MetaException, TException {
		SFile f = ms.getSFile(devid, location);
		if(f == null)
			throw new FileOperationException("Can not find SFile by name: " + node + ":" + devid + ":" + location, FOFailReason.INVALID_FILE);
		return f;
	}

	@Override
	public Index get_index_by_name(String dbName, String tableName, String indexName)
			throws MetaException, NoSuchObjectException, TException {
			String key = dbName + "." + tableName + "." + indexName;
			Index ind = ms.getIndex(dbName, tableName, indexName);
			
			if(ind == null)
				throw new NoSuchObjectException("Index not found by name:"+key);
			return ind;
	}

	@Override
	public List<String> get_index_names(String dbName, String tblName, short arg2)
			throws MetaException, TException {
		List<String> indNames = new ArrayList<String>();
		for(String key : CacheStore.getIndexHm().keySet()){
			String[] keys = key.split("\\.");
			if(dbName.equalsIgnoreCase(keys[0]) && tblName.equalsIgnoreCase(keys[1])){
				indNames.add(keys[2]);
			}
		}
		return indNames;
	}

	@Override
	public List<Index> get_indexes(String dbName, String tblName, short arg2)
			throws NoSuchObjectException, MetaException, TException {
		List<Index> inds = new ArrayList<Index>();
		for(String key : CacheStore.getIndexHm().keySet()){
			String[] keys = key.split("\\.");
			if(dbName.equalsIgnoreCase(keys[0]) && tblName.equalsIgnoreCase(keys[1])){
				inds.add(CacheStore.getIndexHm().get(key));
			}
		}
		return inds;
	}

	@Override
	//MetaStoreClient 初始化时会调这个rpc
	public Database get_local_attribution() throws MetaException, TException {
		// TODO Auto-generated method stub
		String dbname = conf.getLocalDbName();
		try {
			Database db = ms.getDatabase(dbname);
			return db;
		} catch (Exception e) {
			throw new MetaException(e.getMessage());
		}
		//如果返回null会抛异常
		//org.apache.thrift.TApplicationException: get_local_attribution failed: unknown result
	}

	@Override
	public List<String> get_lucene_index_names(String arg0, String arg1,
			short arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Node get_node(String node_name) throws MetaException, TException {
		// TODO Auto-generated method stub
			Node n = ms.getNode(node_name);
			return n;
	}

	@Override
	public Partition get_partition(final String dbname, final String tabname,
	        final List<String> partVals)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition get_partition_by_name(String arg0, String arg1, String arg2)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnStatistics get_partition_column_statistics(String arg0,
			String arg1, String arg2, String arg3)
			throws NoSuchObjectException, MetaException, InvalidInputException,
			InvalidObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<SFileRef> get_partition_index_files(Index arg0, Partition arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_partition_names(String arg0, String arg1, short arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_partition_names_ps(String arg0, String arg1,
			List<String> arg2, short arg3) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition get_partition_with_auth(String arg0, String arg1,
			List<String> arg2, String arg3, List<String> arg4)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions(String arg0, String arg1, short arg2)
			throws NoSuchObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_by_filter(String arg0, String arg1,
			String arg2, short arg3) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_by_names(String arg0, String arg1,
			List<String> arg2) throws MetaException, NoSuchObjectException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_ps(String arg0, String arg1,
			List<String> arg2, short arg3) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_ps_with_auth(String arg0,
			String arg1, List<String> arg2, short arg3, String arg4,
			List<String> arg5) throws NoSuchObjectException, MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_with_auth(String arg0, String arg1,
			short arg2, String arg3, List<String> arg4)
			throws NoSuchObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef arg0,
			String arg1, List<String> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_role_names() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	//模仿HiveMetaStore中的方法写的
	public List<FieldSchema> get_schema(String dbname, String tablename)
			throws MetaException, UnknownTableException, UnknownDBException, TException {
		try{
			String baseTableName = tablename.split("\\.")[0];
//			Table baseTable = (Table) ms.readObject(ObjectType.TABLE, dbname+"."+baseTableName);
			Table baseTable = ms.getTable(dbname, baseTableName);
			if(baseTable == null)
				throw new UnknownTableException("Table not found by name:"+baseTableName);
			List<FieldSchema> fss = baseTable.getSd().getCols();
			if(baseTable.getPartitionKeys() != null)
				fss.addAll(baseTable.getPartitionKeys());
			
			return fss;
		}catch(Exception e){
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public List<SFileRef> get_subpartition_index_files(Index arg0,
			Subpartition arg1) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Subpartition> get_subpartitions(String arg0, String arg1,
			Partition arg2) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Table get_table(String dbname, String tablename) throws MetaException,
			NoSuchObjectException, TException {
			Table t = ms.getTable(dbname, tablename);
			return t;
	}

	@Override
	public ColumnStatistics get_table_column_statistics(String arg0,
			String arg1, String arg2) throws NoSuchObjectException,
			MetaException, InvalidInputException, InvalidObjectException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_table_names_by_filter(String dbName, String filter, short maxTables) 
			throws MetaException, InvalidOperationException, UnknownDBException, TException {
		// TODO Auto-generated method stub
		return null;
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
		return ms.getTableObjectsByName(dbname, names);
	}

	@Override
	public List<String> get_tables(String arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Type get_type(String arg0) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Type> get_type_all(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean grant_privileges(PrivilegeBag arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean grant_role(String arg0, String arg1, PrincipalType arg2,
			String arg3, PrincipalType arg4, boolean arg5)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isPartitionMarkedForEvent(String arg0, String arg1,
			Map<String, String> arg2, PartitionEventType arg3)
			throws MetaException, NoSuchObjectException, UnknownDBException,
			UnknownTableException, UnknownPartitionException,
			InvalidPartitionException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<NodeGroup> listDBNodeGroups(String dbName) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<EquipRoom> listEquipRoom() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Long> listFilesByDigest(String digest) throws MetaException,
			TException {
		
		return ms.findSpecificDigestFiles(digest);
	}

	@Override
	public List<GeoLocation> listGeoLocation() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<NodeGroup> listNodeGroupByNames(List<String> ngNames)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		
		return ms.listNodeGroupByNames(ngNames);
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<GlobalSchema> listSchemas() throws MetaException, TException {
		List<GlobalSchema> gss = new ArrayList<GlobalSchema>(); 
		gss.addAll(CacheStore.getGlobalSchemaHm().values());
		return gss;
	}

	/*
	void setRange(long fromIncl, long toExcl)

    Set the range of results to return. The execution of the query is modified to return only a 
    subset of results. If the filter would normally return 100 instances, and fromIncl is set to 50, 
    and toExcl is set to 70, then the first 50 results that would have been returned are skipped, 
    the next 20 results are returned and the remaining 30 results are ignored. An implementation should 
    execute the query such that the range algorithm is done at the data store.

    Parameters:
        fromIncl - 0-based inclusive start index
        toExcl - 0-based exclusive end index, or Long.MAX_VALUE for no limit.
    Since:
        2.0
	
	返回的结果集中包含from，不包含to，一共返回from-to个元素
	而zrange的两边是inclusive
	 */
	@Override
	public List<Long> listTableFiles(String dbName, String tabName, int from, int to) 
			throws MetaException, TException {
		
		return ms.listTableFiles(dbName, tabName, from, to-1);
	}

	@Override
	public List<NodeGroup> listTableNodeDists(String dbName, String tabName)
			throws MetaException, TException {
		Table t = get_table(dbName, tabName);
		if(t == null)
			throw new MetaException("No table found by dbname:"+dbName+", tableName:"+tabName);
		return t.getNodeGroups();
	}

	@Override
	public List<User> listUsers() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Device> list_device() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HiveObjectPrivilege> list_privileges(String arg0,
			PrincipalType arg1, HiveObjectRef arg2) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Role> list_roles(String arg0, PrincipalType arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> list_users(Database arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> list_users_names() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void markPartitionForEvent(String arg0, String arg1,
			Map<String, String> arg2, PartitionEventType arg3)
			throws MetaException, NoSuchObjectException, UnknownDBException,
			UnknownTableException, UnknownPartitionException,
			InvalidPartitionException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean migrate2_in(Table arg0, List<Partition> arg1,
			List<Index> arg2, String arg3, String arg4,
			Map<Long, SFileLocation> arg5) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<SFileLocation> migrate2_stage1(String arg0, String arg1,
			List<String> arg2, String arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean migrate2_stage2(String arg0, String arg1, List<String> arg2,
			String arg3, String arg4, String arg5) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean migrate_in(Table arg0, Map<Long, SFile> arg1,
			List<Index> arg2, String arg3, String arg4,
			Map<Long, SFileLocation> arg5) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<SFileLocation> migrate_stage1(String arg0, String arg1,
			List<Long> arg2, String arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean migrate_stage2(String arg0, String arg1, List<Long> arg2,
			String arg3, String arg4, String arg5, String arg6, String arg7)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifyEquipRoom(EquipRoom arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifyGeoLocation(GeoLocation arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifyNodeGroup(String arg0, NodeGroup arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifySchema(String arg0, GlobalSchema arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Device modify_device(Device arg0, Node arg1) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean modify_user(User arg0) throws NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean offline_filelocation(SFileLocation arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean online_filelocation(SFile arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, String> partition_name_to_spec(String arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> partition_name_to_vals(String arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String pingPong(String str) throws MetaException, TException {
		return str;
	}

	@Override
	public void rename_partition(String arg0, String arg1, List<String> arg2,
			Partition arg3) throws InvalidOperationException, MetaException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long renew_delegation_token(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean reopen_file(long arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int restore_file(SFile arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean revoke_privileges(PrivilegeBag arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean revoke_role(String arg0, String arg1, PrincipalType arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int rm_file_logical(SFile arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int rm_file_physical(SFile arg0) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		return null;
	}
	
//	@Override
//	public int del_fileLocation(SFileLocation arg0)
//			throws FileOperationException, MetaException, TException {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//	

}
