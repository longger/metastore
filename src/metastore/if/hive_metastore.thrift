#!/usr/local/bin/thrift -java

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

#
# Thrift Service that the MetaStore is built on
#

include "share/fb303/if/fb303.thrift"

namespace java org.apache.hadoop.hive.metastore.api
namespace php metastore
namespace cpp Apache.Hadoop.Hive

const string DDL_TIME = "transient_lastDdlTime"

struct Version {
  1: string version,
  2: string comments
}

struct FieldSchema {
  1: string name, // name of the field
  2: string type, // type of the field. primitive types defined above, specify list<TYPE_NAME>, map<TYPE_NAME, TYPE_NAME> for lists & maps
  3: string comment,
  4: optional i64	version,
}


struct Type {
  1: string          name,             // one of the types in PrimitiveTypes or CollectionTypes or User defined types
  2: optional string type1,            // object type if the name is 'list' (LIST_TYPE), key type if the name is 'map' (MAP_TYPE)
  3: optional string type2,            // val type if the name is 'map' (MAP_TYPE)
  4: optional list<FieldSchema> fields // if the name is one of the user defined types
}

enum HiveObjectType {
  GLOBAL = 1,
  DATABASE = 2,
  TABLE = 3,
  PARTITION = 4,
  COLUMN = 5,
  SCHEMA = 6,
}

enum PrincipalType {
  USER = 1,
  ROLE = 2,
  GROUP = 3,
}

const string HIVE_FILTER_FIELD_OWNER = "hive_filter_field_owner__"
const string HIVE_FILTER_FIELD_PARAMS = "hive_filter_field_params__"
const string HIVE_FILTER_FIELD_LAST_ACCESS = "hive_filter_field_last_access__"

enum PartitionEventType {
  LOAD_DONE = 1,  
}

enum FOFailReason {
  INVALID_NODE = 1,
  INVALID_TABLE = 2,
  INVALID_FILE = 3,
  INVALID_SPLIT_VALUES = 4,
  INVALID_ATTRIBUTION = 5,
  INVALID_NODE_GROUPS = 6,
  
  NOSPACE = 10,
  NOTEXIST = 11,
  
  SAFEMODE = 12,
  INVALID_STATE = 13,
  
  TRY_AGAIN = 14,
}

struct HiveObjectRef{
  1: HiveObjectType objectType,
  2: string dbName,
  3: string objectName,
  4: list<string> partValues,
  5: string columnName,
}

struct PrivilegeGrantInfo {
  1: string privilege,
  2: i32 createTime,
  3: string grantor,
  4: PrincipalType grantorType,
  5: bool grantOption,
}

struct HiveObjectPrivilege {
  1: HiveObjectRef  hiveObject,
  2: string principalName,
  3: PrincipalType principalType,
  4: PrivilegeGrantInfo grantInfo,
}

struct PrivilegeBag {
  1: list<HiveObjectPrivilege> privileges,
}

struct PrincipalPrivilegeSet {
  1: map<string, list<PrivilegeGrantInfo>> userPrivileges, // user name -> privilege grant info
  2: map<string, list<PrivilegeGrantInfo>> groupPrivileges, // group name -> privilege grant info
  3: map<string, list<PrivilegeGrantInfo>> rolePrivileges, //role name -> privilege grant info
}

enum FindNodePolicy {
	ALL_NGS = 1,
	SINGLE_NG = 2,
}

enum MSOperation {
	EXPLAIN = 1,
	CREATEDATABASE = 2,
	DROPDATABASE = 3,
	DROPTABLE = 4,
	DESCTABLE = 5,
	ALTERTABLE_RENAME = 6,
	ALTERTABLE_RENAMECOL = 7,
	ALTERTABLE_ADDPARTS = 8,
	ALTERTABLE_DROPPARTS = 9,
	ALTERTABLE_ADDCOLS = 10,
	ALTERTABLE_REPLACECOLS = 11,
	ALTERTABLE_RENAMEPART = 12,
	ALTERTABLE_PROPERTIES = 13,
	SHOWDATABASES = 14,
	SHOWTABLES = 15,
	SHOWCOLUMNS = 16,
	SHOW_TABLESTATUS = 17,
	SHOW_TBLPROPERTIES = 18,
	SHOW_CREATETABLE = 19,
	SHOWINDEXES = 20,
	SHOWPARTITIONS = 21,
	CREATEVIEW = 22,
	DROPVIEW = 23,
	CREATEINDEX = 24,
	DROPINDEX = 25,
	ALTERINDEX_REBUILD = 26,
	ALTERVIEW_PROPERTIES = 27,
	CREATEUSER = 28,
	DROPUSER = 29,
	CHANGE_PWD = 30,
	AUTHENTICATION = 31,
	SHOW_USERNAMES = 32,
	CREATEROLE = 33,
	DROPROLE = 34,
	GRANT_PRIVILEGE = 35,
	REVOKE_PRIVILEGE = 36,
	SHOW_GRANT = 37,
	GRANT_ROLE = 38,
	REVOKE_ROLE = 39,
	SHOW_ROLE_GRANT = 40,
	CREATETABLE = 41,
	QUERY = 42,
	ALTERINDEX_PROPS = 43,
	ALTERDATABASE = 44,
	DESCDATABASE = 45,
	ALTERTABLE_DROP_PROPERTIES = 46, // by tianlong
}

enum CreateOperation {
	CREATE_NEW = 1,
    CREATE_IF_NOT_EXIST_AND_GET_IF_EXIST = 2,
    CREATE_NEW_IN_NODEGROUPS = 3,
    
    CREATE_AUX_IDX_FILE = 4,
    CREATE_NEW_RANDOM =5,
}

struct statfs {
  1: i64 from,
  2: i64 to,
  3: i64 increate,
  4: i64 close,
  5: i64 replicated,
  6: i64 rm_logical,
  7: i64 rm_physical,
  8: i64 underrep,
  9: i64 overrep,
  10: i64 linger,
  11: i64 suspect,

  12: i64 inc_ons,		// INCREATE, but contains online (1)
  13: i64 inc_ons2,     // INCREATE, but contains online (>1)
  14: i64 cls_offs,		// CLOSE, but all offline
  
  15: list<i64> incs,
  16: list<i64> clos,
  17: map<string, i64> fnrs;
  
  18: i64 recordnr,
  19: i64 length,
}

struct Role {
  1: string roleName,
  2: i32 createTime,
  3: string ownerName,
}

//struct User, added by liulichao for authentication
struct User {  
  1: string userName,
  2: string password,
  3: i64 createTime,
  4: string ownerName
}



struct Node {
  1: string node_name,
  2: list<string> ips,
  3: i32    status,
}


struct NodeGroup{
  1: string node_group_name,
  2: string comment,
  3: i32 status,
  4: set<Node> nodes,
}

// namespace for tables
struct Database {
  1: string name,
  2: string description,
  3: string locationUri,
  4: map<string, string> parameters, // properties associated with the database
  5: optional PrincipalPrivilegeSet privileges,
}

// This object holds the information needed by SerDes
struct SerDeInfo {
  1: string name,                   // name of the serde, table name by default
  2: string serializationLib,       // usually the class that implements the extractor & loader
  3: map<string, string> parameters // initialization parameters
}

// sort order of a column (column name along with asc(1)/desc(0))
struct Order {
  1: string col,  // sort column name
  2: i32    order // asc(1) or desc(0)
}

// this object holds all the information about skewed table
struct SkewedInfo {
  1: list<string> skewedColNames, // skewed column names
  2: list<list<string>> skewedColValues, //skewed values
  3: map<list<string>, string> skewedColValueLocationMaps, //skewed value to location mappings
}

// this object holds all the information about physical storage of the data belonging to a table
struct StorageDescriptor {
  1: list<FieldSchema> cols,  // required (refer to types defined above)
  2: string location,         // defaults to <warehouse loc>/<db loc>/tablename
  3: string inputFormat,      // SequenceFileInputFormat (binary) or TextInputFormat`  or custom format
  4: string outputFormat,     // SequenceFileOutputFormat (binary) or IgnoreKeyTextOutputFormat or custom format
  5: bool   compressed,       // compressed or not
  6: i32    numBuckets,       // this must be specified if there are any dimension columns
  7: SerDeInfo    serdeInfo,  // serialization and deserialization information
  8: list<string> bucketCols, // reducer grouping columns and clustering columns and bucketing columns`
  9: list<Order>  sortCols,   // sort order of the data in each bucket
  10: map<string, string> parameters, // any user supplied key value hash
  11: optional SkewedInfo skewedInfo, // skewed information
  12: optional bool   storedAsSubDirectories       // stored as subdirectories or not
}

  
struct Subpartition {
  1: list<string> values // string value is converted to appropriate partition key type
  2: string       dbName,
  3: string       tableName,
  4: i32          createTime,
  5: i32          lastAccessTime,
  6: StorageDescriptor   sd,
  7: map<string, string> parameters,
  8: list<i64>    files,
  9: optional string       partitionName,
  10: optional i32 version,
  11: optional PrincipalPrivilegeSet privileges
}

struct Partition {
  1: list<string> values // string value is converted to appropriate partition key type
  2: string       dbName,
  3: string       tableName,
  4: i32          createTime,
  5: i32          lastAccessTime,
  6: StorageDescriptor   sd,
  7: map<string, string> parameters,
  8: list<i64>    files,
  9: optional string       partitionName,
  10: optional list<Subpartition> subpartitions,
  11: optional i32 version,
  12: optional PrincipalPrivilegeSet privileges
}

// Schema information
struct GlobalSchema {
  1: string schemaName,                // name of the table
  2: string owner,                    // owner of this table
  3: i32    createTime,               // creation time of the table
  4: i32    lastAccessTime,           // last access time (usually this will be filled from HDFS and shouldn't be relied on)
  5: i32    retention,                // retention time
  6: StorageDescriptor sd,            // storage descriptor of the table
  7: map<string, string> parameters,   // to store comments or any other user level parameters
  8: string viewOriginalText,         // original view text, null for non-view
  9: string viewExpandedText,         // expanded view text, null for non-view
  10: string schemaType,                 // table type enum, e.g. EXTERNAL_TABLE
  11: optional PrincipalPrivilegeSet privileges
}

// table information
struct Table {
  1: string tableName,                // name of the table
  2: string dbName,                   // database name ('default')
  3: string schemaName,				  // schemaName name 
  4: string owner,                    // owner of this table
  5: i32    createTime,               // creation time of the table
  6: i32    lastAccessTime,           // last access time (usually this will be filled from HDFS and shouldn't be relied on)
  7: i32    retention,                // retention time
  8: StorageDescriptor sd,            // storage descriptor of the table
  9: list<FieldSchema> partitionKeys, // partition keys of the table. only primitive types are supported
  10: map<string, string> parameters,   // to store comments or any other user level parameters
  11: string viewOriginalText,         // original view text, null for non-view
  12: string viewExpandedText,         // expanded view text, null for non-view
  13: string tableType,                 // table type enum, e.g. EXTERNAL_TABLE
  14: list<NodeGroup> nodeGroups,
  15: optional PrincipalPrivilegeSet privileges,
  16: optional list<Partition> partitions
  17: list<FieldSchema> fileSplitKeys,	// file split keys
}



struct BusiTypeColumn {
  1: string busiType,  // required @ip,@content,@tel,@time
  2: Table table,         // table
  3: string column      //column
}

struct BusiTypeDatacenter {
  1: string busiType,  // required @ip,@content,@tel,@time
  2: Database db,         // Database 
}

struct SplitValue {
  1: string splitKeyName,
  2: i32    level,
  3: string value,
  4: i64	verison,
}

struct CreatePolicy {
  1: CreateOperation operation,
  2: list<string> arguments,
}

struct Device {
  1: string devid,
  2: i32    prop,
  3: string node_name,
  4: i32	status,
  5: string ng_name,
}

struct SFileLocation {
  1: string node_name,
  2: i64    fid,
  3: string devid,
  4: string location,
  5: i32    rep_id,
  6: i64    update_time,
  7: i32    visit_status,
  8: string digest,
}

struct SFile {
  1: i64	fid,
  2: string dbName,
  3: string tableName,
  4: i32	store_status,
  5: i32	rep_nr,
  6: string digest,
  7: i64	record_nr,
  8: i64	all_record_nr,
  9: list<SFileLocation> locations,
  10: i64    length,
  11: list<i64> ref_files,
  12: list<SplitValue> values,
  13: i32 	load_status,
}

struct SFileRef {
  1: SFile  file,
  2: i64    origin_fid,
}

struct Busitype {
  1: string name, // name of the field
  2: string comment
}

struct Index {
  1: string       indexName, // unique with in the whole database namespace
  2: string       indexHandlerClass, // reserved
  3: string       dbName,
  4: string       origTableName,
  5: i32          createTime,
  6: i32          lastAccessTime,
  7: string       indexTableName,
  8: StorageDescriptor   sd,
  9: map<string, string> parameters,
  10: bool         deferredRebuild
}

// column statistics
struct BooleanColumnStatsData {
1: required i64 numTrues,
2: required i64 numFalses,
3: required i64 numNulls
}

struct DoubleColumnStatsData {
1: required double lowValue,
2: required double highValue,
3: required i64 numNulls,
4: required i64 numDVs
}

struct LongColumnStatsData {
1: required i64 lowValue,
2: required i64 highValue,
3: required i64 numNulls,
4: required i64 numDVs
}

struct StringColumnStatsData {
1: required i64 maxColLen,
2: required double avgColLen,
3: required i64 numNulls,
4: required i64 numDVs
}

struct BinaryColumnStatsData {
1: required i64 maxColLen,
2: required double avgColLen,
3: required i64 numNulls
}

union ColumnStatisticsData {
1: BooleanColumnStatsData booleanStats,
2: LongColumnStatsData longStats,
3: DoubleColumnStatsData doubleStats,
4: StringColumnStatsData stringStats,
5: BinaryColumnStatsData binaryStats
}

struct ColumnStatisticsObj {
1: required string colName,
2: required string colType,
3: required ColumnStatisticsData statsData
}

struct ColumnStatisticsDesc {
1: required bool isTblLevel,
2: required string dbName,
3: required string tableName,
4: optional string partName,
5: optional i64 lastAnalyzed
}

struct ColumnStatistics {
1: required ColumnStatisticsDesc statsDesc,
2: required list<ColumnStatisticsObj> statsObj;
}

// schema of the table/query results etc.
struct Schema {
 // column names, types, comments
 1: list<FieldSchema> fieldSchemas,  // delimiters etc
 2: map<string, string> properties
}

// Key-value store to be used with selected
// Metastore APIs (create, alter methods).
// The client can pass environment properties / configs that can be
// accessed in hooks.
struct EnvironmentContext {
  1: map<string, string> properties
}

//strat off with cry
struct GeoLocation {
1: required string geoLocName,
2: required string nation,
3: required string province,
4: required string city,
5: required string dist
}
struct EquipRoom {
1: required string eqRoomName,
2: required i32 status,
3: optional string comment,
4: optional GeoLocation geolocation
}

exception MetaException {
  1: string message
}

exception UnknownTableException {
  1: string message
}

exception UnknownDBException {
  1: string message
}

exception AlreadyExistsException {
  1: string message
}

exception InvalidPartitionException {
  1: string message
}

exception UnknownPartitionException {
  1: string message
}

exception InvalidObjectException {
  1: string message
}

exception NoSuchObjectException {
  1: string message
}

exception IndexAlreadyExistsException {
  1: string message
}

exception InvalidOperationException {
  1: string message
}

exception ConfigValSecurityException {
  1: string message
}

exception InvalidInputException {
  1: string message
}

exception FileOperationException {
  1: string			message,
  2: FOFailReason	reason,
}

/**
* This interface is live.
*/
service ThriftHiveMetastore extends fb303.FacebookService
{
//added by zjw
  void create_attribution(1:Database db) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  Database get_attribution(1:string name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  void drop_attribution(1:string name, 2:bool deleteData, 3:bool cascade) throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)
  void update_attribution(1:Database db) throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)
  
  list<Database> get_all_attributions() throws(1:MetaException o1)
  Database get_local_attribution() throws(1:MetaException o1)
  
  list<string> get_lucene_index_names(1:string db_name, 2:string tbl_name, 3:i16 max_indexes=-1)
                       throws(1:MetaException o2)
  list<BusiTypeColumn> get_all_busi_type_cols() throws(1:MetaException o1)
                       
  list<BusiTypeDatacenter> get_all_busi_type_datacenters() throws(1:MetaException o1) 
  
  void append_busi_type_datacenter(1:BusiTypeDatacenter busiTypeDatacenter) throws(1:InvalidObjectException o1, 2:MetaException o2)
                        
  //start of partition,RPC for now is enough,subparttion operation can be done within partition rpc
  //end of partiton
  
  bool add_datawarehouse_sql(1:i32 dwNum, 2:string sql) throws(1:InvalidObjectException o1, 2:MetaException o2)
  
  list<Busitype> showBusitypes()  throws(1:InvalidObjectException o1, 2:MetaException o2)
  
  i32 createBusitype(1:Busitype busitype)  throws(1:InvalidObjectException o1, 2:MetaException o2)
  
  //start of partition file
  i32 add_partition_files(1:Partition part, 2:list<SFile> files)
  i32 drop_partition_files(1:Partition part, 2:list<SFile> files)
  i32 add_subpartition_files(1:Subpartition subpart, 2:list<SFile> files)
  i32 drop_subpartition_files(1:Subpartition subpart, 2:list<SFile> files)
  
  //start of partition index
  bool add_partition_index(1:Index index, 2:Partition part) throws (1:MetaException o1, 2:AlreadyExistsException o2);
  bool drop_partition_index(1:Index index, 2:Partition part) throws (1:MetaException o1, 2:AlreadyExistsException o2);
  
  bool add_subpartition_index(1:Index index, 2:Subpartition part) throws (1:MetaException o1, 2:AlreadyExistsException o2);
  bool drop_subpartition_index(1:Index index, 2:Subpartition part) throws (1:MetaException o1, 2:AlreadyExistsException o2);
  
  bool add_subpartition(1:string dbname, 2:string tbl_name, 3:list<string> part_vals,4:Subpartition sub_part)
  list<Subpartition> get_subpartitions(1:string dbname, 2:string tbl_name, 3:Partition part)
 
  //start of partition index file
  bool add_partition_index_files(1:Index index, 2: Partition part,3:list<SFile> file, 4:list<i64> originfid) throws(1:MetaException o1)
  list<SFileRef> get_partition_index_files(1:Index index, 2: Partition part) throws(1:MetaException o1)
  bool drop_partition_index_files(1:Index index, 2: Partition part,3:list<SFile> file) throws(1:MetaException o1)
  
  bool add_subpartition_index_files(1:Index index, 2: Subpartition subpart,3:list<SFile> file, 4:list<i64> originfid) throws(1:MetaException o1)
  list<SFileRef> get_subpartition_index_files(1:Index index, 2: Subpartition subpart) throws(1:MetaException o1)
  bool drop_subpartition_index_files(1:Index index, 2: Subpartition subpart,3:list<SFile> file) throws(1:MetaException o1)
  
//end of zjw

//strat off with cry

  bool addGeoLocation(1:GeoLocation gl) throws(1:MetaException o1)
  bool modifyGeoLocation(1:GeoLocation gl) throws(1:MetaException o1)
  bool deleteGeoLocation(1:GeoLocation gl) throws(1:MetaException o1)
  list<GeoLocation> listGeoLocation() throws(1:MetaException o1)
  
  bool addEquipRoom(1:EquipRoom er) throws(1:MetaException o1)
  bool modifyEquipRoom(1:EquipRoom er) throws(1:MetaException o1)
  bool deleteEquipRoom(1:EquipRoom er) throws(1:MetaException o1)
  list<EquipRoom> listEquipRoom() throws(1:MetaException o1)
  
  GeoLocation getGeoLocationByName(1:string geoLocName) throws (1:MetaException o1, 2:NoSuchObjectException o2)
  list<GeoLocation> getGeoLocationByNames(1:list<string> geoLocNames) throws (1:MetaException o1)
  
  bool addNodeAssignment(1:string nodeName, 2:string dbName) throws(1:MetaException o1, 2:NoSuchObjectException o2)
  bool deleteNodeAssignment(1:string nodeName, 2:string dbName) throws(1:MetaException o1, 2:NoSuchObjectException o2)
  list<Node> listNodes() throws(1:MetaException o1)
  
  bool addUserAssignment(1:string roleName, 2:string dbName) throws(1:MetaException o1, 2:NoSuchObjectException o2)
  bool deleteUserAssignment(1:string roleName, 2:string dbName) throws(1:MetaException o1, 2:NoSuchObjectException o2)
  list<User> listUsers() throws(1:MetaException o1)
  
  bool addRoleAssignment(1:string userName, 2:string dbName) throws(1:MetaException o1, 2:NoSuchObjectException o2)
  bool deleteRoleAssignment(1:string userName, 2:string dbName) throws(1:MetaException o1, 2:NoSuchObjectException o2)
  list<Role> listRoles() throws(1:MetaException o1)
  
  bool addNodeGroupAssignment (1:NodeGroup ng, 2:string dbName) throws(1:MetaException o1)
  bool deleteNodeGroupAssignment (1:NodeGroup ng, 2:string dbName) throws(1:MetaException o1)
  
  string pingPong(1:string str) throws (1:MetaException o1);
  
//end up with cry

  bool alterNodeGroup(1:NodeGroup ng) throws (1:AlreadyExistsException o1, 2:MetaException o2)

  void create_database(1:Database database) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  Database get_database(1:string name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  void drop_database(1:string name, 2:bool deleteData, 3:bool cascade) throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)
  list<string> get_databases(1:string pattern) throws(1:MetaException o1)
  list<string> get_all_databases() throws(1:MetaException o1)
  void alter_database(1:string dbname, 2:Database db) throws(1:MetaException o1, 2:NoSuchObjectException o2)
  
  // returns the type with given name (make seperate calls for the dependent types if needed)
  Type get_type(1:string name)  throws(1:MetaException o1, 2:NoSuchObjectException o2)
  bool create_type(1:Type type) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool drop_type(1:string type) throws(1:MetaException o1, 2:NoSuchObjectException o2)
  map<string, Type> get_type_all(1:string name)
                                throws(1:MetaException o2)

  // Gets a list of FieldSchemas describing the columns of a particular table
  list<FieldSchema> get_fields(1: string db_name, 2: string table_name) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3),

  // Gets a list of FieldSchemas describing both the columns and the partition keys of a particular table
  list<FieldSchema> get_schema(1: string db_name, 2: string table_name) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3)

  // create a Hive table. Following fields must be set
  // tableName
  // database        (only 'default' for now until Hive QL supports databases)
  // owner           (not needed, but good to have for tracking purposes)
  // sd.cols         (list of field schemas)
  // sd.inputFormat  (SequenceFileInputFormat (binary like falcon tables or u_full) or TextInputFormat)
  // sd.outputFormat (SequenceFileInputFormat (binary) or TextInputFormat)
  // sd.serdeInfo.serializationLib (SerDe class name eg org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe
  // * See notes on DDL_TIME
  void create_table(1:Table tbl) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:NoSuchObjectException o4)
  void create_table_by_user(1:Table tbl, 2:User user) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:NoSuchObjectException o4)
  void create_table_with_environment_context(1:Table tbl,
      2:EnvironmentContext environment_context)
      throws (1:AlreadyExistsException o1,
              2:InvalidObjectException o2, 3:MetaException o3,
              4:NoSuchObjectException o4)
  // drops the table and all the partitions associated with it if the table has partitions
  // delete data (including partitions) if deleteData is set to true
  void drop_table(1:string dbname, 2:string name, 3:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o3)
  list<string> get_tables(1: string db_name, 2: string pattern) throws (1: MetaException o1)
  list<string> get_all_tables(1: string db_name) throws (1: MetaException o1)

  Table get_table(1:string dbname, 2:string tbl_name)
                       throws (1:MetaException o1, 2:NoSuchObjectException o2)
  list<Table> get_table_objects_by_name(1:string dbname, 2:list<string> tbl_names)
				   throws (1:MetaException o1, 2:InvalidOperationException o2, 3:UnknownDBException o3)

  // Get a list of table names that match a filter.
  // The filter operators are LIKE, <, <=, >, >=, =, <>
  //
  // In the filter statement, values interpreted as strings must be enclosed in quotes,
  // while values interpreted as integers should not be.  Strings and integers are the only
  // supported value types.
  //
  // The currently supported key names in the filter are:
  // Constants.HIVE_FILTER_FIELD_OWNER, which filters on the tables' owner's name
  //   and supports all filter operators
  // Constants.HIVE_FILTER_FIELD_LAST_ACCESS, which filters on the last access times
  //   and supports all filter operators except LIKE
  // Constants.HIVE_FILTER_FIELD_PARAMS, which filters on the tables' parameter keys and values
  //   and only supports the filter operators = and <>.
  //   Append the parameter key name to HIVE_FILTER_FIELD_PARAMS in the filter statement.
  //   For example, to filter on parameter keys called "retention", the key name in the filter
  //   statement should be Constants.HIVE_FILTER_FIELD_PARAMS + "retention"
  //   Also, = and <> only work for keys that exist
  //   in the tables. E.g., if you are looking for tables where key1 <> value, it will only
  //   look at tables that have a value for the parameter key1.
  // Some example filter statements include:
  // filter = Constants.HIVE_FILTER_FIELD_OWNER + " like \".*test.*\" and " +
  //   Constants.HIVE_FILTER_FIELD_LAST_ACCESS + " = 0";
  // filter = Constants.HIVE_FILTER_FIELD_PARAMS + "retention = \"30\" or " +
  //   Constants.HIVE_FILTER_FIELD_PARAMS + "retention = \"90\""
  // @param dbName
  //          The name of the database from which you will retrieve the table names
  // @param filterType
  //          The type of filter
  // @param filter
  //          The filter string
  // @param max_tables
  //          The maximum number of tables returned
  // @return  A list of table names that match the desired filter
  list<string> get_table_names_by_filter(1:string dbname, 2:string filter, 3:i16 max_tables=-1)
                       throws (1:MetaException o1, 2:InvalidOperationException o2, 3:UnknownDBException o3)

  // alter table applies to only future partitions not for existing partitions
  // * See notes on DDL_TIME
  void alter_table(1:string dbname, 2:string tbl_name, 3:Table new_tbl)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)
  void alter_table_with_environment_context(1:string dbname, 2:string tbl_name,
      3:Table new_tbl, 4:EnvironmentContext environment_context)
      throws (1:InvalidOperationException o1, 2:MetaException o2) 
  // the following applies to only tables that have partitions
  // * See notes on DDL_TIME
  Partition add_partition(1:Partition new_part)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition add_partition_with_environment_context(1:Partition new_part,
      2:EnvironmentContext environment_context)
      throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2,
      3:MetaException o3)
  i32 add_partitions(1:list<Partition> new_parts)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals)
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  Partition append_partition_by_name(1:string db_name, 2:string tbl_name, 3:string part_name)
                       throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  bool drop_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool drop_partition_by_name(1:string db_name, 2:string tbl_name, 3:string part_name, 4:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2) 
  Partition get_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  Partition get_partition_with_auth(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 
      4: string user_name, 5: list<string> group_names) throws(1:MetaException o1, 2:NoSuchObjectException o2)

  Partition get_partition_by_name(1:string db_name 2:string tbl_name, 3:string part_name)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // returns all the partitions for this table in reverse chronological order.
  // If max parts is given then it will return only that many.
  list<Partition> get_partitions(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  list<Partition> get_partitions_with_auth(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1, 
     4: string user_name, 5: list<string> group_names) throws(1:NoSuchObjectException o1, 2:MetaException o2)                       

  list<string> get_partition_names(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1)
                       throws(1:MetaException o2)
                       
  // get_partition*_ps methods allow filtering by a partial partition specification, 
  // as needed for dynamic partitions. The values that are not restricted should 
  // be empty strings. Nulls were considered (instead of "") but caused errors in 
  // generated Python code. The size of part_vals may be smaller than the
  // number of partition columns - the unspecified values are considered the same
  // as "".
  list<Partition> get_partitions_ps(1:string db_name 2:string tbl_name 
  	3:list<string> part_vals, 4:i16 max_parts=-1)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)
  list<Partition> get_partitions_ps_with_auth(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:i16 max_parts=-1, 
     5: string user_name, 6: list<string> group_names) throws(1:NoSuchObjectException o1, 2:MetaException o2)                       
  
  list<string> get_partition_names_ps(1:string db_name, 
  	2:string tbl_name, 3:list<string> part_vals, 4:i16 max_parts=-1)
  	                   throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // get the partitions matching the given partition filter
  list<Partition> get_partitions_by_filter(1:string db_name 2:string tbl_name
    3:string filter, 4:i16 max_parts=-1)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // get partitions give a list of partition names
  list<Partition> get_partitions_by_names(1:string db_name 2:string tbl_name 3:list<string> names)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  // changes the partition to the new partition object. partition is identified from the part values
  // in the new_part
  // * See notes on DDL_TIME
  void alter_partition(1:string db_name, 2:string tbl_name, 3:Partition new_part)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)
                       
  // change a list of partitions. All partitions are altered atomically and all 
  // prehooks are fired together followed by all post hooks
  void alter_partitions(1:string db_name, 2:string tbl_name, 3:list<Partition> new_parts)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)

  void alter_partition_with_environment_context(1:string db_name,
      2:string tbl_name, 3:Partition new_part,
      4:EnvironmentContext environment_context)
      throws (1:InvalidOperationException o1, 2:MetaException o2)

  // rename the old partition to the new partition object by changing old part values to the part values
  // in the new_part. old partition is identified from part_vals.
  // partition keys in new_part should be the same as those in old partition.
  void rename_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:Partition new_part)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)

  // gets the value of the configuration key in the metastore server. returns
  // defaultValue if the key does not exist. if the configuration key does not
  // begin with "hive", "mapred", or "hdfs", a ConfigValSecurityException is
  // thrown.
  string get_config_value(1:string name, 2:string defaultValue)
                          throws(1:ConfigValSecurityException o1)
                          
  // converts a partition name into a partition values array
  list<string> partition_name_to_vals(1: string part_name)
                          throws(1: MetaException o1)
  // converts a partition name into a partition specification (a mapping from
  // the partition cols to the values)
  map<string, string> partition_name_to_spec(1: string part_name)
                          throws(1: MetaException o1)
  
  void markPartitionForEvent(1:string db_name, 2:string tbl_name, 3:map<string,string> part_vals,
                  4:PartitionEventType eventType) throws (1: MetaException o1, 2: NoSuchObjectException o2, 
                  3: UnknownDBException o3, 4: UnknownTableException o4, 5: UnknownPartitionException o5,
                  6: InvalidPartitionException o6) 
  bool isPartitionMarkedForEvent(1:string db_name, 2:string tbl_name, 3:map<string,string> part_vals, 
                  4: PartitionEventType eventType) throws (1: MetaException o1, 2:NoSuchObjectException o2,
                  3: UnknownDBException o3, 4: UnknownTableException o4, 5: UnknownPartitionException o5,
                  6: InvalidPartitionException o6) 
                         
  //index
  Index add_index(1:Index new_index, 2: Table index_table)
                       throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  void alter_index(1:string dbname, 2:string base_tbl_name, 3:string idx_name, 4:Index new_idx)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)
  bool drop_index_by_name(1:string db_name, 2:string tbl_name, 3:string index_name, 4:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2) 
  Index get_index_by_name(1:string db_name 2:string tbl_name, 3:string index_name)
                       throws(1:MetaException o1, 2:NoSuchObjectException o2)

  list<Index> get_indexes(1:string db_name, 2:string tbl_name, 3:i16 max_indexes=-1)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  list<string> get_index_names(1:string db_name, 2:string tbl_name, 3:i16 max_indexes=-1)
                       throws(1:MetaException o2)

  // column statistics interfaces

  // update APIs persist the column statistics object(s) that are passed in. If statistics already
  // exists for one or more columns, the existing statistics will be overwritten. The update APIs
  // validate that the dbName, tableName, partName, colName[] passed in as part of the ColumnStatistics
  // struct are valid, throws InvalidInputException/NoSuchObjectException if found to be invalid
  bool update_table_column_statistics(1:ColumnStatistics stats_obj) throws (1:NoSuchObjectException o1,
              2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)
  bool update_partition_column_statistics(1:ColumnStatistics stats_obj) throws (1:NoSuchObjectException o1,
              2:InvalidObjectException o2, 3:MetaException o3, 4:InvalidInputException o4)

  // get APIs return the column statistics corresponding to db_name, tbl_name, [part_name], col_name if
  // such statistics exists. If the required statistics doesn't exist, get APIs throw NoSuchObjectException
  // For instance, if get_table_column_statistics is called on a partitioned table for which only
  // partition level column stats exist, get_table_column_statistics will throw NoSuchObjectException
  ColumnStatistics get_table_column_statistics(1:string db_name, 2:string tbl_name, 3:string col_name) throws
              (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidInputException o3, 4:InvalidObjectException o4)
  ColumnStatistics get_partition_column_statistics(1:string db_name, 2:string tbl_name, 3:string part_name,
               4:string col_name) throws (1:NoSuchObjectException o1, 2:MetaException o2,
               3:InvalidInputException o3, 4:InvalidObjectException o4)

  // delete APIs attempt to delete column statistics, if found, associated with a given db_name, tbl_name, [part_name]
  // and col_name. If the delete API doesn't find the statistics record in the metastore, throws NoSuchObjectException
  // Delete API validates the input and if the input is invalid throws InvalidInputException/InvalidObjectException.
  bool delete_partition_column_statistics(1:string db_name, 2:string tbl_name, 3:string part_name, 4:string col_name) throws
              (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidObjectException o3,
               4:InvalidInputException o4)
  bool delete_table_column_statistics(1:string db_name, 2:string tbl_name, 3:string col_name) throws
              (1:NoSuchObjectException o1, 2:MetaException o2, 3:InvalidObjectException o3,
               4:InvalidInputException o4)

//authentication and authorization with user by liulichao, begin

  bool create_user(1:User user) throws(1:InvalidObjectException o1, 2:MetaException o2)
  bool drop_user(1:string user_name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool modify_user(1:User user) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  list<string> list_users_names() throws(1:MetaException o1)
  list<string> list_users(1:Database db) throws (1:MetaException o1)
  bool authentication(1:string user_name, 2:string passwd) throws(1:NoSuchObjectException o1, 2:MetaException o2) //authenticate the userName and passwd.
  bool user_authority_check(1:User user, 2:Table tbl, 3:list<MSOperation> ops) throws (1:MetaException o1)

//  list<string> get_user_grants(1:string user_name) throws(1:MetaException o1, 2:NoSuchObjectException o2) //if there is no user_name, then show all users' grants.

  //authentication and authorization with user by liulichao, end  

  //authorization privileges
                       
  bool create_role(1:Role role) throws(1:MetaException o1)
  bool drop_role(1:string role_name) throws(1:MetaException o1)
  list<string> get_role_names() throws(1:MetaException o1)
  bool grant_role(1:string role_name, 2:string principal_name, 3:PrincipalType principal_type, 
    4:string grantor, 5:PrincipalType grantorType, 6:bool grant_option) throws(1:MetaException o1)
  bool revoke_role(1:string role_name, 2:string principal_name, 3:PrincipalType principal_type) 
                        throws(1:MetaException o1)
  list<Role> list_roles(1:string principal_name, 2:PrincipalType principal_type) throws(1:MetaException o1)

  PrincipalPrivilegeSet get_privilege_set(1:HiveObjectRef hiveObject, 2:string user_name, 
    3: list<string> group_names) throws(1:MetaException o1)
  list<HiveObjectPrivilege> list_privileges(1:string principal_name, 2:PrincipalType principal_type, 
    3: HiveObjectRef hiveObject) throws(1:MetaException o1)
  
  bool grant_privileges(1:PrivilegeBag privileges) throws(1:MetaException o1)
  bool revoke_privileges(1:PrivilegeBag privileges) throws(1:MetaException o1)
  
  // this is used by metastore client to send UGI information to metastore server immediately
  // after setting up a connection. 
  list<string> set_ugi(1:string user_name, 2:list<string> group_names) throws (1:MetaException o1)

  //Authentication (delegation token) interfaces
  
  // get metastore server delegation token for use from the map/reduce tasks to authenticate
  // to metastore server
  string get_delegation_token(1:string token_owner, 2:string renewer_kerberos_principal_name)
    throws (1:MetaException o1)

  // method to renew delegation token obtained from metastore server
  i64 renew_delegation_token(1:string token_str_form) throws (1:MetaException o1)

  // method to cancel delegation token obtained from metastore server
  void cancel_delegation_token(1:string token_str_form) throws (1:MetaException o1)
  
  // method for file operations
  SFile create_file(1:string node_name, 2:i32 repnr, 3:string db_name, 4:string table_name, 5:list<SplitValue> values) throws (1:FileOperationException o1)
  
  SFile create_file_by_policy(1:CreatePolicy policy, 2:i32 repnr, 3:string db_name, 4:string table_name, 5:list<SplitValue> values) throws (1:FileOperationException o1)
  
  void set_file_repnr(1:i64 fid, 2:i32 repnr) throws (1:FileOperationException o1)
  
  bool reopen_file(1:i64 fid) throws (1:FileOperationException o1, 2:MetaException o2)
  
  i32 close_file(1:SFile file) throws (1:FileOperationException o1, 2:MetaException o2)
  
  bool online_filelocation(1:SFile file) throws (1:MetaException o1)
  
  bool offline_filelocation(1:SFileLocation sfl) throws (1:MetaException o1)
  
  bool del_filelocation(1:SFileLocation slf) throws (1:MetaException o1)
  
  bool set_loadstatus_bad(1:i64 fid) throws (1:MetaException o1)
  
  bool toggle_safemode() throws (1:MetaException o1)
  
  bool update_ms_service(1:i32 status) throws (1:MetaException o1)
  
  string get_ms_uris() throws (1:MetaException o1)
  
  SFile get_file_by_id(1:i64 fid) throws (1:FileOperationException o1, 2:MetaException o2)
  
  list<SFile> get_files_by_ids(list<i64> fids) throws (1:FileOperationException o1, 2:MetaException o2)
  
  SFile get_file_by_name(1:string node, 2:string devid, 3:string location) throws (1:FileOperationException o1, 2:MetaException o2)
  
  i32 rm_file_logical(1:SFile file) throws (1:FileOperationException o1, 2:MetaException o2)
  
  i32 restore_file(1:SFile file) throws (1:FileOperationException o1, 2:MetaException o2)
  
  i32 rm_file_physical(1:SFile file) throws (1:FileOperationException o1, 2:MetaException o2)
  
  // method for node operations
  Node get_node(1:string node_name) throws (1:MetaException o1)
  
  Node add_node(1:string node_name, 2:list<string> ipl) throws (1:MetaException o1)
  
  i32 del_node(1:string node_name) throws (1:MetaException o1)
  
  Device create_device(1:string devid, 2:i32 prop, 3:string node_name) throws (1:MetaException o1)
  
  Device get_device(1:string devid) throws (1:MetaException o1, 2:NoSuchObjectException o2)
  
  bool del_device(1:string devid) throws (1:MetaException o1)
  
  Device modify_device(1:Device dev, 2:Node node) throws (1:MetaException o1)
  
  list<Device> list_device() throws (1:MetaException o1)
  
  Node alter_node(1:string node_name, 2:list<string> ipl, 3:i32 status) throws (1:MetaException o1)
  
  list<Node> find_best_nodes(1:i32 nr) throws (1:MetaException o1)
  
  list<Node> find_best_nodes_in_groups(1:string dbName, 2:string tableName, 3:i32 nr, 4:FindNodePolicy policy) throws (1:MetaException o1)

  list<Node> get_all_nodes() throws(1:MetaException o1)
  
  string getDMStatus() throws(1:MetaException o1)
  
  string getNodeInfo() throws(1:MetaException o1)
  
  string getSysInfo() throws(1:MetaException o1)
  
  bool migrate_in(1:Table tbl, 2:map<i64, SFile> files, 3:list<Index> idxs, 4:string from_db, 5:string to_devid, 6:map<i64, SFileLocation> fileMap) throws (1:MetaException o1)
  
  list<SFileLocation> migrate_stage1(1:string dbName, 2:string tableName, 3:list<i64> files, 4:string to_db) throws (1:MetaException o1)
  
  bool migrate_stage2(1:string dbName, 2:string tableName, 3:list<i64> files, 4:string from_db, 5:string to_db, 6:string to_devid, 7:string user, 8:string password) throws (1:MetaException o1)
  
  bool migrate2_in(1:Table tbl, 2:list<Partition> parts, 3:list<Index> idxs, 4:string from_db, 5:string to_nas_devid, 6:map<i64, SFileLocation> fileMap) throws (1:MetaException o1)
  
  list<SFileLocation> migrate2_stage1(1:string dbName, 2:string tableName, 3:list<string> partNames, 4:string to_db) throws (1:MetaException o1)
  
  bool migrate2_stage2(1:string dbName, 2:string tableName, 3:list<string> partNames, 4:string from_db, 5:string to_db, 6:string to_nas_devid) throws (1:MetaException o1)
  
  i32 replicate(1:i64 fid, 2:i32 dtype) throws (1:MetaException o1, 2:FileOperationException o2)
  
  bool update_sfile_nrs(1:i64 fid, i64 rec_nr, i64 all_rec_nr, i64 length) throws (1:MetaException o1, 2:FileOperationException o2)
  
  string getMP(1:string node_name, 2:string devid) throws (1:MetaException o1)
  
  i64 getSessionId() throws (1:MetaException o1) 
  
  bool createSchema(1:GlobalSchema schema) throws (1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool modifySchema(1:string schemaName,2:GlobalSchema schema) throws (1:MetaException o1)
  bool deleteSchema(1:string schemaName) throws (1:MetaException o1)
  list<GlobalSchema> listSchemas() throws (1:MetaException o1)
  GlobalSchema getSchemaByName(1:string schemaName) throws (1:NoSuchObjectException o1,2:MetaException o2)
  
  list<NodeGroup> getTableNodeGroups(1:string dbName,2:string tabName) throws (1:MetaException o1)
  list<SFile> getTableNodeFiles(1:string dbName,2:string tabName,3:string nodeName)  throws (1:MetaException o1)
  
  list<i64> listTableFiles(1:string dbName,2:string tabName,3:i32 from, 4:i32 to)  throws (1:MetaException o1)
  list<i64> listFilesByDigest(1:string digest) throws (1:MetaException o1)
  list<string> listDevsByNode(1:string nodeName) throws (1:MetaException o1)
  list<i64> listFilesByDevs(1:list<string> devids) throws (1:MetaException o1)
  list<SFile> filterTableFiles(1:string dbName,2:string tabName,3:list<SplitValue> values)  throws (1:MetaException o1)
  void truncTableFiles(1:string dbName, 2:string tabName) throws (1:MetaException o1)
  
  bool addNodeGroup(1:NodeGroup ng) throws (1:AlreadyExistsException o1,2:MetaException o2)
  bool modifyNodeGroup (1:string schemaName,2:NodeGroup ng) throws (1:MetaException o1)
  bool deleteNodeGroup (1:NodeGroup ng) throws (1:MetaException o1)
  list<NodeGroup> listNodeGroups() throws (1:MetaException o1)
  list<NodeGroup> listDBNodeGroups(1:string dbName) throws (1:MetaException o1)
  list<NodeGroup> listNodeGroupByNames(1:list<string> ngNames) throws (1:MetaException o1 )
  
  bool addTableNodeDist(1:string db,2:string tab,3:list<string> ng)  throws (1:MetaException o1)
  bool deleteTableNodeDist(1:string db,2:string tab,3:list<string> ng) throws (1:MetaException o1)
  list<NodeGroup> listTableNodeDists (1:string dbName,2:string tabName) throws (1:MetaException o1)
  
  bool assiginSchematoDB(1:string dbName, 2:string schemaName,3:list<FieldSchema> fileSplitKeys,
      4:list<FieldSchema> part_keys,5:list<NodeGroup> ngs) throws (1:InvalidObjectException o1, 2:NoSuchObjectException o2, 3:MetaException o3)
  
  statfs statFileSystem(1:i64 begin_time, 2:i64 end_time) throws (1:MetaException o1)
  
  i64 getMaxFid() throws (1:MetaException o1)
  
  bool offlineDevicePhysically(1:string devid) throws (1:MetaException o1)
  
  bool flSelectorWatch(1:string table, 2:i32 op) throws (1:MetaException o1)
}

// * Note about the DDL_TIME: When creating or altering a table or a partition,
// if the DDL_TIME is not set, the current time will be used.

// For storing info about archived partitions in parameters

// Whether the partition is archived
const string IS_ARCHIVED = "is_archived",
// The original location of the partition, before archiving. After archiving,
// this directory will contain the archive. When the partition
// is dropped, this directory will be deleted
const string ORIGINAL_LOCATION = "original_location",

// these should be needed only for backward compatibility with filestore
const string META_TABLE_COLUMNS   = "columns",
const string META_TABLE_COLUMN_TYPES   = "columns.types",
const string BUCKET_FIELD_NAME    = "bucket_field_name",
const string BUCKET_COUNT         = "bucket_count",
const string FIELD_TO_DIMENSION   = "field_to_dimension",
const string META_TABLE_NAME      = "name",
const string META_TABLE_DB        = "db",
const string META_TABLE_LOCATION  = "location",
const string META_TABLE_SERDE     = "serde",
const string META_TABLE_PARTITION_COLUMNS = "partition_columns",
const string FILE_INPUT_FORMAT    = "file.inputformat",
const string FILE_OUTPUT_FORMAT   = "file.outputformat",
const string META_TABLE_STORAGE   = "storage_handler",

//added by zjw
const string META_LUCENE_INDEX   = "lucene.stored",
const string META_LUCENE_ANALYZE   = "lucene.analyzed",
const string META_LUCENE_STORE   = "lucene.indexd",



