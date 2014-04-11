// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "ThriftHiveMetastore.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace  ::Apache::Hadoop::Hive;

class ThriftHiveMetastoreHandler : virtual public ThriftHiveMetastoreIf {
 public:
  ThriftHiveMetastoreHandler() {
    // Your initialization goes here
  }

  void create_attribution(const Database& db) {
    // Your implementation goes here
    printf("create_attribution\n");
  }

  void get_attribution(Database& _return, const std::string& name) {
    // Your implementation goes here
    printf("get_attribution\n");
  }

  void drop_attribution(const std::string& name, const bool deleteData, const bool cascade) {
    // Your implementation goes here
    printf("drop_attribution\n");
  }

  void update_attribution(const Database& db) {
    // Your implementation goes here
    printf("update_attribution\n");
  }

  void get_all_attributions(std::vector<Database> & _return) {
    // Your implementation goes here
    printf("get_all_attributions\n");
  }

  void get_local_attribution(Database& _return) {
    // Your implementation goes here
    printf("get_local_attribution\n");
  }

  void get_lucene_index_names(std::vector<std::string> & _return, const std::string& db_name, const std::string& tbl_name, const int16_t max_indexes) {
    // Your implementation goes here
    printf("get_lucene_index_names\n");
  }

  void get_all_busi_type_cols(std::vector<BusiTypeColumn> & _return) {
    // Your implementation goes here
    printf("get_all_busi_type_cols\n");
  }

  void get_all_busi_type_datacenters(std::vector<BusiTypeDatacenter> & _return) {
    // Your implementation goes here
    printf("get_all_busi_type_datacenters\n");
  }

  void append_busi_type_datacenter(const BusiTypeDatacenter& busiTypeDatacenter) {
    // Your implementation goes here
    printf("append_busi_type_datacenter\n");
  }

  bool add_datawarehouse_sql(const int32_t dwNum, const std::string& sql) {
    // Your implementation goes here
    printf("add_datawarehouse_sql\n");
  }

  void showBusitypes(std::vector<Busitype> & _return) {
    // Your implementation goes here
    printf("showBusitypes\n");
  }

  int32_t createBusitype(const Busitype& busitype) {
    // Your implementation goes here
    printf("createBusitype\n");
  }

  void get_busi_type_schema_cols(std::vector<BusiTypeSchemaColumn> & _return) {
    // Your implementation goes here
    printf("get_busi_type_schema_cols\n");
  }

  void get_busi_type_schema_cols_by_name(std::vector<BusiTypeSchemaColumn> & _return, const std::string& schemaName) {
    // Your implementation goes here
    printf("get_busi_type_schema_cols_by_name\n");
  }

  int32_t add_partition_files(const Partition& part, const std::vector<SFile> & files) {
    // Your implementation goes here
    printf("add_partition_files\n");
  }

  int32_t drop_partition_files(const Partition& part, const std::vector<SFile> & files) {
    // Your implementation goes here
    printf("drop_partition_files\n");
  }

  int32_t add_subpartition_files(const Subpartition& subpart, const std::vector<SFile> & files) {
    // Your implementation goes here
    printf("add_subpartition_files\n");
  }

  int32_t drop_subpartition_files(const Subpartition& subpart, const std::vector<SFile> & files) {
    // Your implementation goes here
    printf("drop_subpartition_files\n");
  }

  bool add_partition_index(const Index& index, const Partition& part) {
    // Your implementation goes here
    printf("add_partition_index\n");
  }

  bool drop_partition_index(const Index& index, const Partition& part) {
    // Your implementation goes here
    printf("drop_partition_index\n");
  }

  bool add_subpartition_index(const Index& index, const Subpartition& part) {
    // Your implementation goes here
    printf("add_subpartition_index\n");
  }

  bool drop_subpartition_index(const Index& index, const Subpartition& part) {
    // Your implementation goes here
    printf("drop_subpartition_index\n");
  }

  bool add_subpartition(const std::string& dbname, const std::string& tbl_name, const std::vector<std::string> & part_vals, const Subpartition& sub_part) {
    // Your implementation goes here
    printf("add_subpartition\n");
  }

  void get_subpartitions(std::vector<Subpartition> & _return, const std::string& dbname, const std::string& tbl_name, const Partition& part) {
    // Your implementation goes here
    printf("get_subpartitions\n");
  }

  bool add_partition_index_files(const Index& index, const Partition& part, const std::vector<SFile> & file, const std::vector<int64_t> & originfid) {
    // Your implementation goes here
    printf("add_partition_index_files\n");
  }

  void get_partition_index_files(std::vector<SFileRef> & _return, const Index& index, const Partition& part) {
    // Your implementation goes here
    printf("get_partition_index_files\n");
  }

  bool drop_partition_index_files(const Index& index, const Partition& part, const std::vector<SFile> & file) {
    // Your implementation goes here
    printf("drop_partition_index_files\n");
  }

  bool add_subpartition_index_files(const Index& index, const Subpartition& subpart, const std::vector<SFile> & file, const std::vector<int64_t> & originfid) {
    // Your implementation goes here
    printf("add_subpartition_index_files\n");
  }

  void get_subpartition_index_files(std::vector<SFileRef> & _return, const Index& index, const Subpartition& subpart) {
    // Your implementation goes here
    printf("get_subpartition_index_files\n");
  }

  bool drop_subpartition_index_files(const Index& index, const Subpartition& subpart, const std::vector<SFile> & file) {
    // Your implementation goes here
    printf("drop_subpartition_index_files\n");
  }

  bool addGeoLocation(const GeoLocation& gl) {
    // Your implementation goes here
    printf("addGeoLocation\n");
  }

  bool modifyGeoLocation(const GeoLocation& gl) {
    // Your implementation goes here
    printf("modifyGeoLocation\n");
  }

  bool deleteGeoLocation(const GeoLocation& gl) {
    // Your implementation goes here
    printf("deleteGeoLocation\n");
  }

  void listGeoLocation(std::vector<GeoLocation> & _return) {
    // Your implementation goes here
    printf("listGeoLocation\n");
  }

  bool addEquipRoom(const EquipRoom& er) {
    // Your implementation goes here
    printf("addEquipRoom\n");
  }

  bool modifyEquipRoom(const EquipRoom& er) {
    // Your implementation goes here
    printf("modifyEquipRoom\n");
  }

  bool deleteEquipRoom(const EquipRoom& er) {
    // Your implementation goes here
    printf("deleteEquipRoom\n");
  }

  void listEquipRoom(std::vector<EquipRoom> & _return) {
    // Your implementation goes here
    printf("listEquipRoom\n");
  }

  void getGeoLocationByName(GeoLocation& _return, const std::string& geoLocName) {
    // Your implementation goes here
    printf("getGeoLocationByName\n");
  }

  void getGeoLocationByNames(std::vector<GeoLocation> & _return, const std::vector<std::string> & geoLocNames) {
    // Your implementation goes here
    printf("getGeoLocationByNames\n");
  }

  bool addNodeAssignment(const std::string& nodeName, const std::string& dbName) {
    // Your implementation goes here
    printf("addNodeAssignment\n");
  }

  bool deleteNodeAssignment(const std::string& nodeName, const std::string& dbName) {
    // Your implementation goes here
    printf("deleteNodeAssignment\n");
  }

  void listNodes(std::vector<Node> & _return) {
    // Your implementation goes here
    printf("listNodes\n");
  }

  bool addUserAssignment(const std::string& roleName, const std::string& dbName) {
    // Your implementation goes here
    printf("addUserAssignment\n");
  }

  bool deleteUserAssignment(const std::string& roleName, const std::string& dbName) {
    // Your implementation goes here
    printf("deleteUserAssignment\n");
  }

  void listUsers(std::vector<User> & _return) {
    // Your implementation goes here
    printf("listUsers\n");
  }

  bool addRoleAssignment(const std::string& userName, const std::string& dbName) {
    // Your implementation goes here
    printf("addRoleAssignment\n");
  }

  bool deleteRoleAssignment(const std::string& userName, const std::string& dbName) {
    // Your implementation goes here
    printf("deleteRoleAssignment\n");
  }

  void listRoles(std::vector<Role> & _return) {
    // Your implementation goes here
    printf("listRoles\n");
  }

  bool addNodeGroupAssignment(const NodeGroup& ng, const std::string& dbName) {
    // Your implementation goes here
    printf("addNodeGroupAssignment\n");
  }

  bool deleteNodeGroupAssignment(const NodeGroup& ng, const std::string& dbName) {
    // Your implementation goes here
    printf("deleteNodeGroupAssignment\n");
  }

  void pingPong(std::string& _return, const std::string& str) {
    // Your implementation goes here
    printf("pingPong\n");
  }

  bool alterNodeGroup(const NodeGroup& ng) {
    // Your implementation goes here
    printf("alterNodeGroup\n");
  }

  void create_database(const Database& database) {
    // Your implementation goes here
    printf("create_database\n");
  }

  void get_database(Database& _return, const std::string& name) {
    // Your implementation goes here
    printf("get_database\n");
  }

  void drop_database(const std::string& name, const bool deleteData, const bool cascade) {
    // Your implementation goes here
    printf("drop_database\n");
  }

  void get_databases(std::vector<std::string> & _return, const std::string& pattern) {
    // Your implementation goes here
    printf("get_databases\n");
  }

  void get_all_databases(std::vector<std::string> & _return) {
    // Your implementation goes here
    printf("get_all_databases\n");
  }

  void alter_database(const std::string& dbname, const Database& db) {
    // Your implementation goes here
    printf("alter_database\n");
  }

  void get_type(Type& _return, const std::string& name) {
    // Your implementation goes here
    printf("get_type\n");
  }

  bool create_type(const Type& type) {
    // Your implementation goes here
    printf("create_type\n");
  }

  bool drop_type(const std::string& type) {
    // Your implementation goes here
    printf("drop_type\n");
  }

  void get_type_all(std::map<std::string, Type> & _return, const std::string& name) {
    // Your implementation goes here
    printf("get_type_all\n");
  }

  void get_fields(std::vector<FieldSchema> & _return, const std::string& db_name, const std::string& table_name) {
    // Your implementation goes here
    printf("get_fields\n");
  }

  void get_schema(std::vector<FieldSchema> & _return, const std::string& db_name, const std::string& table_name) {
    // Your implementation goes here
    printf("get_schema\n");
  }

  void create_table(const Table& tbl) {
    // Your implementation goes here
    printf("create_table\n");
  }

  void create_table_by_user(const Table& tbl, const User& user) {
    // Your implementation goes here
    printf("create_table_by_user\n");
  }

  void create_table_with_environment_context(const Table& tbl, const EnvironmentContext& environment_context) {
    // Your implementation goes here
    printf("create_table_with_environment_context\n");
  }

  void drop_table(const std::string& dbname, const std::string& name, const bool deleteData) {
    // Your implementation goes here
    printf("drop_table\n");
  }

  void get_tables(std::vector<std::string> & _return, const std::string& db_name, const std::string& pattern) {
    // Your implementation goes here
    printf("get_tables\n");
  }

  void get_all_tables(std::vector<std::string> & _return, const std::string& db_name) {
    // Your implementation goes here
    printf("get_all_tables\n");
  }

  void get_table(Table& _return, const std::string& dbname, const std::string& tbl_name) {
    // Your implementation goes here
    printf("get_table\n");
  }

  void get_table_objects_by_name(std::vector<Table> & _return, const std::string& dbname, const std::vector<std::string> & tbl_names) {
    // Your implementation goes here
    printf("get_table_objects_by_name\n");
  }

  void get_table_names_by_filter(std::vector<std::string> & _return, const std::string& dbname, const std::string& filter, const int16_t max_tables) {
    // Your implementation goes here
    printf("get_table_names_by_filter\n");
  }

  void alter_table(const std::string& dbname, const std::string& tbl_name, const Table& new_tbl) {
    // Your implementation goes here
    printf("alter_table\n");
  }

  void alter_table_with_environment_context(const std::string& dbname, const std::string& tbl_name, const Table& new_tbl, const EnvironmentContext& environment_context) {
    // Your implementation goes here
    printf("alter_table_with_environment_context\n");
  }

  void add_partition(Partition& _return, const Partition& new_part) {
    // Your implementation goes here
    printf("add_partition\n");
  }

  void add_partition_with_environment_context(Partition& _return, const Partition& new_part, const EnvironmentContext& environment_context) {
    // Your implementation goes here
    printf("add_partition_with_environment_context\n");
  }

  int32_t add_partitions(const std::vector<Partition> & new_parts) {
    // Your implementation goes here
    printf("add_partitions\n");
  }

  void append_partition(Partition& _return, const std::string& db_name, const std::string& tbl_name, const std::vector<std::string> & part_vals) {
    // Your implementation goes here
    printf("append_partition\n");
  }

  void append_partition_by_name(Partition& _return, const std::string& db_name, const std::string& tbl_name, const std::string& part_name) {
    // Your implementation goes here
    printf("append_partition_by_name\n");
  }

  bool drop_partition(const std::string& db_name, const std::string& tbl_name, const std::vector<std::string> & part_vals, const bool deleteData) {
    // Your implementation goes here
    printf("drop_partition\n");
  }

  bool drop_partition_by_name(const std::string& db_name, const std::string& tbl_name, const std::string& part_name, const bool deleteData) {
    // Your implementation goes here
    printf("drop_partition_by_name\n");
  }

  void get_partition(Partition& _return, const std::string& db_name, const std::string& tbl_name, const std::vector<std::string> & part_vals) {
    // Your implementation goes here
    printf("get_partition\n");
  }

  void get_partition_with_auth(Partition& _return, const std::string& db_name, const std::string& tbl_name, const std::vector<std::string> & part_vals, const std::string& user_name, const std::vector<std::string> & group_names) {
    // Your implementation goes here
    printf("get_partition_with_auth\n");
  }

  void get_partition_by_name(Partition& _return, const std::string& db_name, const std::string& tbl_name, const std::string& part_name) {
    // Your implementation goes here
    printf("get_partition_by_name\n");
  }

  void get_partitions(std::vector<Partition> & _return, const std::string& db_name, const std::string& tbl_name, const int16_t max_parts) {
    // Your implementation goes here
    printf("get_partitions\n");
  }

  void get_partitions_with_auth(std::vector<Partition> & _return, const std::string& db_name, const std::string& tbl_name, const int16_t max_parts, const std::string& user_name, const std::vector<std::string> & group_names) {
    // Your implementation goes here
    printf("get_partitions_with_auth\n");
  }

  void get_partition_names(std::vector<std::string> & _return, const std::string& db_name, const std::string& tbl_name, const int16_t max_parts) {
    // Your implementation goes here
    printf("get_partition_names\n");
  }

  void get_partitions_ps(std::vector<Partition> & _return, const std::string& db_name, const std::string& tbl_name, const std::vector<std::string> & part_vals, const int16_t max_parts) {
    // Your implementation goes here
    printf("get_partitions_ps\n");
  }

  void get_partitions_ps_with_auth(std::vector<Partition> & _return, const std::string& db_name, const std::string& tbl_name, const std::vector<std::string> & part_vals, const int16_t max_parts, const std::string& user_name, const std::vector<std::string> & group_names) {
    // Your implementation goes here
    printf("get_partitions_ps_with_auth\n");
  }

  void get_partition_names_ps(std::vector<std::string> & _return, const std::string& db_name, const std::string& tbl_name, const std::vector<std::string> & part_vals, const int16_t max_parts) {
    // Your implementation goes here
    printf("get_partition_names_ps\n");
  }

  void get_partitions_by_filter(std::vector<Partition> & _return, const std::string& db_name, const std::string& tbl_name, const std::string& filter, const int16_t max_parts) {
    // Your implementation goes here
    printf("get_partitions_by_filter\n");
  }

  void get_partitions_by_names(std::vector<Partition> & _return, const std::string& db_name, const std::string& tbl_name, const std::vector<std::string> & names) {
    // Your implementation goes here
    printf("get_partitions_by_names\n");
  }

  void alter_partition(const std::string& db_name, const std::string& tbl_name, const Partition& new_part) {
    // Your implementation goes here
    printf("alter_partition\n");
  }

  void alter_partitions(const std::string& db_name, const std::string& tbl_name, const std::vector<Partition> & new_parts) {
    // Your implementation goes here
    printf("alter_partitions\n");
  }

  void alter_partition_with_environment_context(const std::string& db_name, const std::string& tbl_name, const Partition& new_part, const EnvironmentContext& environment_context) {
    // Your implementation goes here
    printf("alter_partition_with_environment_context\n");
  }

  void rename_partition(const std::string& db_name, const std::string& tbl_name, const std::vector<std::string> & part_vals, const Partition& new_part) {
    // Your implementation goes here
    printf("rename_partition\n");
  }

  void get_config_value(std::string& _return, const std::string& name, const std::string& defaultValue) {
    // Your implementation goes here
    printf("get_config_value\n");
  }

  void partition_name_to_vals(std::vector<std::string> & _return, const std::string& part_name) {
    // Your implementation goes here
    printf("partition_name_to_vals\n");
  }

  void partition_name_to_spec(std::map<std::string, std::string> & _return, const std::string& part_name) {
    // Your implementation goes here
    printf("partition_name_to_spec\n");
  }

  void markPartitionForEvent(const std::string& db_name, const std::string& tbl_name, const std::map<std::string, std::string> & part_vals, const PartitionEventType::type eventType) {
    // Your implementation goes here
    printf("markPartitionForEvent\n");
  }

  bool isPartitionMarkedForEvent(const std::string& db_name, const std::string& tbl_name, const std::map<std::string, std::string> & part_vals, const PartitionEventType::type eventType) {
    // Your implementation goes here
    printf("isPartitionMarkedForEvent\n");
  }

  void add_index(Index& _return, const Index& new_index, const Table& index_table) {
    // Your implementation goes here
    printf("add_index\n");
  }

  void alter_index(const std::string& dbname, const std::string& base_tbl_name, const std::string& idx_name, const Index& new_idx) {
    // Your implementation goes here
    printf("alter_index\n");
  }

  bool drop_index_by_name(const std::string& db_name, const std::string& tbl_name, const std::string& index_name, const bool deleteData) {
    // Your implementation goes here
    printf("drop_index_by_name\n");
  }

  void get_index_by_name(Index& _return, const std::string& db_name, const std::string& tbl_name, const std::string& index_name) {
    // Your implementation goes here
    printf("get_index_by_name\n");
  }

  void get_indexes(std::vector<Index> & _return, const std::string& db_name, const std::string& tbl_name, const int16_t max_indexes) {
    // Your implementation goes here
    printf("get_indexes\n");
  }

  void get_index_names(std::vector<std::string> & _return, const std::string& db_name, const std::string& tbl_name, const int16_t max_indexes) {
    // Your implementation goes here
    printf("get_index_names\n");
  }

  bool update_table_column_statistics(const ColumnStatistics& stats_obj) {
    // Your implementation goes here
    printf("update_table_column_statistics\n");
  }

  bool update_partition_column_statistics(const ColumnStatistics& stats_obj) {
    // Your implementation goes here
    printf("update_partition_column_statistics\n");
  }

  void get_table_column_statistics(ColumnStatistics& _return, const std::string& db_name, const std::string& tbl_name, const std::string& col_name) {
    // Your implementation goes here
    printf("get_table_column_statistics\n");
  }

  void get_partition_column_statistics(ColumnStatistics& _return, const std::string& db_name, const std::string& tbl_name, const std::string& part_name, const std::string& col_name) {
    // Your implementation goes here
    printf("get_partition_column_statistics\n");
  }

  bool delete_partition_column_statistics(const std::string& db_name, const std::string& tbl_name, const std::string& part_name, const std::string& col_name) {
    // Your implementation goes here
    printf("delete_partition_column_statistics\n");
  }

  bool delete_table_column_statistics(const std::string& db_name, const std::string& tbl_name, const std::string& col_name) {
    // Your implementation goes here
    printf("delete_table_column_statistics\n");
  }

  bool create_user(const User& user) {
    // Your implementation goes here
    printf("create_user\n");
  }

  bool drop_user(const std::string& user_name) {
    // Your implementation goes here
    printf("drop_user\n");
  }

  bool modify_user(const User& user) {
    // Your implementation goes here
    printf("modify_user\n");
  }

  void list_users_names(std::vector<std::string> & _return) {
    // Your implementation goes here
    printf("list_users_names\n");
  }

  void list_users(std::vector<std::string> & _return, const Database& db) {
    // Your implementation goes here
    printf("list_users\n");
  }

  bool authentication(const std::string& user_name, const std::string& passwd) {
    // Your implementation goes here
    printf("authentication\n");
  }

  bool user_authority_check(const User& user, const Table& tbl, const std::vector<MSOperation::type> & ops) {
    // Your implementation goes here
    printf("user_authority_check\n");
  }

  bool create_role(const Role& role) {
    // Your implementation goes here
    printf("create_role\n");
  }

  bool drop_role(const std::string& role_name) {
    // Your implementation goes here
    printf("drop_role\n");
  }

  void get_role_names(std::vector<std::string> & _return) {
    // Your implementation goes here
    printf("get_role_names\n");
  }

  bool grant_role(const std::string& role_name, const std::string& principal_name, const PrincipalType::type principal_type, const std::string& grantor, const PrincipalType::type grantorType, const bool grant_option) {
    // Your implementation goes here
    printf("grant_role\n");
  }

  bool revoke_role(const std::string& role_name, const std::string& principal_name, const PrincipalType::type principal_type) {
    // Your implementation goes here
    printf("revoke_role\n");
  }

  void list_roles(std::vector<Role> & _return, const std::string& principal_name, const PrincipalType::type principal_type) {
    // Your implementation goes here
    printf("list_roles\n");
  }

  void get_privilege_set(PrincipalPrivilegeSet& _return, const HiveObjectRef& hiveObject, const std::string& user_name, const std::vector<std::string> & group_names) {
    // Your implementation goes here
    printf("get_privilege_set\n");
  }

  void list_privileges(std::vector<HiveObjectPrivilege> & _return, const std::string& principal_name, const PrincipalType::type principal_type, const HiveObjectRef& hiveObject) {
    // Your implementation goes here
    printf("list_privileges\n");
  }

  bool grant_privileges(const PrivilegeBag& privileges) {
    // Your implementation goes here
    printf("grant_privileges\n");
  }

  bool revoke_privileges(const PrivilegeBag& privileges) {
    // Your implementation goes here
    printf("revoke_privileges\n");
  }

  void set_ugi(std::vector<std::string> & _return, const std::string& user_name, const std::vector<std::string> & group_names) {
    // Your implementation goes here
    printf("set_ugi\n");
  }

  void get_delegation_token(std::string& _return, const std::string& token_owner, const std::string& renewer_kerberos_principal_name) {
    // Your implementation goes here
    printf("get_delegation_token\n");
  }

  int64_t renew_delegation_token(const std::string& token_str_form) {
    // Your implementation goes here
    printf("renew_delegation_token\n");
  }

  void cancel_delegation_token(const std::string& token_str_form) {
    // Your implementation goes here
    printf("cancel_delegation_token\n");
  }

  void create_file(SFile& _return, const std::string& node_name, const int32_t repnr, const std::string& db_name, const std::string& table_name, const std::vector<SplitValue> & values) {
    // Your implementation goes here
    printf("create_file\n");
  }

  void create_file_by_policy(SFile& _return, const CreatePolicy& policy, const int32_t repnr, const std::string& db_name, const std::string& table_name, const std::vector<SplitValue> & values) {
    // Your implementation goes here
    printf("create_file_by_policy\n");
  }

  void set_file_repnr(const int64_t fid, const int32_t repnr) {
    // Your implementation goes here
    printf("set_file_repnr\n");
  }

  bool reopen_file(const int64_t fid) {
    // Your implementation goes here
    printf("reopen_file\n");
  }

  int32_t close_file(const SFile& file) {
    // Your implementation goes here
    printf("close_file\n");
  }

  bool online_filelocation(const SFile& file) {
    // Your implementation goes here
    printf("online_filelocation\n");
  }

  bool offline_filelocation(const SFileLocation& sfl) {
    // Your implementation goes here
    printf("offline_filelocation\n");
  }

  bool del_filelocation(const SFileLocation& slf) {
    // Your implementation goes here
    printf("del_filelocation\n");
  }

  bool set_loadstatus_bad(const int64_t fid) {
    // Your implementation goes here
    printf("set_loadstatus_bad\n");
  }

  bool toggle_safemode() {
    // Your implementation goes here
    printf("toggle_safemode\n");
  }

  void get_file_by_id(SFile& _return, const int64_t fid) {
    // Your implementation goes here
    printf("get_file_by_id\n");
  }

  void get_files_by_ids(std::vector<SFile> & _return, const std::vector<int64_t> & fids) {
    // Your implementation goes here
    printf("get_files_by_ids\n");
  }

  void get_file_by_name(SFile& _return, const std::string& node, const std::string& devid, const std::string& location) {
    // Your implementation goes here
    printf("get_file_by_name\n");
  }

  int32_t rm_file_logical(const SFile& file) {
    // Your implementation goes here
    printf("rm_file_logical\n");
  }

  int32_t restore_file(const SFile& file) {
    // Your implementation goes here
    printf("restore_file\n");
  }

  int32_t rm_file_physical(const SFile& file) {
    // Your implementation goes here
    printf("rm_file_physical\n");
  }

  void get_node(Node& _return, const std::string& node_name) {
    // Your implementation goes here
    printf("get_node\n");
  }

  void add_node(Node& _return, const std::string& node_name, const std::vector<std::string> & ipl) {
    // Your implementation goes here
    printf("add_node\n");
  }

  int32_t del_node(const std::string& node_name) {
    // Your implementation goes here
    printf("del_node\n");
  }

  void create_device(Device& _return, const std::string& devid, const int32_t prop, const std::string& node_name) {
    // Your implementation goes here
    printf("create_device\n");
  }

  void get_device(Device& _return, const std::string& devid) {
    // Your implementation goes here
    printf("get_device\n");
  }

  bool del_device(const std::string& devid) {
    // Your implementation goes here
    printf("del_device\n");
  }

  void modify_device(Device& _return, const Device& dev, const Node& node) {
    // Your implementation goes here
    printf("modify_device\n");
  }

  void list_device(std::vector<Device> & _return) {
    // Your implementation goes here
    printf("list_device\n");
  }

  void alter_node(Node& _return, const std::string& node_name, const std::vector<std::string> & ipl, const int32_t status) {
    // Your implementation goes here
    printf("alter_node\n");
  }

  void find_best_nodes(std::vector<Node> & _return, const int32_t nr) {
    // Your implementation goes here
    printf("find_best_nodes\n");
  }

  void find_best_nodes_in_groups(std::vector<Node> & _return, const std::string& dbName, const std::string& tableName, const int32_t nr, const FindNodePolicy::type policy) {
    // Your implementation goes here
    printf("find_best_nodes_in_groups\n");
  }

  void get_all_nodes(std::vector<Node> & _return) {
    // Your implementation goes here
    printf("get_all_nodes\n");
  }

  void getDMStatus(std::string& _return) {
    // Your implementation goes here
    printf("getDMStatus\n");
  }

  void getNodeInfo(std::string& _return) {
    // Your implementation goes here
    printf("getNodeInfo\n");
  }

  bool migrate_in(const Table& tbl, const std::map<int64_t, SFile> & files, const std::vector<Index> & idxs, const std::string& from_db, const std::string& to_devid, const std::map<int64_t, SFileLocation> & fileMap) {
    // Your implementation goes here
    printf("migrate_in\n");
  }

  void migrate_stage1(std::vector<SFileLocation> & _return, const std::string& dbName, const std::string& tableName, const std::vector<int64_t> & files, const std::string& to_db) {
    // Your implementation goes here
    printf("migrate_stage1\n");
  }

  bool migrate_stage2(const std::string& dbName, const std::string& tableName, const std::vector<int64_t> & files, const std::string& from_db, const std::string& to_db, const std::string& to_devid, const std::string& user, const std::string& password) {
    // Your implementation goes here
    printf("migrate_stage2\n");
  }

  bool migrate2_in(const Table& tbl, const std::vector<Partition> & parts, const std::vector<Index> & idxs, const std::string& from_db, const std::string& to_nas_devid, const std::map<int64_t, SFileLocation> & fileMap) {
    // Your implementation goes here
    printf("migrate2_in\n");
  }

  void migrate2_stage1(std::vector<SFileLocation> & _return, const std::string& dbName, const std::string& tableName, const std::vector<std::string> & partNames, const std::string& to_db) {
    // Your implementation goes here
    printf("migrate2_stage1\n");
  }

  bool migrate2_stage2(const std::string& dbName, const std::string& tableName, const std::vector<std::string> & partNames, const std::string& from_db, const std::string& to_db, const std::string& to_nas_devid) {
    // Your implementation goes here
    printf("migrate2_stage2\n");
  }

  void getMP(std::string& _return, const std::string& node_name, const std::string& devid) {
    // Your implementation goes here
    printf("getMP\n");
  }

  int64_t getSessionId() {
    // Your implementation goes here
    printf("getSessionId\n");
  }

  bool createSchema(const GlobalSchema& schema) {
    // Your implementation goes here
    printf("createSchema\n");
  }

  bool modifySchema(const std::string& schemaName, const GlobalSchema& schema) {
    // Your implementation goes here
    printf("modifySchema\n");
  }

  bool deleteSchema(const std::string& schemaName) {
    // Your implementation goes here
    printf("deleteSchema\n");
  }

  void listSchemas(std::vector<GlobalSchema> & _return) {
    // Your implementation goes here
    printf("listSchemas\n");
  }

  void getSchemaByName(GlobalSchema& _return, const std::string& schemaName) {
    // Your implementation goes here
    printf("getSchemaByName\n");
  }

  void getTableNodeGroups(std::vector<NodeGroup> & _return, const std::string& dbName, const std::string& tabName) {
    // Your implementation goes here
    printf("getTableNodeGroups\n");
  }

  void getTableNodeFiles(std::vector<SFile> & _return, const std::string& dbName, const std::string& tabName, const std::string& nodeName) {
    // Your implementation goes here
    printf("getTableNodeFiles\n");
  }

  void listTableFiles(std::vector<int64_t> & _return, const std::string& dbName, const std::string& tabName, const int32_t from, const int32_t to) {
    // Your implementation goes here
    printf("listTableFiles\n");
  }

  void listFilesByDigest(std::vector<int64_t> & _return, const std::string& digest) {
    // Your implementation goes here
    printf("listFilesByDigest\n");
  }

  void listDevsByNode(std::vector<std::string> & _return, const std::string& nodeName) {
    // Your implementation goes here
    printf("listDevsByNode\n");
  }

  void listFilesByDevs(std::vector<int64_t> & _return, const std::vector<std::string> & devids) {
    // Your implementation goes here
    printf("listFilesByDevs\n");
  }

  void filterTableFiles(std::vector<SFile> & _return, const std::string& dbName, const std::string& tabName, const std::vector<SplitValue> & values) {
    // Your implementation goes here
    printf("filterTableFiles\n");
  }

  void truncTableFiles(const std::string& dbName, const std::string& tabName) {
    // Your implementation goes here
    printf("truncTableFiles\n");
  }

  bool addNodeGroup(const NodeGroup& ng) {
    // Your implementation goes here
    printf("addNodeGroup\n");
  }

  bool modifyNodeGroup(const std::string& schemaName, const NodeGroup& ng) {
    // Your implementation goes here
    printf("modifyNodeGroup\n");
  }

  bool deleteNodeGroup(const NodeGroup& ng) {
    // Your implementation goes here
    printf("deleteNodeGroup\n");
  }

  void listNodeGroups(std::vector<NodeGroup> & _return) {
    // Your implementation goes here
    printf("listNodeGroups\n");
  }

  void listDBNodeGroups(std::vector<NodeGroup> & _return, const std::string& dbName) {
    // Your implementation goes here
    printf("listDBNodeGroups\n");
  }

  void listNodeGroupByNames(std::vector<NodeGroup> & _return, const std::vector<std::string> & ngNames) {
    // Your implementation goes here
    printf("listNodeGroupByNames\n");
  }

  bool addTableNodeDist(const std::string& db, const std::string& tab, const std::vector<std::string> & ng) {
    // Your implementation goes here
    printf("addTableNodeDist\n");
  }

  bool deleteTableNodeDist(const std::string& db, const std::string& tab, const std::vector<std::string> & ng) {
    // Your implementation goes here
    printf("deleteTableNodeDist\n");
  }

  void listTableNodeDists(std::vector<NodeGroup> & _return, const std::string& dbName, const std::string& tabName) {
    // Your implementation goes here
    printf("listTableNodeDists\n");
  }

  bool assiginSchematoDB(const std::string& dbName, const std::string& schemaName, const std::vector<FieldSchema> & fileSplitKeys, const std::vector<FieldSchema> & part_keys, const std::vector<NodeGroup> & ngs) {
    // Your implementation goes here
    printf("assiginSchematoDB\n");
  }

  void statFileSystem(statfs& _return, const int64_t begin_time, const int64_t end_time) {
    // Your implementation goes here
    printf("statFileSystem\n");
  }

  int64_t getMaxFid() {
    // Your implementation goes here
    printf("getMaxFid\n");
  }

  bool offlineDevicePhysically(const std::string& devid) {
    // Your implementation goes here
    printf("offlineDevicePhysically\n");
  }

  bool flSelectorWatch(const std::string& table, const int32_t op) {
    // Your implementation goes here
    printf("flSelectorWatch\n");
  }

};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<ThriftHiveMetastoreHandler> handler(new ThriftHiveMetastoreHandler());
  shared_ptr<TProcessor> processor(new ThriftHiveMetastoreProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

