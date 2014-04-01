package org.apache.hadoop.hive.metastore.newms;

import iie.metastore.MetaStoreClient;

import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

public class TestThriftMethod {

	/**
	 * @param args
	 * @throws MetaException 
	 */
	public static void main(String[] args) throws MetaException {

		MetaStoreClient msc = new MetaStoreClient("node13",8111);
		try {
		  
		  //newms测试
		  
		  //SCHEMA TEST getSchemaByName getSchema listSchemas
//			GlobalSchema gls = msc.client.getSchemaByName("sczyt1");
//			System.out.println("Schema 's name is " + gls.getSchemaName());
		  
//		  List<FieldSchema> sch = msc.client.getSchema("dbzy", "c_c");
//		  for(FieldSchema fs : sch){
//	      System.out.println(fs.getName() + "----" + fs.getType());
//		  }
		  
//		  List<GlobalSchema> gss = msc.client.listSchemas();
//		  for(GlobalSchema gs : gss){
//		    System.out.println(gs.getSchemaName() + "----" + gs.getSchemaType());
//		  }
		  
		  //DATABASE TEST getAllDatabases get_database
//		  List<String> dbs = msc.client.getAllDatabases();
//		  for(String str : dbs){
//		    System.out.println("DB 's name is :" + str);
//		  }
		  
//		  Database db = msc.client.getDatabase("db1");
//		  System.out.println(db.getName());
		  
		  //NODE TEST get_all_nodes
//		  List<Node> nodes = msc.client.get_all_nodes();
//		  for(Node n : nodes){
//		    System.out.println("NODE 's name is " + n.getNode_name());
//		  }
		  
		  //TABLE TEST getAllTables getTable getTableObjectsByName listTableNodeDists
//		  List<String> tbls = msc.client.getAllTables("dbzy");
//		  System.out.println(tbls.size());
//		  for(String str : tbls){
//		    System.out.println("TABLE 's name is " + str);
//		  }
		  
//		  Table tbl = msc.client.getTable("dbzy", "c_c");
//		  List<FieldSchema> fss = tbl.getSd().getCols();
//		  for(FieldSchema fs : fss){
//		    System.out.println(fs.getName() + "----" + fs.getType());
//		  }
		  
//		  List<String> tblNames = new ArrayList<String>();
//		  tblNames.add("c_c");
//		  List<Table> tbl = (List<Table>) msc.client.getTableObjectsByName("dbzy", tblNames);
//		  for(Table t : tbl){
//		    System.out.println(t.getDbName() + "." + t.getTableName());
//		  }
		  
		  //DEVICE TEST getDevice listDevice
//		  Device de = msc.client.getDevice("222e1000155b7615b");
//		  System.out.println(de.getDevid() + de.getNg_name());
		  
//		  List<Device> des = msc.client.listDevice();
//		  for(Device de : des){
//		    System.out.println(de.getDevid() + "----" + de.getNg_name() + "----" + de.getNode_name());
//		  }
		  
		  // FieldSchema TEST getFields（获取表的列）
//		  List<FieldSchema> fs = msc.client.getFields("dbzy", "c_c");
//		  for(FieldSchema fsa : fs){
//		    System.out.println(fsa.getName());
//		  }
		  
		  //INDEX TEST getIndex listIndexNames
//		  Index ind = msc.client.getIndex("dbzy", "c_c", "c_usernum");
//		  System.out.println(ind.getIndexName());
		  
//		  List<String> inds = msc.client.listIndexNames("dbzy", "c_c", (short) 1);
//		  for(String str : inds){
//	      System.out.println(str);
//		  }

//		  List<Index> inds = msc.client.listIndexes("dbzy", "c_c", (short) 1);
//		  for(Index ind : inds){
//		    System.out.println(ind.getDbName() + "." + ind.getIndexName());
//		  }
		  
		  //NODE TEST get_node listNodes
//		  Node no = msc.client.get_node("NODE32");
//		  System.out.println(no.getNode_name());
		  
//		  List<Node> nds = msc.client.listNodes();
//		  for(Node nd : nds){
//		    System.out.println(nd.getNode_name());
//		  }
		  
		  //NODEGROUP TEST listNodeGroups
//		  List<NodeGroup> ngs = msc.client.listNodeGroups();
//		  for(NodeGroup ng : ngs){
//		    System.out.println(ng.getNode_group_name() + "---size:" + ng.getNodesSize());
//		  }
		  
		  
		  
		  
		  //oldms 测试
		  
		  //DATABASE TEST createDatabase alterDatabase dropDatabase
		  
//		  Map<String, String> testmap = new HashMap<String,String>();
//		  Database db = new Database("testdb2", "test db for oldms", "/user/hive/warehouse/testdb2.db", testmap);
//		  msc.client.authentication("root", "111111");
//		  msc.client.createDatabase(db);
//		  Database dd =  msc.client.getDatabase("testdb2");
//		  System.out.println(dd);
		  
//		  Map<String, String> testmap = new HashMap<String,String>();
//		  Database db = msc.client.getDatabase("dbtest7");
//		  db.setParameters(testmap);
//		  msc.client.alterDatabase("dbtest7", db);
		  
//		  msc.client.dropDatabase("dbtest",true,true);
		  
		  //TABLE TEST dropTable createTable
//		  msc.client.authentication("root", "111111");
//		  msc.client.dropTable("dbtest7", "qydx15");
//		  msc.client.dropTable("dbtest7", "qydx3", true, true);
		  
//		  StorageDescriptor sdc = new StorageDescriptor();
//		  GlobalSchema sch = msc.client.getSchemaByName("c_c");
//		  sdc = sch.getSd();
//		  Table t = msc.client.getTable("db1", "c_c");
//		  Table tbl = new Table("zqhtest", "db1", "c_c", "root", 1389582161, 0, 0, sdc, t.getPartitionKeys(), t.getParameters(), "", "", "MANAGED_TABLE", t.getNodeGroups(), t.getFileSplitKeys());
//		  msc.client.createTable(tbl);

//		  Table t = msc.client.getTable("db1", "zqhtest");
//		  t.setTableName("zqhtest1");
//		  msc.client.alter_table("db1", "zqhtest", t);
		  
		  //NODE TEST add_node
//		  List<String> ips = new ArrayList<String>();
//		  ips.add("192.168.1.37");
//		  msc.client.add_node("NODE37", ips);
		  
		  //INDEX TEST dropIndex createIndex
//		  msc.client.dropIndex("hainan", "t_dx_rz_jkdx", "c_ydz_ascode", true);
//		  System.out.println(msc.client.getIndex("hainan", "t_dx_rz_jkdx", "c_ydz_ascode"));
		  
//		  Table tbl = msc.client.getTable("hainan", "t_dx_rz_jkdx");
//      StorageDescriptor sdc = tbl.getSd();
//      List<FieldSchema> fss = new ArrayList<FieldSchema>();
//      for(FieldSchema fs : sdc.getCols()){
//        if(!fs.getName().equals("c_ydz_ascode")){
//          fss.add(fs);
//        }
//      }
//      sdc.getCols().removeAll(fss);
//		  Index index = new Index("c_ydz_ascode", "lucene","hainan", "t_dx_rz_jkdx", 1384758438, 1384758438, "t_dx_rz_jkdx", sdc, null, true);
//		  msc.client.createIndex(index, tbl);
		  
//		  Table tbl = msc.client.getTable("hainan", "t_dx_rz_jkdx");
//		  StorageDescriptor sdc = tbl.getSd();
//		  List<FieldSchema> fss = new ArrayList<FieldSchema>();
//		  for(FieldSchema fs : sdc.getCols()){
//		    if(fs.getName().equals("c_ydz_ascode")){
//		      fss.add(fs);
//		    }
//		  }
//		  sdc.getCols().removeAll(fss);
//		  //msc.client.dropIndex("hainan", "t_dx_rz_jkdx", "c_ydz_ascode", true);
//		  Index index = new Index("c_ydz_ascode", "lucene","hainan", "t_dx_rz_jkdx", 1384758438, 1384758438, "t_dx_rz_jkdx", sdc, null, true);
//		  msc.client.alter_index("hainan", "t_dx_rz_jkdx", "c_ydz_ascode",index);
		  
		  Index ind = msc.client.getIndex("hainan", "t_dx_rz_jkdx", "c_ydz_ascode");
		  Index ind1 =msc.client.getIndex("hainan", "t_dx_rz_jkdx", "c_ydz_spcode");
		  ind.setParameters(ind1.getParameters());
		  msc.client.alter_index("hainan", "t_dx_rz_jkdx", "c_ydz_ascode", ind);
		  
		  
		  
		  
		  
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

}
