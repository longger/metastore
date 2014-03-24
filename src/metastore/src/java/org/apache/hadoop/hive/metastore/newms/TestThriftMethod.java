package org.apache.hadoop.hive.metastore.newms;

import iie.metastore.MetaStoreClient;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Device;
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
		  
		  //DEVICE TEST getDevice
//		  Device de = msc.client.getDevice("222e1000155b7615b");
//		  System.out.println(de.getDevid() + de.getNg_name());
		  
		  List<Device> des = msc.client.listDevice();
		  for(Device de : des){
		    System.out.println(de.getDevid() + "----" + de.getNg_name() + "----" + de.getNode_name());
		  }
		  
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
		  
		  //NODEGROUP TEST
//		  List<NodeGroup> ngs = msc.client.listNodeGroups();
//		  for(NodeGroup ng : ngs){
//		    System.out.println(ng.getNode_group_name() + "---size:" + ng.getNodesSize());
//		  }
		  
		  
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

}
