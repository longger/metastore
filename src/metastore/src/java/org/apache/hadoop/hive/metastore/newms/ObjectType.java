package org.apache.hadoop.hive.metastore.newms;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.Table;

public class ObjectType {
	public static final TypeDesc DATABASE = new TypeDesc(1,"database", Database.class);
	public static final TypeDesc TABLE = new TypeDesc(2,"table", TableImage.class);
	public static final TypeDesc PARTITION = new TypeDesc(3,"partition", Partition.class);
	public static final TypeDesc SFILE = new TypeDesc(4,"sfile", SFileImage.class);
	public static final TypeDesc INDEX = new TypeDesc(5,"index",Index.class);
	public static final TypeDesc NODE = new TypeDesc(6,"node", Node.class);
	public static final TypeDesc GLOBALSCHEMA = new TypeDesc(7,"globalschema",GlobalSchema.class);
	public static final TypeDesc NODEGROUP = new TypeDesc(8,"nodegroup", NodeGroupImage.class);
	public static final TypeDesc PRIVILEGE = new TypeDesc(9,"privilegebag", PrivilegeBag.class);
	public static final TypeDesc SFILELOCATION = new TypeDesc(10,"sfilelocation",SFileLocation.class);
	public static final TypeDesc DEVICE = new TypeDesc(11,"device",Device.class);
	
	public static class TypeDesc
	{
		private int id;
		private String name;
		private Class cla;
		
		public TypeDesc(int id, String name, Class c) {
			this.id = id;
			this.name = name;
			this.cla = c;
		}
		
		public int getId() {
			return id;
		}
		
		public String getName() {
			return name;
		}
		
		public Class getCla() {
			return cla;
		}

		public boolean equals(TypeDesc t)
		{
			return this.getId() == t.getId();
		}
	}
}
