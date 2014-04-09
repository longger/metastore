package org.apache.hadoop.hive.metastore.newms;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.SFileLocation;

public class ObjectType {
  public enum TYPEID {
    DATABASE, TABLE, PARTITION, SFILE, INDEX, NODE, GLOBALSCHEMA, NODEGROUP, PRIVILEGE, SFILELOCATION, DEVICE,
  }

	public static final TypeDesc DATABASE = new TypeDesc(TYPEID.DATABASE, "database", Database.class);
	public static final TypeDesc TABLE = new TypeDesc(TYPEID.TABLE, "table", TableImage.class);
	public static final TypeDesc PARTITION = new TypeDesc(TYPEID.PARTITION, "partition", Partition.class);
	public static final TypeDesc SFILE = new TypeDesc(TYPEID.SFILE, "sfile", SFileImage.class);
	public static final TypeDesc INDEX = new TypeDesc(TYPEID.INDEX, "index", Index.class);
	public static final TypeDesc NODE = new TypeDesc(TYPEID.NODE, "node", Node.class);
	public static final TypeDesc GLOBALSCHEMA = new TypeDesc(TYPEID.GLOBALSCHEMA, "globalschema", GlobalSchema.class);
	public static final TypeDesc NODEGROUP = new TypeDesc(TYPEID.NODEGROUP, "nodegroup", NodeGroupImage.class);
	public static final TypeDesc PRIVILEGE = new TypeDesc(TYPEID.PRIVILEGE, "privilegebag", PrivilegeBag.class);
	public static final TypeDesc SFILELOCATION = new TypeDesc(TYPEID.SFILELOCATION, "sfilelocation", SFileLocation.class);
	public static final TypeDesc DEVICE = new TypeDesc(TYPEID.DEVICE, "device", Device.class);

	public static class TypeDesc
	{
		private final TYPEID id;
		private final String name;
		private final Class cla;

		public TypeDesc(TYPEID id, String name, Class c) {
			this.id = id;
			this.name = name;
			this.cla = c;
		}

		public TYPEID getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Class getCla() {
			return cla;
		}

		public boolean equals(TypeDesc t) {
			return this.getId() == t.getId();
		}
	}
}
