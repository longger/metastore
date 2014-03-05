package org.apache.hadoop.hive.metastore.newms;

public class ObjectType {
	public static final TypeDesc DATABASE = new TypeDesc(1,"database");
	public static final TypeDesc TABLE = new TypeDesc(2,"table");
	public static final TypeDesc PARTITION = new TypeDesc(3,"partition");
	public static final TypeDesc SFILE = new TypeDesc(4,"sfile");
	public static final TypeDesc INDEX = new TypeDesc(5,"index");
	public static final TypeDesc NODE = new TypeDesc(6,"node");
	public static final TypeDesc GLOBALSCHEMA = new TypeDesc(7,"globalschema");
	public static final TypeDesc NODEGROUP = new TypeDesc(8,"nodegroup");
	public static final TypeDesc PRIVILEGE = new TypeDesc(9,"privilegebag");
	public static final TypeDesc SFILELOCATION = new TypeDesc(10,"sfilelocation");
	
	public static class TypeDesc
	{
		private int id;
		private String name;
		
		public TypeDesc(int id, String name) {
			this.id = id;
			this.name = name;
		}
		
		public int getId() {
			return id;
		}
		
		public String getName() {
			return name;
		}
		public boolean equals(TypeDesc t)
		{
			return this.getId() == t.getId();
		}
	}
}
