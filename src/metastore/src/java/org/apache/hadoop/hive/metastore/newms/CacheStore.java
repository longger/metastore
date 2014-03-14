package org.apache.hadoop.hive.metastore.newms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SplitValue;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class CacheStore {
  private RedisFactory rf;
  private NewMSConf conf;
  private static boolean initialized = false;
  private static String sha = null;
 
  private static ConcurrentHashMap<String, Database> databaseHm = new ConcurrentHashMap<String, Database>();
  private static ConcurrentHashMap<String, PrivilegeBag> privilegeBagHm = new ConcurrentHashMap<String, PrivilegeBag>();
  private static ConcurrentHashMap<String, Partition> partitionHm = new ConcurrentHashMap<String, Partition>();
  private static ConcurrentHashMap<String, Node> nodeHm = new ConcurrentHashMap<String, Node>();
  private static ConcurrentHashMap<String, NodeGroup> nodeGroupHm = new ConcurrentHashMap<String, NodeGroup>();
  private static ConcurrentHashMap<String, GlobalSchema> globalSchemaHm = new ConcurrentHashMap<String, GlobalSchema>();
  private static ConcurrentHashMap<String, Table> tableHm = new ConcurrentHashMap<String, Table>();
  private static ConcurrentHashMap<String, Index> indexHm = new ConcurrentHashMap<String, Index>();
  private static ConcurrentHashMap<String, Device> deviceHm = new ConcurrentHashMap<String, Device>();
  private static TimeLimitedCacheMap sFileHm = new TimeLimitedCacheMap(270, 60, 300, TimeUnit.SECONDS);
  private static TimeLimitedCacheMap sflHm = new TimeLimitedCacheMap(270, 60, 300, TimeUnit.SECONDS);

  public CacheStore(NewMSConf conf)
  {
    this.conf = conf;
    rf = new RedisFactory(conf);

    initialize();
  }
//初始化与类相关的静态属性，为的是这些属性只初始化一次
  private void initialize()
  {
    synchronized (this.getClass()) {
      if(!initialized)
      {
        Jedis jedis = null;
        try {
          //向sorted set中加入一个元素，score是从0开始的整数(listtablefiles方法的from和to，zrange方法中的参数都是从0开始的，正好一致)，自动递增
          //如果元素已经存在，不做任何操作
          jedis = rf.getDefaultInstance();
          String script = "local score = redis.call('zscore',KEYS[1],ARGV[1]);"
              + "if not score then "        //lua里只有false和nil被认为是逻辑的非
              + "local size = redis.call('zcard',KEYS[1]);"
              + "return redis.call('zadd',KEYS[1],size,ARGV[1]); end";
          sha = jedis.scriptLoad(script);
          
          //每次系统启动时，从redis中读取已经持久化的对象到内存缓存中(SFile和SFileLocation除外)
          long start = System.currentTimeMillis();
          readAll(ObjectType.DATABASE);
          readAll(ObjectType.GLOBALSCHEMA);
          readAll(ObjectType.INDEX);
          readAll(ObjectType.NODE);
          readAll(ObjectType.NODEGROUP);
          readAll(ObjectType.PARTITION);
          readAll(ObjectType.PRIVILEGE);
          readAll(ObjectType.TABLE);
          readAll(ObjectType.DEVICE);
          readAll(ObjectType.SFILE);
          readAll(ObjectType.SFILELOCATION);
          long end = System.currentTimeMillis();
          System.out.println("loading objects from redis into cache takes "+(end-start)+" ms");
          
        } catch (JedisConnectionException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          RedisFactory.putBrokenInstance(jedis);
          jedis = null;
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (ClassNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }finally{
          RedisFactory.putInstance(jedis);
        }
      }
      initialized = true;
    }
  }


  public void writeObject(ObjectType.TypeDesc key, String field, Object o)throws JedisConnectionException, IOException {
    Jedis jedis = null;
    try{
      jedis = rf.getDefaultInstance();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      jedis.hset(key.getName().getBytes(), field.getBytes(), baos.toByteArray());

      if(key.equals(ObjectType.SFILE))
      {
      	SFileImage sfi = (SFileImage)o;
      	//为listtablefiles
      	if(sfi.getDbName() != null && sfi.getTableName() != null)
      	{
      		String k = generateLtfKey(sfi.getTableName(), sfi.getDbName());
      		jedis.evalsha(sha, 1, k, sfi.getFid()+"");
      	}
      	//为filtertablefiles
      	if(sfi.getValues() != null && sfi.getValues().size() > 0)
      	{
      		String k2 = generateFtlKey(sfi.getValues());
      		jedis.sadd(k2, sfi.getFid()+"");
      	}
      	//listFilesByDegist
      	if(sfi.getDigest() != null)
      	{
      		String k = generateLfbdKey(sfi.getDigest());
      		jedis.sadd(k, sfi.getFid()+"");
      	}
      	//为findFiles
      	{
      		String k = generateSfStatKey(sfi.getStore_status());
      		jedis.sadd(k, sfi.getFid()+"");
      	}
      	
      }
      else if(key.equals(ObjectType.SFILELOCATION)) {
      	sflHm.put(field, (SFileLocation)o);
      	
      	//为了getSFileLocations(int status)
      	SFileLocation sfl = (SFileLocation)o;
      	jedis.sadd(generateSflStatKey(sfl.getVisit_status()), SFileImage.generateSflkey(sfl.getLocation(), sfl.getDevid()));
      }
      else if(key.equals(ObjectType.DATABASE)) {
        databaseHm.put(field, (Database)o);
      }
  //    if(key.equals(ObjectType.TABLE))
  //      tableHm.put(field, (Table)o);
  //    对于sfile，函数参数是sfileimage
      if(key.equals(ObjectType.INDEX)) {
        indexHm.put(field, (Index)o);
      }
      else if(key.equals(ObjectType.NODE)) {
        nodeHm.put(field, (Node)o);
      }
  //    if(key.equals(ObjectType.NODEGROUP))
  //      nodeGroupHm.put(field, (NodeGroup)o);
      else if(key.equals(ObjectType.GLOBALSCHEMA)) {
        globalSchemaHm.put(field, (GlobalSchema)o);
      }
      else if(key.equals(ObjectType.PRIVILEGE)) {
        privilegeBagHm.put(field, (PrivilegeBag)o);
      }
      else if(key.equals(ObjectType.PARTITION)) {
        partitionHm.put(field, (Partition)o);
      }
      else if(key.equals(ObjectType.DEVICE)) {
      	deviceHm.put(field, (Device)o);
      }
    }catch(JedisConnectionException e){
      RedisFactory.putBrokenInstance(jedis);
      jedis = null;
      throw e;
    }finally{
      RedisFactory.putInstance(jedis);
    }
  }

  public Object readObject(ObjectType.TypeDesc key, String field)throws JedisConnectionException, IOException,ClassNotFoundException {
    Object o = null;
    if(key.equals(ObjectType.SFILE)) {
      o = sFileHm.get(field);
    }
    else if(key.equals(ObjectType.SFILELOCATION)) {
      o = sflHm.get(field);
    }
    else if(key.equals(ObjectType.DATABASE)) {
      o = databaseHm.get(field);
    }
    else if(key.equals(ObjectType.TABLE)) {
      o = tableHm.get(field);
    }
    else if(key.equals(ObjectType.INDEX)) {
      o = indexHm.get(field);
    }
    else if(key.equals(ObjectType.NODE)) {
      o = nodeHm.get(field);
    }
    else if(key.equals(ObjectType.NODEGROUP)) {
      o = nodeGroupHm.get(field);
    }
    else if(key.equals(ObjectType.GLOBALSCHEMA)) {
      o = globalSchemaHm.get(field);
    }
    else if(key.equals(ObjectType.PRIVILEGE)) {
      o = privilegeBagHm.get(field);
    }
    else if(key.equals(ObjectType.PARTITION)) {
      o = partitionHm.get(field);
    }
    else if(key.equals(ObjectType.DEVICE)) {
      o = deviceHm.get(field);
    }
    if(o != null)
    {
      System.out.println("in function readObject: read "+key.getName()+":"+field+" from cache.");
      return o;
    }
    Jedis jedis = null;
    try{
      jedis = rf.getDefaultInstance();
      byte[] buf = jedis.hget(key.getName().getBytes(), field.getBytes());
      if(buf == null) {
        return null;
      }
      ByteArrayInputStream bais = new ByteArrayInputStream(buf);
      ObjectInputStream ois = new ObjectInputStream(bais);
      o = ois.readObject();
      //SFile 要特殊处理
      if(key.equals(ObjectType.SFILE))
      {
        SFileImage sfi = (SFileImage)o;
        List<SFileLocation> locations = new ArrayList<SFileLocation>();
        for(int i = 0;i<sfi.getSflkeys().size();i++)
        {
          SFileLocation sfl = (SFileLocation)readObject(ObjectType.SFILELOCATION, sfi.getSflkeys().get(i));
          if(sfl != null) {
            locations.add(sfl);
          }
        }
        SFile sf =  new SFile(sfi.getFid(),sfi.getDbName(),sfi.getTableName(),sfi.getStore_status(),sfi.getRep_nr(),
            sfi.getDigest(),sfi.getRecord_nr(),sfi.getAll_record_nr(),locations,sfi.getLength(),
            sfi.getRef_files(),sfi.getValues(),sfi.getLoad_status());
        sFileHm.put(field, sf);
        o = sf;
      }

      //table
      else if(key.equals(ObjectType.TABLE))
      {
        TableImage ti = (TableImage)o;
        List<NodeGroup> ngs = new ArrayList<NodeGroup>();
        for(int i = 0;i<ti.getNgKeys().size();i++)
        {
          NodeGroup ng = (NodeGroup)readObject(ObjectType.NODEGROUP, ti.getNgKeys().get(i));
          if(ng != null) {
            ngs.add(ng);
          }
        }
        Table t = new Table(ti.getTableName(),ti.getDbName(),ti.getSchemaName(),
            ti.getOwner(),ti.getCreateTime(),ti.getLastAccessTime(),ti.getRetention(),
            ti.getSd(),ti.getPartitionKeys(),ti.getParameters(),ti.getViewOriginalText(),
            ti.getViewExpandedText(),ti.getTableType(),ngs,ti.getFileSplitKeys());
        tableHm.put(field, t);
        o = t;
      }

      //nodegroup
      else if(key.equals(ObjectType.NODEGROUP))
      {
        NodeGroupImage ngi = (NodeGroupImage)o;
        Set<Node> nodes = new HashSet<Node>();
        for(String s : ngi.getNodeKeys())
        {
          Node n = (Node)readObject(ObjectType.NODE, s);
          if(n != null) {
            nodes.add(n);
          }
        }
        NodeGroup ng = new NodeGroup(ngi.getNode_group_name(), ngi.getComment(), ngi.getStatus(), nodes);
        nodeGroupHm.put(field, ng);
        o = ng;
      }

      else if(key.equals(ObjectType.DATABASE)) {
        databaseHm.put(field, (Database)o);
      }
      else if(key.equals(ObjectType.SFILELOCATION)) {
        sflHm.put(field, (SFileLocation)o);
      }
      else if(key.equals(ObjectType.INDEX)) {
        indexHm.put(field, (Index)o);
      }
      else if(key.equals(ObjectType.NODE)) {
        nodeHm.put(field, (Node)o);
      }
      else if(key.equals(ObjectType.GLOBALSCHEMA)) {
        globalSchemaHm.put(field, (GlobalSchema)o);
      }
      else if(key.equals(ObjectType.PRIVILEGE)) {
        privilegeBagHm.put(field, (PrivilegeBag)o);
      }
      else if(key.equals(ObjectType.PARTITION)) {
        partitionHm.put(field, (Partition)o);
      }
      else if(key.equals(ObjectType.DEVICE)) {
        deviceHm.put(field, (Device)o);
      }
      System.out.println("in function readObject: read "+key.getName()+":"+field+" from redis.");


    }catch(JedisConnectionException e){
      RedisFactory.putBrokenInstance(jedis);
      jedis = null;
      throw e;
    }finally{
      RedisFactory.putInstance(jedis);
    }
    return o;
  }

  public void removeObject(ObjectType.TypeDesc key, String field) throws JedisConnectionException, IOException, ClassNotFoundException
  {
    //删除一个sfile时要把预先建立的一些信息也删掉
    if(key.equals(ObjectType.SFILE))
    {
        SFile sf = (SFile) readObject(key, field);
        if(sf != null)
        {
          Jedis jedis = null;
          try{
            jedis = rf.getDefaultInstance();
            Pipeline p = jedis.pipelined();
            p.srem(generateLfbdKey(sf.getDigest()), field);
            p.srem(generateFtlKey(sf.getValues()), field);
            p.zrem(generateLtfKey(sf.getTableName(), sf.getDbName()), field);
            p.srem(generateSflStatKey(sf.getStore_status()), sf.getFid()+"");
            p.hdel(key.getName(), field);
            p.sync();
          }catch(JedisConnectionException e){
            RedisFactory.putBrokenInstance(jedis);
            jedis = null;
            throw e;
          }finally{
            RedisFactory.putInstance(jedis);
          }
        }
      
    } else if(key.equals(ObjectType.SFILELOCATION)){
    	SFileLocation sfl = (SFileLocation) readObject(key, field);
      if(sfl != null)
      {
        Jedis jedis = null;
        try{
          jedis = rf.getDefaultInstance();
          jedis.srem(generateSflStatKey(sfl.getVisit_status()), field);
        }catch(JedisConnectionException e){
          RedisFactory.putBrokenInstance(jedis);
          jedis = null;
          throw e;
        }finally{
          RedisFactory.putInstance(jedis);
        }
      }
    }
    else
    {
    	Jedis jedis = null;
      try{
        jedis = rf.getDefaultInstance();
        jedis.hdel(key.getName(), field);
      }catch(JedisConnectionException e){
        RedisFactory.putBrokenInstance(jedis);
        jedis = null;
        throw e;
      }finally{
        RedisFactory.putInstance(jedis);
      }
    }

    if(key.equals(ObjectType.SFILE)){
    	sFileHm.remove(field);
    }
    else if(key.equals(ObjectType.DATABASE)) {
      databaseHm.remove(field);
    }
    else if(key.equals(ObjectType.TABLE)) {
      tableHm.remove(field);
    }
    else if(key.equals(ObjectType.SFILELOCATION)) {
      sflHm.remove(field);
    }
    else if(key.equals(ObjectType.INDEX)) {
      indexHm.remove(field);
    }
    else if(key.equals(ObjectType.NODE)) {
      nodeHm.remove(field);
    }
    else if(key.equals(ObjectType.NODEGROUP)) {
      nodeGroupHm.remove(field);
    }
    else if(key.equals(ObjectType.GLOBALSCHEMA)) {
      globalSchemaHm.remove(field);
    }
    else if(key.equals(ObjectType.PRIVILEGE)) {
      privilegeBagHm.remove(field);
    }
    else if(key.equals(ObjectType.PARTITION)) {
      partitionHm.remove(field);
    }
    else if(key.equals(ObjectType.DEVICE)) {
      deviceHm.remove(field);
    }

  }

  private void readAll(ObjectType.TypeDesc key) throws JedisConnectionException, IOException, ClassNotFoundException
  {
    Jedis jedis = null;
    try{
      jedis = rf.getDefaultInstance();
      Set<String> fields = jedis.hkeys(key.getName());

      if(fields != null)
      {
        if(key.equals(ObjectType.SFILE)|| key.equals(ObjectType.SFILELOCATION))
        {
          System.out.println(fields.size()+" "+key.getName()+" in redis.");
          return;
        }
        System.out.println("read "+fields.size()+" "+key.getName()+" from redis into cache.");
        for(String field : fields)
        {
          readObject(key, field);
        }
      }
    }catch(JedisConnectionException e){
      RedisFactory.putBrokenInstance(jedis);
      jedis = null;
      throw e;
    }finally{
      RedisFactory.putInstance(jedis);
    }
  }


  public List<Long> listTableFiles(String dbName, String tabName, int from, int to)
  {
    Jedis jedis = null;
    try{
      jedis = rf.getDefaultInstance();
      String k = generateLtfKey(tabName, dbName);
      Set<String> ss = jedis.zrange(k, from, to);
      List<Long> ids = new ArrayList<Long>();
      if(ss != null) {
        for(String id : ss) {
          ids.add(Long.parseLong(id));
        }
      }
      return ids;
    }catch(JedisConnectionException e){
      RedisFactory.putBrokenInstance(jedis);
      jedis = null;
      throw e;
    }finally{
      RedisFactory.putInstance(jedis);
    }


  }

  public List<SFile> filterTableFiles(String dbName, String tabName, List<SplitValue> values)
  {
    Jedis jedis = null;
    try{
      jedis = rf.getDefaultInstance();
      String k = generateFtlKey(values);
      Set<String> mem = jedis.smembers(k);
      List<SFile> rls = new ArrayList<SFile>();
      if(mem != null)
      {
        for(String id : mem)
        {
          SFile f = null;
          try {
            f = (SFile) readObject(ObjectType.SFILE, id);
            if(f != null) {
              rls.add(f);
            }
          } catch(Exception e) {
            e.printStackTrace();
          }
        }
      }
      return rls;
    }catch(JedisConnectionException e){
      RedisFactory.putBrokenInstance(jedis);
      jedis = null;
      throw e;
    }finally{
      RedisFactory.putInstance(jedis);
    }

  }

  public List<Long> listFilesByDegist(String degist)
  {
    Jedis jedis = null;
    try{
      jedis = rf.getDefaultInstance();
      String k = generateLfbdKey(degist);
      Set<String> ids = jedis.smembers(k);
      List<Long> rl = new ArrayList<Long>();
      if(ids != null) {
        for(String s : ids) {
          rl.add(Long.parseLong(s));
        }
      }
      return rl;
    }catch(JedisConnectionException e){
      RedisFactory.putBrokenInstance(jedis);
      jedis = null;
      throw e;
    }finally{
      RedisFactory.putInstance(jedis);
    }

  }

  public List<String> get_all_tables(String dbname)
  {
    Jedis jedis = null;
    try{
      jedis = rf.getDefaultInstance();
      Set<String> k = jedis.hkeys(ObjectType.TABLE.getName());
      List<String> rl = new ArrayList<String>();
      for(String dt : k)
      {
        if(dt.startsWith(dbname)) {
          rl.add(dt.split("\\.")[1]);
        }
      }
      return rl;
    }catch(JedisConnectionException e){
      RedisFactory.putBrokenInstance(jedis);
      jedis = null;
      throw e;
    }finally{
      RedisFactory.putInstance(jedis);
    }
  }
  
  public List<SFileLocation> getSFileLocations(int status) throws JedisConnectionException, IOException, ClassNotFoundException
  {
  	long start = System.currentTimeMillis();
  	Jedis jedis = null;
  	List<SFileLocation> sfll = new ArrayList<SFileLocation>();
    try{
      jedis = rf.getDefaultInstance();
      ScanResult<String> re = null;
      ScanParams sp = new ScanParams();
      sp.count(5000);
  		String cursor = "0";
  		do{
  			re = jedis.sscan(generateSflStatKey(status), cursor,sp);
  			cursor = re.getStringCursor();
//  			System.out.println(cursor +"  "+re.getResult().size());
  			for(String en : re.getResult())
  			{
  				 ByteArrayInputStream bais = new ByteArrayInputStream(jedis.hget(ObjectType.SFILELOCATION.getName().getBytes(), en.getBytes()));
  		     ObjectInputStream ois = new ObjectInputStream(bais);
  		     SFileLocation sfl = (SFileLocation)ois.readObject();
  		     sfll.add(sfl);
  			}
  		}while(!cursor.equals("0"));
  		System.out.println("in cache store, getSFileLocations() consume "+(System.currentTimeMillis()-start)+"ms");
      return sfll;
    }catch(JedisConnectionException e){
      RedisFactory.putBrokenInstance(jedis);
      jedis = null;
      throw e;
    }finally{
      RedisFactory.putInstance(jedis);
    }
  }
  
	public void findVoidFiles(List<SFile> voidFiles) throws JedisConnectionException, IOException, ClassNotFoundException {
		long start = System.currentTimeMillis();
  	Jedis jedis = null;
    try{
      jedis = rf.getDefaultInstance();
      ScanResult<String> re = null;
      ScanParams sp = new ScanParams();
      sp.count(5000);
  		String cursor = "0";
  		do{
  			re = jedis.sscan(generateSfStatKey(MetaStoreConst.MFileStoreStatus.INCREATE), cursor,sp);
  			cursor = re.getStringCursor();
  			for(String en : re.getResult())
  			{
  				 ByteArrayInputStream bais = new ByteArrayInputStream(jedis.hget(ObjectType.SFILE.getName().getBytes(), en.getBytes()));
  		     ObjectInputStream ois = new ObjectInputStream(bais);
  		     SFile sf = (SFile)ois.readObject();
  		     boolean ok = false;
  		     for(SFileLocation sfl : sf.getLocations())
  		     {
  		    	 if(sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE)
  		    	 {
  		    		 ok = true;
  		    		 break;
  		    	 }
  		     }
  		     if(!ok)
  		    	 voidFiles.add(sf);
  		     
  			}
  		}while(!cursor.equals("0"));
  		System.out.println("in cache store, findVoidFiles() consume "+(System.currentTimeMillis()-start)+"ms");
    }catch(JedisConnectionException e){
      RedisFactory.putBrokenInstance(jedis);
      jedis = null;
      throw e;
    }finally{
      RedisFactory.putInstance(jedis);
    }
	}
	
	public void findFiles(List<SFile> underReplicated, List<SFile> overReplicated, List<SFile> lingering,
      long from, long to) throws JedisConnectionException, IOException, ClassNotFoundException, MetaException {
		long start = System.currentTimeMillis();
		long node_nr = CacheStore.getNodeHm().size();
		List<SFile> temp = new LinkedList<SFile>();
    if (underReplicated == null || overReplicated == null || lingering == null) {
      throw new MetaException("Invalid input List<SFile> collection. IS NULL");
    }
  	Jedis jedis = null;
    try {
    	jedis = rf.getDefaultInstance();
    	for(String key : jedis.keys(generateSfStatKey(-1)))
    	{
    		if(!key.equals(generateSfStatKey(MetaStoreConst.MFileStoreStatus.INCREATE)))
    		{
    			ScanResult<String> re = null;
          ScanParams sp = new ScanParams();
          sp.count(5000);
      		String cursor = "0";
      		do{
      			re = jedis.sscan(key, cursor,sp);
      			cursor = re.getStringCursor();
      			for(String en : re.getResult())
      			{
      				 ByteArrayInputStream bais = new ByteArrayInputStream(jedis.hget(ObjectType.SFILE.getName().getBytes(), en.getBytes()));
      		     ObjectInputStream ois = new ObjectInputStream(bais);
      		     SFile sf = (SFile)ois.readObject();
      		     
      		     temp.add(sf);
      			}
      		}while(!cursor.equals("0"));
    		}
    	}
    	
    }catch(JedisConnectionException e){
      RedisFactory.putBrokenInstance(jedis);
      jedis = null;
      throw e;
    }finally{
      RedisFactory.putInstance(jedis);
    }
    List<SFile> files = temp.subList((int)from, (int)to);
    for(SFile m : files)
    {
    	List<SFileLocation> l = m.getLocations();

      // find under replicated files
      if (m.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED ||
          m.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
        int nr = 0;

        for (SFileLocation fl : l) {
          if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            nr++;
          }
        }
        if (m.getRep_nr() > nr) {
          try {
            underReplicated.add(m);
          } catch (javax.jdo.JDOObjectNotFoundException e) {
            // it means the file slips ...
            e.printStackTrace();
          }
        }
      }
      
      // find over  replicated files
      if (m.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
        int nr = 0;

        for (SFileLocation fl : l) {
          if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            nr++;
          }
        }
        if (m.getRep_nr() < nr) {
          try {
            overReplicated.add(m);
          } catch (javax.jdo.JDOObjectNotFoundException e) {
            // it means the file slips ...
          	e.printStackTrace();
          }
        }
      }
      // find lingering files
      if (m.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL) {
        lingering.add(m);
      }
      if (m.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
        int offnr = 0, onnr = 0;

        for (SFileLocation fl : l) {
          if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            onnr++;
          } else if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.OFFLINE) {
            offnr++;
          }
        }
        if ((m.getRep_nr() <= onnr && offnr > 0) ||
            (onnr + offnr >= node_nr && offnr > 0)) {
          try {
            lingering.add(m);
          } catch (javax.jdo.JDOObjectNotFoundException e) {
            // it means the file slips ...
          	e.printStackTrace();
          }
        }
      }
    }
    System.out.println("in cache store, findFiles() consume "+(System.currentTimeMillis()-start)+"ms");   
	}

  private String generateLtfKey(String tablename, String dbname)
  {
    String p = "sf.ltf.";
    if(tablename == null || dbname == null) {
      return p;
    }
    return p+dbname+"."+tablename;
  }

  private String generateFtlKey(List<SplitValue> value )
  {
    String p = "sf.ftf.";
    if(value == null) {
      return p;
    }
    return p+value.hashCode();
  }
  private String generateLfbdKey(String digest)
  {
    String p = "sf.lfbd.";
    if(digest == null) {
      return p;
    }
    return p+digest;
  }
  private String generateSflStatKey(int status)
  {
  	return "sfl.stat."+status;
  }
  private String generateSfStatKey(int status)
  {
  	if(status < 0)
  		return "sf.stat.*";
  	return "sf.stat."+status;
  }


  public static ConcurrentHashMap<String, Database> getDatabaseHm() {
    return databaseHm;
  }


  public RedisFactory getRf() {
    return rf;
  }

  public NewMSConf getConf() {
    return conf;
  }

  public static ConcurrentHashMap<String, PrivilegeBag> getPrivilegeBagHm() {
    return privilegeBagHm;
  }

  public static ConcurrentHashMap<String, Partition> getPartitionHm() {
    return partitionHm;
  }

  public static ConcurrentHashMap<String, Node> getNodeHm() {
    return nodeHm;
  }

  public static ConcurrentHashMap<String, NodeGroup> getNodeGroupHm() {
    return nodeGroupHm;
  }

  public static ConcurrentHashMap<String, GlobalSchema> getGlobalSchemaHm() {
    return globalSchemaHm;
  }

  public static ConcurrentHashMap<String, Table> getTableHm() {
    return tableHm;
  }

  public static ConcurrentHashMap<String, Index> getIndexHm() {
    return indexHm;
  }
	public static TimeLimitedCacheMap getsFileHm() {
		return sFileHm;
	}
	public static TimeLimitedCacheMap getSflHm() {
		return sflHm;
	}
	public static ConcurrentHashMap<String, Device> getDeviceHm() {
		return deviceHm;
	}



}

