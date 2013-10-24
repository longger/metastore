package org.apache.hadoop.hive.metastore.metalog;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.zk.MetaConstants;
import org.apache.hadoop.hive.metastore.zk.ZKUtil;
import org.apache.hadoop.hive.metastore.zk.ZooKeeperWatcher;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

/**
 *
 * LogManager keep all db .
 *
 */
public class MetaLogManager implements Abortable {

  private static Log LOG = LogFactory.getLog(MetaLogManager.class);

  private static MetaLogManager instance = null;

  private ZooKeeperWatcher zooKeeper;
  private BookKeeper bkc;
  private BookKeeperAdmin bkcAdmin;
  private static RedoLogExecutor redoLogExecutor;

  private int bkensemble = 1;
  private int bkquorum = 1;
  private Configuration conf;
  private byte[] passwd ;
  private MetaData metaData;
//  public Map<Long, LedgerHandle> logMap;
  /**
   * LedgerHandleid->LedgerHandle
   */
  private final Map<Long, LedgerHandle> logMap = new HashMap<Long, LedgerHandle>();
  private Long active_log_id = -1L;

  private MetaLogManager() {
  };

  public Map<Long, LedgerHandle> getLogMap() {
    return logMap;
  }

  public static MetaLogManager getInstance(Configuration config) throws MetaException
  {
    if(instance == null){
      try {
        instance = new MetaLogManager();
        instance.conf = config;
        instance.initBookKeeper();
        redoLogExecutor = new RedoLogExecutor(instance);
        redoLogExecutor.replayLog();
      } catch (Exception e) {
        LOG.error(e, e);
        throw new MetaException(e.getMessage());
      }
    }
    return instance;
  }

  private void initBookKeeper() throws IOException, InterruptedException, KeeperException, BKException {
    if(conf == null){
      conf = new Configuration();
    }
    String zkservers = conf.get("meta.txnlog.bookkeeper.zkservers", "localhost:2181");
    bkensemble = Integer.valueOf(conf.get("meta.txnlog.bookkeeper.bkensemble", "1"));
    bkquorum = Integer.valueOf(conf.get("meta.txnlog.bookkeeper.bkquorum", "1"));
    passwd = conf.get("meta.txnlog.bookkeeper.passwd", "").getBytes();
    int bkthrottle = Integer.valueOf(conf.get("bkthrottle", "10000"));

    ClientConfiguration bkconf = new ClientConfiguration();
    bkconf.setThrottleValue(bkthrottle);
    this.zooKeeper = new ZooKeeperWatcher(conf,null, "metalog" , this, true);
    bkconf.setZkServers(zkservers);
    bkc = new BookKeeper(bkconf);
    bkcAdmin = new BookKeeperAdmin(bkc);

    metaData = MetaData.load(zooKeeper);

    for( Long id : metaData.getLogs()){
      LedgerHandle handle = null;
      try{
        handle = bkc.openLedgerNoRecovery(id, DigestType.CRC32, passwd);
        metaData.getMaxPos().put(id, handle.getLastAddConfirmed());
      }catch(BKNoSuchLedgerExistsException e){
        LOG.error(e,e);
        deleteLogFile(id);
        continue;
      }
      logMap.put(id, handle);
    }

  }

  private LedgerHandle createLogFile() throws InterruptedException, BKException {
    LedgerHandle lh = bkc.createLedger(bkensemble, bkquorum, DigestType.CRC32, passwd);
    logMap.put(lh.getId(), lh);
    return lh;
  }

//  public LedgerHandle getLogFile(String db) throws InterruptedException, BKException{
  public LedgerHandle getLogFile() throws InterruptedException, BKException{
    LedgerHandle lh = null;
//    if(logMap == null || logMap.isEmpty()){
//      lh = this.createLogFile();
//      metaData.put(MetaLogEntry.getTopDB(), lh.getId());
//    }else{
//      lh = logMap.get(metaData.getLogIdByDbName(MetaLogEntry.getTopDB()));
//    }
    if(active_log_id == -1L){
      lh = createLogFile();
      active_log_id = lh.getId();
    }else{
      lh = logMap.get(active_log_id);
    }
    return lh;
  }

  public void deleteLogFile(Long logId) throws InterruptedException, BKException, NoNodeException, IOException, KeeperException {
    metaData.isChanged = true;
    try{
      getBkc().deleteLedger(logId);
    }catch(BKNoSuchLedgerExistsException e){

    }
    logMap.remove(logId);
    metaData.remove(logId);
    metaData.flush(zooKeeper);
  }



  public BookKeeper getBkc() {
    return bkc;
  }

  public void setBkc(BookKeeper bkc) {
    this.bkc = bkc;
  }



  public MetaData getMetaData() {
    return metaData;
  }

  public void setMetaData(MetaData metaData) {
    this.metaData = metaData;
  }

  public BookKeeperAdmin getBkcAdmin() {
    return bkcAdmin;
  }

  public void setBkcAdmin(BookKeeperAdmin bkcAdmin) {
    this.bkcAdmin = bkcAdmin;
  }

  public long writeEntry(MetaLogEntry entry) throws Exception {
    LedgerHandle handle = null;
//    if(metaData.getLogIdByDbName(entry.getDbs()) !=  null){
//      Long id = metaData.getLogIdByDbName(entry.getDbs());

//  if(metaData.getLogIdByDbName(entry.getTopDB()) !=  null){
//  Long id = metaData.getLogIdByDbName(entry.getTopDB());
//    if(metaData.getLogIdByDbName(entry.getTopDB()) !=  null){
//      handle = logMap.get(id);
//    }else{
//      handle = this.createLogFile();
//
//    }
    handle = getLogFile();
    return handle.addEntry(entry.toByteArray());
  }


  public static class MetaData implements Writable{
    public static String metaLogZNode = MetaConstants.DEFAULT_ZOOKEEPER_ZNODE_LOG;
    /**
     * dbName->dbURL
     */
    private Long chkptPos = 0L;
    private boolean isChanged = false;
//    private Map<String,List<Long>> dbMap = new HashMap<String, Long>();
    private ConcurrentLinkedQueue<Long> logs = new ConcurrentLinkedQueue<Long>();
    private HashMap<Long,Long> maxPos = new HashMap<Long,Long>();


//    public Set<Entry<String, Long>> getLogEntrys(){
//      return dbMap.entrySet();
//    }
//
//    public void put(String dbName,Long id){
//      dbMap.put(dbName, id);
//      isChanged = true;
//    }
//
//    public void remove(String dbName){
//      dbMap.remove(dbName);
//      isChanged = true;
//    }
//
//    public Long getLogIdByDbName(String dbName) {
//      return dbMap.get(dbName);
//    }

    public Long getChkptPos() {
      return chkptPos;
    }

    public void remove(Long logId) {
      if(logs.contains(logId)){
        logs.remove(logId);
        maxPos.remove(logId);
      }

}

    public void setChkptPos(Long chkptPos) {
      this.chkptPos = chkptPos;
    }

    public static MetaData load(ZooKeeperWatcher zooKeeper) throws InterruptedException, IOException {
      byte [] data = null;
      MetaData meta = new MetaData();

      int retry = MetaConstants.DEFAULT_ZOOKEPER_CHECK_RETRY;
      while(retry -- >0){
        try {
          int exist = ZKUtil.checkExists(zooKeeper, metaLogZNode);
          if(exist < 0){
            meta.isChanged = true;
            meta.flush(zooKeeper);
            meta.isChanged = false;
          }
        } catch (KeeperException e) {
          LOG.error(e,e);
          continue;
        }
      }

      data =  ZKUtil.blockUntilAvailable(zooKeeper, metaLogZNode, MetaConstants.DEFAULT_ZK_SESSION_TIMEOUT);
      if (data == null) {
        return null;
      }else{

        for(int i=0;i<data.length;i++){

          LOG.info(" bytes=="+data[i]);
        }

        try {
          DataInputBuffer input = new DataInputBuffer();
          input.reset(data, data.length);

          for(int i=0;i<data.length;i++){

            LOG.info("=="+data[i]);
          }
          meta.readFields(input);
          return meta;
        } catch (IOException e) {
           LOG.error(e,e);
           throw e;
        }
      }

    }

    public void flush(ZooKeeperWatcher zooKeeper) throws IOException, NoNodeException, KeeperException {
      if(isChanged){
        DataOutputBuffer ouputBuffer = new DataOutputBuffer();
        this.write(ouputBuffer);
        byte[] data = ouputBuffer.getData();

        if(ZKUtil.checkExists(zooKeeper, metaLogZNode) == -1){
          ZKUtil.createNodeIfNotExistsAndWatch(zooKeeper, metaLogZNode, data);
        }

        ZKUtil.setData(zooKeeper, metaLogZNode, data);
        byte[] data2 = ZKUtil.getData(zooKeeper, metaLogZNode);
        for(int i=0;i<Math.min(data.length, data2.length);i++){
          if(data[i] != data2[i]){
            LOG.info("ERROR:----different");
          }
          LOG.info(data[i]);
        }
        LOG.info("--"+data.length+"--"+ data2.length);
        isChanged = false;
      }//end of change
    }

//    public void setDbMap(Map<String, Long> dbMap) {
//      this.dbMap = dbMap;
//    }

    @Override
    public void readFields(DataInput input) throws IOException {
      this.chkptPos = input.readLong();
      int num = input.readInt();
      if(num > 0 ){
        String dbString = WritableUtils.readString(input);
        Long id = input.readLong();
//        this.logs.put(dbString, id);
        this.logs.add( id);
      }
      LOG.info("logs.size--"+logs.size());
    }

    @Override
    public void write(DataOutput ouput) throws IOException {
      ouput.writeLong(this.chkptPos);
//      if(this.dbMap == null || this.dbMap.isEmpty()){
      if(this.logs == null || this.logs.isEmpty()){
        ouput.writeInt( 0);
        return;
      }else{
//        ouput.writeInt(dbMap.size());
      }

//      for(java.util.Map.Entry<String,Long> entry : dbMap.entrySet()){
//          WritableUtils.writeString(ouput,entry.getKey());
//          ouput.writeLong(entry.getValue());
//        }
      for(Long  log: logs){
        ouput.writeLong(log);
      }
    }

    public ConcurrentLinkedQueue<Long> getLogs() {
      return logs;
    }

    public void setLogs(ConcurrentLinkedQueue<Long> logs) {
      this.logs = logs;
    }

    public HashMap<Long,Long> getMaxPos() {
      return maxPos;
    }

    public void setMaxPos(HashMap<Long,Long> maxPos) {
      this.maxPos = maxPos;
    }

  }



  public static class Operation {
    public static final int NULL_OPERATION = 0;
    public static final int CRT_DATABASE = 1001;
    public static final int MOD_DATABASE = 1002;
    public static final int DEL_DATABASE = 1003;
    public static final int CRT_SCHEMA = 1101;
    public static final int MOD_SCHEMA = 1102;
    public static final int DEL_SCHEMA = 1103;
    public static final int CRT_TABLE = 1201;
    public static final int MOD_TABLE = 1202;
    public static final int DEL_TABLE = 1203;
    public static final int CRT_NODEGROUP = 1301;
    public static final int MOD_NODEGROUP = 1302;
    public static final int DEL_NODEGROUP = 1303;
    public static final int CRT_USER = 1401;
    public static final int MOD_USER = 1402;
    public static final int DEL_USER = 1403;
    public static final int CRT_ROLE = 1501;
    public static final int MOD_ROLE = 1502;
    public static final int DEL_ROLE = 1503;

    public int opType;


  }



  @Override
  public void abort(String arg0, Throwable arg1) {
    close();

  }

  @Override
  public boolean isAborted() {
    // TODO Auto-generated method stub
    return false;
  }

  public void close() {

    try {
      this.metaData.flush(zooKeeper);
//      for(LedgerHandle logHandle : this.logMap.values()){
//        try{
//          logHandle.close();
//        }catch(Exception e){
//          LOG.error(e,e);
//        }
//      }
      this.bkcAdmin.close();
      this.bkc.close();
    } catch (InterruptedException e) {

      LOG.error(e,e);
    } catch (BKException e) {

      LOG.error(e,e);
    } catch (NoNodeException e) {

      LOG.error(e,e);
    } catch (IOException e) {

      LOG.error(e,e);
    } catch (KeeperException e) {

      LOG.error(e,e);
    }

    this.zooKeeper.close();

  }

  public Long getActive_log_id() {
    return active_log_id;
  }

  public void setActive_log_id(Long active_log_id) {
    this.active_log_id = active_log_id;
  }




}
