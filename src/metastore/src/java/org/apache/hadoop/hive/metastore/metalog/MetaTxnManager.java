package org.apache.hadoop.hive.metastore.metalog;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Random;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.metalog.MetaTxn.TxnLog;


/**
 *  需要同步的元素和操作如下：
    模式（schema，包含列等相关信息）：增删改
    视图（view,特指在schema中的视图）：增删改
    语义（busitype）：增删改
    归属（database）：增删改
    用户（user，role）：增删改
    权限 （privilege）：增删改
    部分物理信息（节点组，节点？）：增删改
 * MetaTxnManager.
 *
 */
public class MetaTxnManager {
  private static Log LOG = LogFactory.getLog(MetaTxnManager.class);

  private static MetaTxnManager instance ;

  Long nextTxnId = 0L;

  MetaLogManager txnLogManager;

  Short logIdPrefixShort;

  private MetaTxnManager(Configuration conf) throws MetaException{
    logIdPrefixShort = getIPHash();
    txnLogManager = MetaLogManager.getInstance(conf);
  }

  public static MetaTxnManager getInstance(Configuration conf) throws MetaException{
    instance = new MetaTxnManager(conf);
    return instance;
  }

  LinkedHashMap<Long, Transaction> txns =
      new LinkedHashMap<Long, Transaction>();

  public Transaction newTransaction() throws MetaException{
    return newTransaction(null);
  }


  public Transaction newTransaction(Class<? extends Transaction> clazz) throws MetaException{
    if(clazz == null){
      clazz = MetaTxn.class;
    }
    Transaction txn = null;
    try {
      TxnLog txnLog = new TxnLog(txnLogManager.getLogFile(),this);
      txn = (Transaction)clazz.getConstructor(Long.class,TxnLog.class).newInstance(getnextTxnId(),txnLog);

//      txn = new MetaTxn(getnextTxnId());
      txns.put(txn.getTxnId(), txn);
      return txn;
    } catch (Exception e) {
      // TODO Auto-generated catch block
      LOG.error(e,e);
      throw new MetaException("open new Transation failed.");
    }

  }

  public void close() {
    this.txnLogManager.close();

  }

  /**
   * can berden
   *
   * long striped 8 byes,2 bytes local_prefix,6 bytes time(mini-sec)
   * [ip3][ip4][time1][time1][time1][time1][time1][time1]
   *
   * @return
   */
  private Long getnextTxnId(){
    Long cur_id = nextTxnId;
    Long timeLong = System.currentTimeMillis();
    Long prefix = (long)logIdPrefixShort;
    nextTxnId = timeLong ^ (prefix << 48);
    LOG.info("nextTxnid="+nextTxnId+"--"+timeLong+"--"+(prefix << 48));
    return cur_id;
  }

  public short getIPHash(){
    try {
      InetAddress.getLocalHost();
      InetAddress ip = InetAddress.getLocalHost();
      int hashCode = ip.hashCode();
      Short retShort = (short)(hashCode>>16);
      //long.max=9223372036854775807
      retShort = (short) ((retShort ^ (hashCode & 0xFFFF)) % 922337);
      return retShort;
    } catch (UnknownHostException e) {
      return (short) (new Random().nextInt()& 0xFFFF);
    }
  }

  public static void main(String args[]) throws Exception{
    Long timeLong = System.currentTimeMillis();
    MetaTxnManager txnManager = MetaTxnManager.getInstance(null);
//    LOG.info(txnManager.newTransaction().getTxnId());
    LOG.info(Long.MAX_VALUE);

    MetaTxn txn = (MetaTxn)txnManager.newTransaction();
    ArrayList<Object> paramsArrayList = new ArrayList<Object>();
    paramsArrayList.add(new Database("a","","",null));
    txn.preExecuteTxn(new MetaLogEntry(txn.getTxnId(),1001,paramsArrayList));
    txn.executeTxnOK(null);
    txn.commitTxn();

    LedgerHandle logHandle = txnManager.txnLogManager.getBkc().
        openLedger(txn.getLog().log.getId(),  DigestType.CRC32, "".getBytes());
    long num = logHandle.getLastAddConfirmed();
    LOG.info("##"+num+"=="+logHandle.getLastAddConfirmed()+"--"+logHandle.getLastAddPushed());
    Enumeration<LedgerEntry> it = logHandle.readEntries(0, num);
    while(it.hasMoreElements()){
      MetaLogEntry record = new MetaLogEntry(0);

      LedgerEntry recordEntry  = it.nextElement();
      record.fromByteArray(recordEntry.getEntry());
      LOG.info("record:"+record.toString());
    }


    LOG.info("Test Done");
    BookKeeperAdmin admin = txnManager.txnLogManager.getBkcAdmin();

    Iterable<Long> ledgerIds = admin.listLedgers();
    for(Long lId : ledgerIds){
        LOG.info("LOG:"+lId);
//        txnManager.txnLogManager.getBkc().deleteLedger(lId);
    }

    txnManager.close();

  }


}
