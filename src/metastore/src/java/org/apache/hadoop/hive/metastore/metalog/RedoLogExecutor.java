package org.apache.hadoop.hive.metastore.metalog;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.metalog.MetaLogManager.Operation;
import org.apache.hadoop.hive.metastore.metalog.MetaTxn.TxnLogType;
import org.apache.hadoop.hive.metastore.metalog.Transaction.TxnStatus;

public class RedoLogExecutor {

  private static Log LOG = LogFactory.getLog(RedoLogExecutor.class);

  private transient List<MetaLogEntry> logEntries;

  private final Long delay_sec = 1800L;

  MetaLogManager metaLogManager = null;

  private final  Thread executor = new Thread() {
      @Override
       public void run() {
        while(true){
        LOG.info(RedoLogExecutor.class + " – run task …");

        try {
          replayLog();
        } catch (Exception e) {
          LOG.error(e,e);
        }
       try {
          Thread.sleep(delay_sec * 1000);
        } catch (InterruptedException e) {
        }
       LOG.info(RedoLogExecutor.class + " – task end.");
       }

      }

   };

  public RedoLogExecutor(MetaLogManager metaLogManager){
    this.metaLogManager = metaLogManager;
    executor.setDaemon(true);
    executor.start();
  }

  public List<MetaLogEntry> getLogEntries() {
    return logEntries;
  }

  public void setLogEntries(List<MetaLogEntry> logEntries) {
    this.logEntries = logEntries;
  }

  public boolean replayLog() throws Exception{
    LinkedHashMap<Long, MetaTxn> redoTxns = new LinkedHashMap<Long, MetaTxn>();
    LinkedHashMap<Long, List<MetaTxn>> logToTxns = new LinkedHashMap<Long,  List<MetaTxn>>();

    LOG.info("##########Replaying log start##########");
    for(LedgerHandle logHandle : metaLogManager.getLogMap().values()){
      long num = logHandle.getLastAddConfirmed();
      LOG.info("##--Replaying log"+logHandle.getId()+"--##");
      LOG.info("##"+num+"=="+logHandle.getLastAddConfirmed()+"--"+logHandle.getLastAddPushed());
      Enumeration<LedgerEntry> it = logHandle.readEntries(0, num);

      List<MetaTxn> txnsOfOneLog = new ArrayList<MetaTxn>();
      while(it.hasMoreElements()){

        MetaTxn txn = new MetaTxn();
        MetaLogEntry record = new MetaLogEntry(0);

        LedgerEntry recordEntry  = it.nextElement();
        record.fromByteArray(recordEntry.getEntry());

        txn.setTxnId(record.getTxnid());
        if(record.getTxnLogType() == TxnLogType.start){
          txn.setDbs(record.getDbs());
        }
        txn.setLogStatus(record.getTxnLogType());

        txnsOfOneLog.add(txn);
        redoTxns.put(txn.getTxnId(), txn);

        LOG.info("record:"+record.toString());
      }
      logToTxns.put(logHandle.getId(), txnsOfOneLog);
    }


    for(MetaTxn txn :redoTxns.values()){
      redoOperation(txn);
    }

    for(Long logid : logToTxns.keySet()){
      if(metaLogManager.getActive_log_id() == logid){
        continue;
      }
      boolean isAllFinished = true;
      for(MetaTxn txn : logToTxns.get(logid)){
        if(txn.getTxnStatus() == TxnStatus.exefa
            ||txn.getTxnStatus() == TxnStatus.prexe
            ){
          isAllFinished = false;
        }
      }

      if(isAllFinished){
        LOG.info("---logFile"+logid+" replay fnish,delete...");
        metaLogManager.deleteLogFile(logid);
      }
    }
    LOG.info("##########Replaying log done ##########");
    return true;
  }

  public void redoOperation(MetaTxn txn) {
    if(txn.getLogStatus() == TxnLogType.start
        ||txn.getLogStatus() == TxnLogType.abort
        ||txn.getLogStatus() == TxnLogType.chkpt
        ||txn.getLogStatus() == TxnLogType.comit
        ) {
      return;
    }
    int op = -1;
    List<Object> params = null;
    LinkedHashSet<String> needRedoDbs = new LinkedHashSet<String>();
    needRedoDbs.addAll(txn.getDbs());
    boolean all_success = true;
    Long curr_pos = -1L;
    for(MetaLogEntry logEntry : this.logEntries){
      curr_pos++;

      if(logEntry.getTxnLogType() == TxnLogType.exeok){
        needRedoDbs.removeAll(logEntry.getDbs());
      }
      if(logEntry.getTxnLogType() == TxnLogType.prexe){
        op = logEntry.getOperation();
        params = logEntry.getParams();
      }

      for(String db : needRedoDbs){
        ArrayList<String> redoDB = new ArrayList<String>();
        redoDB.add(db);
        boolean succ = false;
        String failedMsg = "redo failed.";
        try {
          succ = redo(db,op, params);
        } catch (Exception e1) {
          failedMsg = e1.getMessage();
        }

        try {
          if(succ){
            txn.executeTxnOK(redoDB);
            txn.commitTxn();
          }else{
            all_success = false;
            txn.executeTxnFailed(redoDB, failedMsg);
          }
        } catch (MetaLogException e) {
          LOG.error(e.getMessage());
        }
      }
    }

    if(all_success){
      try {
        txn.getLog().manager.txnLogManager.getMetaData().setChkptPos(txn.chpt());
      } catch (MetaLogException e) {
        LOG.error(e.getMessage());
      }
    }

  }


  /**
   * redo all rpc here
   * @param le
   * @return
   * @throws Exception
   */
  public boolean redo(String db,int op,List<Object> params) throws Exception {
//    LedgerHandle handle = null;
    try {
      IMetaStoreClient client = MetaStoreClientManager.getDbCliMap().get(db);
      switch (op) {
      case Operation.CRT_DATABASE:
        client.createDatabase((Database)params.get(0));
        break;
      case Operation.MOD_DATABASE:
        break;
      default:
        return false;

      }
    } catch (Exception e) {
      LOG.error(e, e);
      throw e;
    }

    return true;
  }


}
