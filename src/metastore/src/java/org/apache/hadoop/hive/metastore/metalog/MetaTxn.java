package org.apache.hadoop.hive.metastore.metalog;

import java.util.Date;
import java.util.List;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MetaTxn implements Transaction {

  private static Log LOG = LogFactory.getLog(Transaction.class);
  Long txnId = 0L;
  TxnStatus txnStatus;

//  LedgerHandle log ;
  public TxnLog log;

  private List<String> dbs;

  private transient TxnLogType logStatus;

  public MetaTxn(){}

  public MetaTxn(Long txnId,TxnLog log) throws MetaLogException {
    this.txnId = txnId;
    txnStatus = TxnStatus.start;
    this.log = log;
    log.logStart(this);
  }

  @Override
  public boolean preExecuteTxn(MetaLogEntry entry) throws MetaLogException {
    log.logPreExe(this, entry);
    txnStatus = TxnStatus.prexe;
    return true;
  }

  @Override
  public boolean executeTxnOK(List<String> dbs) throws MetaLogException {
    log.logExeOK(this, dbs);
    txnStatus = TxnStatus.exeok;
    return true;
  }

  @Override
  public boolean executeTxnFailed(List<String> dbs,String comment) throws MetaLogException {
    log.logExeFa(this, dbs, comment);
    txnStatus = TxnStatus.exefa;
    return true;
  }

  @Override
  public boolean commitTxn() throws MetaLogException {
    log.logComit(this);
    txnStatus = TxnStatus.commit;
    return true;
  }

  @Override
  public Long chpt() throws MetaLogException {

    txnStatus = TxnStatus.chkpt;
    return log.logChkpt(this);
  }

  @Override
  public boolean rollbackTxn() throws MetaLogException {
    log.logAbort(this);
    txnStatus = TxnStatus.abort;
    return false;
  }

  @Override
  public Long getTxnId() {

    return txnId;
  }

  @Override
  public Long setTxnId(Long txnId) {

    return this.txnId = txnId;
  }

  @Override
  public TxnStatus getTxnStatus() {

    return txnStatus;
  }


  @Override
  public List<String> getDbs() {
    return dbs;
  }
  @Override
  public void setDbs(List<String> dbs) {
    this.dbs = dbs;
  }

  public TxnLog getLog() {
    return log;
  }

  public void setLog(TxnLog log) {
    this.log = log;
  }



  public static enum TxnLogType{
    start("$#start#$"),prexe("$#prexe#$"),exeok("$#exeok#$"),exefa("$#exefa#$"),comit("$#comit#$"),abort("$#abort#$"),chkpt("$#chkpt#$");
    String type;
    private TxnLogType(String type){
      this.type = type;
    }

  }

  public static class TxnLog{
    private static Log LOG = LogFactory.getLog(TxnLog.class);

    LedgerHandle log ;

    MetaTxnManager manager = null;

    public TxnLog(LedgerHandle log ,MetaTxnManager manager){
      this.log = log;
      this.manager = manager;
    }

//    public static final String start  = "$#start#$";
//    public static final String comit  = "$#comit#$";
//    public static final String abort  = "$#abort#$";
//    public static final String chkpt  = "$#chkpt#$";

    public void logStart(Transaction txn) throws MetaLogException {
      MetaLogEntry entry = new MetaLogEntry(txn.getTxnId());
      entry.setTxnLogType(TxnLogType.start);
      try {
        log.addEntry(entry.toByteArray());
      } catch (Exception e) {
        LOG.error(e,e);
        throw new  MetaLogException(e.getMessage());
      }
    }

    public void logComit(Transaction txn) throws MetaLogException{
      try{
        MetaLogEntry entry = new MetaLogEntry(txn.getTxnId());
        entry.setTxnLogType(TxnLogType.comit);
        log.addEntry(entry.toByteArray());
      }catch(Exception e){
        LOG.error(e,e);
        throw new MetaLogException(e.getMessage());
      }
    }

    public void logPreExe(Transaction txn,MetaLogEntry entry) throws MetaLogException{
      try{
        MetaLogEntry newEntry = new MetaLogEntry(txn.getTxnId());
        newEntry.setTxnid(entry.getTxnid());
        newEntry.setDbs(entry.getDbs());
        newEntry.setOperation(entry.getOperation());
        newEntry.setParams(entry.getParams());
        newEntry.setTxnLogType(TxnLogType.prexe);
        log.addEntry(newEntry.toByteArray());
      }catch(Exception e){
        LOG.error(e,e);
        throw new MetaLogException(e.getMessage());
      }
    }

    public void logExeOK(Transaction txn,List<String> dbs) throws MetaLogException{
      try{
        MetaLogEntry newEntry = new MetaLogEntry(txn.getTxnId());
        newEntry.setDbs(dbs);
        newEntry.setTxnLogType(TxnLogType.exeok);
        log.addEntry(newEntry.toByteArray());
      }catch(Exception e){
        LOG.error(e,e);
        throw new MetaLogException(e.getMessage());
      }
    }

    public void logExeFa(Transaction txn,List<String> dbs,String comment) throws MetaLogException{
      try{
        MetaLogEntry newEntry = new MetaLogEntry(txn.getTxnId());
        newEntry.setDbs(dbs);
        newEntry.setTxnLogType(TxnLogType.exefa);
        newEntry.setComment(comment);
        log.addEntry(newEntry.toByteArray());
      }catch(Exception e){
        LOG.error(e,e);
        throw new MetaLogException(e.getMessage());
      }
    }

    public void logAbort(Transaction txn) throws MetaLogException{
      try{
        MetaLogEntry entry = new MetaLogEntry(txn.getTxnId());
        entry.setTxnLogType(TxnLogType.abort);
        log.addEntry(entry.toByteArray());
      }catch(Exception e){
        LOG.error(e,e);
        throw new MetaLogException(e.getMessage());
      }
    }

    public Long logChkpt(Transaction txn) throws MetaLogException{
      try{
        MetaLogEntry entry = new MetaLogEntry(txn.getTxnId());
        entry.setTxnLogType(TxnLogType.chkpt);
        log.addEntry(entry.toByteArray());
        return log.getLastAddConfirmed();
      }catch(Exception e){
        LOG.error(e,e);
        throw new MetaLogException(e.getMessage());
      }
    }
  }

  public TxnLogType getLogStatus() {
    return logStatus;
  }

  public void setLogStatus(TxnLogType logStatus) {
    this.logStatus = logStatus;
  }

  public void setTxnStatus(TxnStatus txnStatus) {
    this.txnStatus = txnStatus;
  }

  public static void main(String args[]){
    Date date = new Date();
    int[] ds = new int[]{1382906716,
        1382907361,
        1382906778,
        1382908409,
        1382904892};
    for(int x:ds){
    date.setTime(x*1000L);
    System.out.println(date);
    }

  }
 }
