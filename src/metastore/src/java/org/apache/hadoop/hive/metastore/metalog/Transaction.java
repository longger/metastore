package org.apache.hadoop.hive.metastore.metalog;

import java.util.List;


public interface Transaction {
  public boolean commitTxn() throws MetaLogException;
  public boolean rollbackTxn()throws MetaLogException;
  public Long getTxnId() throws MetaLogException;
  public TxnStatus getTxnStatus() throws MetaLogException;
  public boolean preExecuteTxn(MetaLogEntry entry) throws MetaLogException;
  public boolean executeTxnOK(List<String> dbs) throws MetaLogException;
  public boolean executeTxnFailed(List<String> dbs,String comment) throws MetaLogException;
  public Long chpt() throws MetaLogException;

  public List<String> getDbs();
  public void setDbs(List<String> dbs);
  public Long setTxnId(Long txnId);


  public static enum TxnStatus{
    start(0),commit(1),prexe(2),exeok(3),exefa(4),abort(5),chkpt(6);
    int status = 0 ;
    private TxnStatus(int status) {
      this.status = status;
    }
  }




}
