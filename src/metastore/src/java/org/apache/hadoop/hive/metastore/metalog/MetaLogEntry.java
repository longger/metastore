package org.apache.hadoop.hive.metastore.metalog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.metalog.MetaLogManager.Operation;
import org.apache.hadoop.hive.metastore.metalog.MetaTxn.TxnLogType;
import org.apache.hadoop.hive.metastore.zk.MetaConstants;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.mortbay.log.Log;



public class MetaLogEntry {

  private LedgerEntry entry;

  private long txnid;

  private MetaTxn.TxnLogType txnLogType;

  private int operation;

  private List<Object> params;

  private List<String> dbs;

  private String comment = "";


  MetaLogEntry(long txnid) {

    txnLogType = MetaTxn.TxnLogType.prexe;

    operation = Operation.NULL_OPERATION;

    params = null;
  }

  MetaLogEntry(long txnid,int operation,List<Object> params) {

    this(txnid,operation,MetaTxn.TxnLogType.prexe,params);
  }

  MetaLogEntry(long txnid,int operation,MetaTxn.TxnLogType type,List<Object> params) {

    this.txnid = txnid;

    this.txnLogType = type;

    this.operation = operation;

    this.params = params;
  }



  public LedgerEntry getEntry() {
    return entry;
  }



  public void setEntry(LedgerEntry entry) {
    this.entry = entry;
  }



  public long getTxnid() {
    return txnid;
  }



  public void setTxnid(long txnid) {
    this.txnid = txnid;
  }



  public int getOperation() {
    return operation;
  }



  public void setOperation(int operation) {
    this.operation = operation;
  }



  public List<Object> getParams() {
    return params;
  }



  public void setParams(List<Object> parms) {
    this.params = parms;
  }


  public MetaTxn.TxnLogType getTxnLogType() {
    return txnLogType;
  }

  public void setTxnLogType(MetaTxn.TxnLogType txnLogType) {
    this.txnLogType = txnLogType;
  }

  /**
   *
   * @return
   * @throws Exception
   */

  public byte[] toByteArray() throws Exception {
    DataOutputBuffer ouput = new DataOutputBuffer();
    ouput.writeLong(this.txnid);
    WritableUtils.writeString(ouput, this.txnLogType.toString());
    if(txnLogType == TxnLogType.start|| txnLogType == TxnLogType.prexe){
      if(this.dbs == null || this.dbs.isEmpty()){
      ouput.writeInt( 0);
      }else{
        ouput.writeInt(dbs.size());
        for(String db : dbs){
          WritableUtils.writeString(ouput,db);
        }
      }
    }
    ouput.writeInt(this.operation);
    if(this.params == null || this.params.isEmpty()){
      ouput.writeInt( 0);
    }else{
      ouput.writeInt( params.size());
      for(Object param : params){
        WritableUtils.writeString(ouput,getObjectName(param));
        if(param instanceof TBase){
          WritableUtils.writeCompressedByteArray(ouput,serialize((TBase)(param)));
        }else if(param instanceof Long){
          ouput.writeLong((Long)(param));
        }else if(param instanceof Integer){
          ouput.writeInt((Integer)(param));
        }else if(param instanceof Float){
          ouput.writeFloat((Float)(param));
        }else if(param instanceof Short){
          ouput.writeShort((Short)(param));
        }else if(param instanceof String){
          WritableUtils.writeString(ouput,(String)(param));
        }else{
          throw new Exception("Not supported serialiaztion class");
        }
      }
    }
    WritableUtils.writeString(ouput,comment);
    return ouput.getData();

  }

  public void fromByteArray(byte[] bs) throws Exception{
    DataInputBuffer input = new DataInputBuffer();
    input.reset(bs, bs.length);
//    ByteSequence input = new ByteSequence(bs);
    this.txnid = input.readLong();
    this.txnLogType = TxnLogType.valueOf(WritableUtils.readString(input));
    if(txnLogType == TxnLogType.start || txnLogType == TxnLogType.prexe){
      int num = input.readInt();

      if(num > 0 ){
        if(dbs == null){
          dbs = new ArrayList<String>();
        }
        for(int i=0;i<num;i++){
          String dbString = WritableUtils.readString(input);
          this.dbs.add(dbString);
        }
      }
    }

    this.operation = input.readInt();
    int num = input.readInt();
    if(num > 0 ){
//      String dbString = WritableUtils.readString(input);
//      Long id = input.readLong();
//      this.params.add(dbString);
      params = new ArrayList<Object>();
      for(int i=0;i<num;i++){
        String paramclazzName = WritableUtils.readString(input);
        int beginIndex = Math.max(paramclazzName.lastIndexOf(".") ,0);
        String param = paramclazzName.substring(beginIndex);
        Log.info("paramclazzName:"+paramclazzName);
        if(paramclazzName.startsWith("org.apache.hadoop.hive.metastore.api")){
          byte[] bytes = WritableUtils.readCompressedByteArray(input);
          TBase classBase = getThriftObject(paramclazzName);
          params.add(deserialize(bytes,classBase));
        }else if(param.equals("Long")){
          params.add(input.readLong());
        }else if(param.equals("Integer")){
          params.add(input.readInt());
        }else if(param.equals("Float")){
          params.add(input.readFloat());
        }else if(param.equals("Short")){
          params.add(input.readShort());
        }else if(param.equals("String")){
          params.add(WritableUtils.readString(input));
        }else{
          throw new Exception("Not supported serialiaztion class");
        }
      }
    }
    comment = WritableUtils.readString(input);
  }


//  public static boolean bytesToThrift(final byte[] buff,
//      TBase ts) throws TException {
//    TMemoryBuffer buffer = new TMemoryBuffer(100);
//    buffer.write(buff);
//    TBinaryProtocol protocol = new TBinaryProtocol((TTransport)buffer);
//    ts.read(protocol);
//    return true;
//  }
//
//
//
//  public static byte[]  ThriftToString(final TBase ts) throws TException {
//    TMemoryBuffer buffer = new TMemoryBuffer(100);
//    TBinaryProtocol protocol = new org.apache.thrift.protocol.TBinaryProtocol((TTransport)buffer);
//    ts.write(protocol);
//    byte[] ss =protocol.getTransport().getBuffer();
////    byte[] ss = buffer.getArray();
//    int pos = buffer.getBufferPosition();
//    byte[] dest = new byte[pos+1];
//    System.arraycopy(ss, 0, dest, 0, pos+1);
//    return buffer.getBuffer();  // NOLINT
//  }
  public static TBase deserialize(byte[] array ,TBase content) throws Exception {
    ByteArrayInputStream bais = new ByteArrayInputStream(array);
    TIOStreamTransport trans = new TIOStreamTransport(bais);
    TBinaryProtocol oprot = new TBinaryProtocol(trans);
    content.read(oprot);
    return content;
  }

  public static TBase getThriftObject(String className) throws Exception{
    Class clazz = Class.forName(className);
    TBase obj = (TBase) clazz.getConstructor().newInstance();
    return obj;
  }

  public static String getObjectName(Object obj){
    String clazz  = obj.getClass().toString();
    if(clazz.startsWith("class ")){
      clazz = clazz.substring("class ".length());
    }
    Log.info("---"+clazz);
    return clazz;
  }

  public static byte[] serialize(TBase obj) throws Exception {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(2);
      TIOStreamTransport trans = new TIOStreamTransport(baos);
      TBinaryProtocol oprot = new TBinaryProtocol(trans);
      obj.write(oprot);
      byte[] array = baos.toByteArray();
//      int expectedSize = array.length;
      return array;
}


  public static void main(String args[]) throws Exception{
    Database db = new Database();
    db.setName("ha");
    db.setParameters(new HashMap<String, String>());
    db.getParameters().put("1", "1");
    byte[] buff = serialize(db);
    System.out.println(new String(buff));
    deserialize(buff,db);

    System.out.println(Long.MAX_VALUE);
  }

  public void setDbs(List<String> dbs) {
    this.dbs = dbs;
  }

  public List<String> getDbs() {
    return this.dbs;
  }

  public static String getTopDB(){
    return MetaConstants.DEFAULT_TOP_DATABASE;
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("txnid:"+txnid);
    sb.append(" operation:"+this.operation);
    sb.append(" db:"+this.dbs);
    if(params != null){
      sb.append(" params.size:"+this.params.size());
    }
    return sb.toString();
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

}
