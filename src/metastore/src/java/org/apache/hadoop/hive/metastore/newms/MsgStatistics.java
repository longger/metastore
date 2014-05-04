package org.apache.hadoop.hive.metastore.newms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.msg.MSGType;

public class MsgStatistics {
  private int createDbIndex;
  private int alterDbIndex;
  private int alterDbparamIndex;
  private int dropDbIndex;
  private int createTblIndex;
  private int alterTblNameIndex;
  private int alterTblDistriIndex;
  private int alterTblPartIndex;
  private int alterTblSplitIndex;
  private int alterTblDelcolIndex;
  private int alterTblAddcolIndex;
  private int alterTblcolNameIndex;
  private int alterTblcolTypeIndex;
  private int alterTblcolLenIndex;
  private int alterTblParamIndex;
  private int dropTblIndex;
  private int TblBusiChangeIndex;
  private int createPartIndex;
  private int alterPartIndex;
  private int delPartIndex;
  private int addPartFileIndex;
  private int alterPartFileIndex;
  private int repFileChangeIndex;
  private int staFileChangeIndex;
  private int repFileOnOffIndex;
  private int delPartFileIndex;
  private int fileUsrSetRepChanIndex;
  private int createIndIndex;
  private int alterIndIndex;
  private int alterIndParmIndex;
  private int delIndIndex;
  private int createPartIndIndex;
  private int alterPartIndIndex;
  private int delPartIndIndex;
  private int createPartIndFileIndex;
  private int alterPartIndFileIndex;
  private int delPartIndFileIndex;
  private int createNodeIndex;
  private int delNodeIndex;
  private int failNodeIndex;
  private int backNodeIndex;
  private int createSchemaIndex;
  private int modifyScheNameIndex;
  private int modifyScheDelcolIndex;
  private int modifyScheAddcolIndex;
  private int modifyScheAltcolNameIndex;
  private int modifyScheAltcolTypeIndex;
  private int modifySchemaParamIndex;
  private int delSchemaIndex;
  private int DDLDirectDw1Index;
  private int DDLDirectDw2Index;
  private int createNgIndex;
  private int modifyNgIndex;
  private int delNgIndex;
  private int alterNgIndex;
  private int createFileIndex;
  private int delFileIndex;
  private int grantGlobalIndex;
  private int grantDBIndex;
  private int grantTblIndex;
  private int grantSchemaIndex;
  private int grantPartIndex;
  private int grantPartCloIndex;
  private int grantTblColIndex;
  private int revokeGlobalIndex;
  private int revokeDBIndex;
  private int revokeTblIndex;
  private int revokeSchemaIndex;
  private int revokePartIndex;
  private int revokePartColIndex;
  private int revokeTblColIndex;
  
  private HashMap<Long, Long> fileCreTime = new HashMap<Long, Long>();
  private HashMap<Long, Long> fileCloTime = new HashMap<Long, Long>();
  private ConcurrentHashMap<Long, String> probMsg = new ConcurrentHashMap<Long, String>();
  

  public MsgStatistics() {
  }

  public void handleMsg(DDLMsg msg) {

    int eventid = (int) msg.getEvent_id();
    String message = msg.toJson();
    HashMap<String,Object> msgDate = msg.getMsg_data();
    switch (eventid) {
      case MSGType.MSG_NEW_DATABESE:
        createDbIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message);
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALTER_DATABESE:
        alterDbIndex++;
        break;
      case MSGType.MSG_ALTER_DATABESE_PARAM:
        alterDbparamIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else if(!msgDate.keySet().contains("param_name") || msgDate.get("param_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DROP_DATABESE:
        dropDbIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_NEW_TALBE:
        createTblIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_TALBE_NAME:
        alterTblNameIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("old_table_name") || msgDate.get("old_table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_TALBE_DISTRIBUTE:
        alterTblDistriIndex++;
        break;
      case MSGType.MSG_ALT_TALBE_PARTITIONING:
        alterTblPartIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("version") || msgDate.get("version")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_TABLE_SPLITKEYS:
        alterTblSplitIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("version") || msgDate.get("version")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_TALBE_DEL_COL:
        alterTblDelcolIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_type") || msgDate.get("column_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_TALBE_ADD_COL:
        alterTblAddcolIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_type") || msgDate.get("column_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_TALBE_ALT_COL_NAME:
        alterTblcolNameIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("old_column_name") || msgDate.get("old_column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_TALBE_ALT_COL_TYPE:
        alterTblcolTypeIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_type") || msgDate.get("column_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("old_column_type") || msgDate.get("old_column_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_TALBE_ALT_COL_LENGTH:
        alterTblcolLenIndex++;
        break;
      case MSGType.MSG_ALT_TABLE_PARAM:
        alterTblParamIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("tbl_param_keys") || msgDate.get("tbl_param_keys")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DROP_TABLE:
        dropTblIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_TABLE_BUSITYPE_CHANGED:
        TblBusiChangeIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        }
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("action") || msgDate.get("action")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("comment") || msgDate.get("comment")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_NEW_PARTITION:
        createPartIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_level") || msgDate.get("partition_level")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_PARTITION:
        alterPartIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_level") || msgDate.get("partition_level")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("old_partition_name") || msgDate.get("old_partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DEL_PARTITION:
        delPartIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_level") || msgDate.get("partition_level")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ADD_PARTITION_FILE:
        addPartFileIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_level") || msgDate.get("partition_level")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("f_id") || msgDate.get("f_id")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("parent_partition_name") || msgDate.get("parent_partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_PARTITION_FILE:
        alterPartFileIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_level") || msgDate.get("partition_level")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("f_id") || msgDate.get("f_id")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("parent_partition_name") || msgDate.get("parent_partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_REP_FILE_CHANGE:
        repFileChangeIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("devid") || msgDate.get("devid")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("location") || msgDate.get("location")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("f_id") || msgDate.get("f_id")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("op") || msgDate.get("op")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_STA_FILE_CHANGE:
        staFileChangeIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("f_id") || msgDate.get("f_id")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("new_status") || msgDate.get("new_status")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_REP_FILE_ONOFF:
        repFileOnOffIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("f_id") || msgDate.get("f_id")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("new_status") || msgDate.get("new_status")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DEL_PARTITION_FILE:
        delPartFileIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_level") || msgDate.get("partition_level")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("f_id") || msgDate.get("f_id")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("parent_partition_name") || msgDate.get("parent_partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_FILE_USER_SET_REP_CHANGE:
        fileUsrSetRepChanIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("f_id") || msgDate.get("f_id")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("new_repnr") || msgDate.get("new_repnr")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_NEW_INDEX:
        createIndIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("index_name") || msgDate.get("index_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_INDEX:
        alterIndIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("index_name") || msgDate.get("index_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_INDEX_PARAM:
        alterIndParmIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("index_name") || msgDate.get("index_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("param_name") || msgDate.get("param_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DEL_INDEX:
        delIndIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("index_name") || msgDate.get("index_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_NEW_PARTITION_INDEX:
        createPartIndIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("index_name") || msgDate.get("index_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALT_PARTITION_INDEX:
        alterPartIndIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("index_name") || msgDate.get("index_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DEL_PARTITION_INDEX:
        delPartIndIndex++;
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("index_name") || msgDate.get("index_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_NEW_PARTITION_INDEX_FILE:
        createPartIndFileIndex++;
        break;
      case MSGType.MSG_ALT_PARTITION_INDEX_FILE:
        alterPartIndFileIndex++;
        break;
      case MSGType.MSG_DEL_PARTITION_INDEX_FILE:
        delPartIndFileIndex++;
        break;
      case MSGType.MSG_NEW_NODE:
        createNodeIndex++;
        if(!msgDate.keySet().contains("node_name") || msgDate.get("node_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DEL_NODE:
        delNodeIndex++;
        if(!msgDate.keySet().contains("node_name") || msgDate.get("node_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        }
        break;
      case MSGType.MSG_FAIL_NODE:
        failNodeIndex++;
        if(!msgDate.keySet().contains("node_name") || msgDate.get("node_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("status") || msgDate.get("status")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_BACK_NODE:
        backNodeIndex++;
        if(!msgDate.keySet().contains("node_name") || msgDate.get("node_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("status") || msgDate.get("status")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_CREATE_SCHEMA:
        createSchemaIndex++;
        if(!msgDate.keySet().contains("schema_name") || msgDate.get("schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        }
        break;
      case MSGType.MSG_MODIFY_SCHEMA_NAME:
        modifyScheNameIndex++;
        if(!msgDate.keySet().contains("schema_name") || msgDate.get("schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("old_schema_name") || msgDate.get("old_schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_MODIFY_SCHEMA_DEL_COL:
        modifyScheDelcolIndex++;
        if(!msgDate.keySet().contains("schema_name") || msgDate.get("schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_MODIFY_SCHEMA_ADD_COL:
        modifyScheAddcolIndex++;
        if(!msgDate.keySet().contains("schema_name") || msgDate.get("schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_MODIFY_SCHEMA_ALT_COL_NAME:
        modifyScheAltcolNameIndex++;
        if(!msgDate.keySet().contains("schema_name") || msgDate.get("schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("old_column_name") || msgDate.get("old_column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_MODIFY_SCHEMA_ALT_COL_TYPE:
        modifyScheAltcolTypeIndex++;
        if(!msgDate.keySet().contains("schema_name") || msgDate.get("schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_type") || msgDate.get("column_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("old_column_type") || msgDate.get("old_column_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_MODIFY_SCHEMA_PARAM:
        modifySchemaParamIndex++;
        if(!msgDate.keySet().contains("schema_name") || msgDate.get("schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("tbl_param_keys") || msgDate.get("tbl_param_keys")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DEL_SCHEMA:
        delSchemaIndex++;
        if(!msgDate.keySet().contains("schema_name") || msgDate.get("schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DDL_DIRECT_DW1:
        DDLDirectDw1Index++;
        if(!msgDate.keySet().contains("sql") || msgDate.get("sql")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DDL_DIRECT_DW2:
        DDLDirectDw2Index++;
        if(!msgDate.keySet().contains("sql") || msgDate.get("sql")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_NEW_NODEGROUP:
        createNgIndex++;
        if(!msgDate.keySet().contains("nodegroup_name") || msgDate.get("nodegroup_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_MODIFY_NODEGROUP:
        modifyNgIndex++;
        if(!msgDate.keySet().contains("nodegroup_name") || msgDate.get("nodegroup_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("old_nodegroup_name") || msgDate.get("old_nodegroup_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DEL_NODEGROUP:
        delNgIndex++;
        if(!msgDate.keySet().contains("nodegroup_name") || msgDate.get("nodegroup_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_ALTER_NODEGROUP:
        alterNgIndex++;
        if(!msgDate.keySet().contains("nodegroup_name") || msgDate.get("nodegroup_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("nodes") || msgDate.get("nodes")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_CREATE_FILE:
        createFileIndex++;
        Long creTime = (Long)msg.getEvent_time();
        Long fid = Long.parseLong(msg.getMsg_data().get("f_id").toString());
        fileCreTime.put(fid, creTime);
        if(!msgDate.keySet().contains("f_id") || msgDate.get("f_id")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_DEL_FILE:
        delFileIndex++;
        Long delTime = (Long)msg.getEvent_time();
        Long f_id = Long.parseLong(msg.getMsg_data().get("f_id").toString());
        fileCloTime.put(f_id, delTime);
        if(!msgDate.keySet().contains("f_id") || msgDate.get("f_id")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_GRANT_GLOBAL:
        grantGlobalIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_GRANT_DB:
        grantDBIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_GRANT_TABLE:
        grantTblIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_GRANT_SCHEMA:
        grantSchemaIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("schema_name") || msgDate.get("schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_GRANT_PARTITION:
        grantPartIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_GRANT_PARTITION_COLUMN:
        grantPartCloIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_GRANT_TABLE_COLUMN:
        grantTblColIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_REVOKE_GLOBAL:
        revokeGlobalIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_REVOKE_DB:
        revokeDBIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_REVOKE_TABLE:
        revokeTblIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_REVOKE_SCHEMA:
        revokeSchemaIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("schema_name") || msgDate.get("schema_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_REVOKE_PARTITION:
        revokePartIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_REVOKE_PARTITION_COLUMN:
        revokePartColIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("partition_name") || msgDate.get("partition_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      case MSGType.MSG_REVOKE_TABLE_COLUMN:
        revokeTblColIndex++;
        if(!msgDate.keySet().contains("grantor") || msgDate.get("grantor")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grantor_type") || msgDate.get("grantor_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_name") || msgDate.get("principal_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("principal_type") || msgDate.get("principal_type")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("privilege") || msgDate.get("privilege")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("create_time") || msgDate.get("create_time")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("grant_option") || msgDate.get("grant_option")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("db_name") || msgDate.get("db_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("table_name") || msgDate.get("table_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else
        if(!msgDate.keySet().contains("column_name") || msgDate.get("column_name")==null){
          probMsg.put(msg.getEvent_id(), message );
        } else{
          probMsg.remove(msg.getEvent_id());
        }
        break;
      default:
      {
        System.out.println("unhandled msg from metaq: "+msg.getEvent_id());
        break;
      }
    }
  }

  public long avgFileCreDelTime(){
    List<Long> avgTime = new ArrayList<Long>(); 
    Long avgFileTime = 0L;
    try {
      for(Long fid : fileCreTime.keySet()){
        Long creTime = fileCreTime.get(fid);
        Long delTime = fileCloTime.get(fid);
        if(delTime == null){
          continue;
        }else{
          Long fileTime = delTime-creTime;
          avgTime.add(fileTime);
        }
      }
      Long sumTime = 0L;
      for(Long time : avgTime){
        sumTime+=time;
      }
      if(avgTime.size()!=0){
//        System.out.println("AvgListSize" + avgTime.size());
        avgFileTime = sumTime/avgTime.size();
      }else{
        throw new Exception("There is no file created yet.");
      }
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    return avgFileTime;
    
 }
  
  
  public int getCreateDbIndex() {
    return createDbIndex;
  }

  public void setCreateDbIndex(int createDbIndex) {
    this.createDbIndex = createDbIndex;
  }

  public int getAlterDbIndex() {
    return alterDbIndex;
  }

  public void setAlterDbIndex(int alterDbIndex) {
    this.alterDbIndex = alterDbIndex;
  }

  public int getAlterDbparamIndex() {
    return alterDbparamIndex;
  }

  public void setAlterDbparamIndex(int alterDbparamIndex) {
    this.alterDbparamIndex = alterDbparamIndex;
  }

  public int getDropDbIndex() {
    return dropDbIndex;
  }

  public void setDropDbIndex(int dropDbIndex) {
    this.dropDbIndex = dropDbIndex;
  }

  public int getCreateTblIndex() {
    return createTblIndex;
  }

  public void setCreateTblIndex(int createTblIndex) {
    this.createTblIndex = createTblIndex;
  }

  public int getAlterTblNameIndex() {
    return alterTblNameIndex;
  }

  public void setAlterTblNameIndex(int alterTblNameIndex) {
    this.alterTblNameIndex = alterTblNameIndex;
  }

  public int getAlterTblDistriIndex() {
    return alterTblDistriIndex;
  }

  public void setAlterTblDistriIndex(int alterTblDistriIndex) {
    this.alterTblDistriIndex = alterTblDistriIndex;
  }

  public int getAlterTblPartIndex() {
    return alterTblPartIndex;
  }

  public void setAlterTblPartIndex(int alterTblPartIndex) {
    this.alterTblPartIndex = alterTblPartIndex;
  }

  public int getAlterTblSplitIndex() {
    return alterTblSplitIndex;
  }

  public void setAlterTblSplitIndex(int alterTblSplitIndex) {
    this.alterTblSplitIndex = alterTblSplitIndex;
  }

  public int getAlterTblDelcolIndex() {
    return alterTblDelcolIndex;
  }

  public void setAlterTblDelcolIndex(int alterTblDelcolIndex) {
    this.alterTblDelcolIndex = alterTblDelcolIndex;
  }

  public int getAlterTblAddcolIndex() {
    return alterTblAddcolIndex;
  }

  public void setAlterTblAddcolIndex(int alterTblAddcolIndex) {
    this.alterTblAddcolIndex = alterTblAddcolIndex;
  }

  public int getAlterTblcolNameIndex() {
    return alterTblcolNameIndex;
  }

  public void setAlterTblcolNameIndex(int alterTblcolNameIndex) {
    this.alterTblcolNameIndex = alterTblcolNameIndex;
  }

  public int getAlterTblcolTypeIndex() {
    return alterTblcolTypeIndex;
  }

  public void setAlterTblcolTypeIndex(int alterTblcolTypeIndex) {
    this.alterTblcolTypeIndex = alterTblcolTypeIndex;
  }

  public int getAlterTblcolLenIndex() {
    return alterTblcolLenIndex;
  }

  public void setAlterTblcolLenIndex(int alterTblcolLenIndex) {
    this.alterTblcolLenIndex = alterTblcolLenIndex;
  }

  public int getAlterTblParamIndex() {
    return alterTblParamIndex;
  }

  public void setAlterTblParamIndex(int alterTblParamIndex) {
    this.alterTblParamIndex = alterTblParamIndex;
  }

  public int getDropTblIndex() {
    return dropTblIndex;
  }

  public void setDorpTblIndex(int dorpTblIndex) {
    this.dropTblIndex = dorpTblIndex;
  }

  public int getTblBusiChangeIndex() {
    return TblBusiChangeIndex;
  }

  public void setTblBusiChangeIndex(int tblBusiChangeIndex) {
    TblBusiChangeIndex = tblBusiChangeIndex;
  }

  public int getCreatePartIndex() {
    return createPartIndex;
  }

  public void setCreatePartIndex(int createPartIndex) {
    this.createPartIndex = createPartIndex;
  }

  public int getAlterPartIndex() {
    return alterPartIndex;
  }

  public void setAlterPartIndex(int alterPartIndex) {
    this.alterPartIndex = alterPartIndex;
  }

  public int getDelPartIndex() {
    return delPartIndex;
  }

  public void setDelPartIndex(int delPartIndex) {
    this.delPartIndex = delPartIndex;
  }

  public int getAddPartFileIndex() {
    return addPartFileIndex;
  }

  public void setAddPartFileIndex(int addPartFileIndex) {
    this.addPartFileIndex = addPartFileIndex;
  }

  public int getAlterPartFileIndex() {
    return alterPartFileIndex;
  }

  public void setAlterPartFileIndex(int alterPartFileIndex) {
    this.alterPartFileIndex = alterPartFileIndex;
  }

  public int getRepFileChangeIndex() {
    return repFileChangeIndex;
  }

  public void setRepFileChangeIndex(int repFileChangeIndex) {
    this.repFileChangeIndex = repFileChangeIndex;
  }

  public int getStaFileChanfeIndex() {
    return staFileChangeIndex;
  }

  public void setStaFileChanfeIndex(int staFileChanfeIndex) {
    this.staFileChangeIndex = staFileChanfeIndex;
  }

  public int getRepFileOnOffIndex() {
    return repFileOnOffIndex;
  }

  public void setRepFileOnOffIndex(int repFileOnOffIndex) {
    this.repFileOnOffIndex = repFileOnOffIndex;
  }

  public int getDelPartFileIndex() {
    return delPartFileIndex;
  }

  public void setDelPartFileIndex(int delPartFileIndex) {
    this.delPartFileIndex = delPartFileIndex;
  }

  public int getFileUsrSetRepChanIndex() {
    return fileUsrSetRepChanIndex;
  }

  public void setFileUsrSetRepChanIndex(int fileUsrSetRepChanIndex) {
    this.fileUsrSetRepChanIndex = fileUsrSetRepChanIndex;
  }

  public int getCreateIndIndex() {
    return createIndIndex;
  }

  public void setCreateIndIndex(int createIndIndex) {
    this.createIndIndex = createIndIndex;
  }

  public int getAlterIndIndex() {
    return alterIndIndex;
  }

  public void setAlterIndIndex(int alterIndIndex) {
    this.alterIndIndex = alterIndIndex;
  }

  public int getAlterIndParmIndex() {
    return alterIndParmIndex;
  }

  public void setAlterIndParmIndex(int alterIndParmIndex) {
    this.alterIndParmIndex = alterIndParmIndex;
  }

  public int getDelIndIndex() {
    return delIndIndex;
  }

  public void setDelIndIndex(int delIndIndex) {
    this.delIndIndex = delIndIndex;
  }

  public int getCreatePartIndIndex() {
    return createPartIndIndex;
  }

  public void setCreatePartIndIndex(int createPartIndIndex) {
    this.createPartIndIndex = createPartIndIndex;
  }

  public int getAlterPartIndIndex() {
    return alterPartIndIndex;
  }

  public void setAlterPartIndIndex(int alterPartIndIndex) {
    this.alterPartIndIndex = alterPartIndIndex;
  }

  public int getDelPartIndIndex() {
    return delPartIndIndex;
  }

  public void setDelPartIndIndex(int delPartIndIndex) {
    this.delPartIndIndex = delPartIndIndex;
  }

  public int getCreatePartIndFileIndex() {
    return createPartIndFileIndex;
  }

  public void setCreatePartIndFileIndex(int createPartIndFileIndex) {
    this.createPartIndFileIndex = createPartIndFileIndex;
  }

  public int getAlterPartIndFileIndex() {
    return alterPartIndFileIndex;
  }

  public void setAlterPartIndFileIndex(int alterPartIndFileIndex) {
    this.alterPartIndFileIndex = alterPartIndFileIndex;
  }

  public int getDelPartIndFileIndex() {
    return delPartIndFileIndex;
  }

  public void setDelPartIndFileIndex(int delPartIndFileIndex) {
    this.delPartIndFileIndex = delPartIndFileIndex;
  }

  public int getCreateNodeIndex() {
    return createNodeIndex;
  }

  public void setCreateNodeIndex(int createNodeIndex) {
    this.createNodeIndex = createNodeIndex;
  }

  public int getDelNodeIndex() {
    return delNodeIndex;
  }

  public void setDelNodeIndex(int delNodeIndex) {
    this.delNodeIndex = delNodeIndex;
  }

  public int getFailNodeIndex() {
    return failNodeIndex;
  }

  public void setFailNodeIndex(int failNodeIndex) {
    this.failNodeIndex = failNodeIndex;
  }

  public int getBackNodeIndex() {
    return backNodeIndex;
  }

  public void setBackNodeIndex(int backNodeIndex) {
    this.backNodeIndex = backNodeIndex;
  }

  public int getCreateSchemaIndex() {
    return createSchemaIndex;
  }

  public void setCreateSchemaIndex(int createSchemaIndex) {
    this.createSchemaIndex = createSchemaIndex;
  }

  public int getModifyScheNameIndex() {
    return modifyScheNameIndex;
  }

  public void setModifyScheNameIndex(int modifyScheNameIndex) {
    this.modifyScheNameIndex = modifyScheNameIndex;
  }

  public int getModifyScheDelcolIndex() {
    return modifyScheDelcolIndex;
  }

  public void setModifyScheDelcolIndex(int modifyScheDelcolIndex) {
    this.modifyScheDelcolIndex = modifyScheDelcolIndex;
  }

  public int getModifyScheAddcolIndex() {
    return modifyScheAddcolIndex;
  }

  public void setModifyScheAddcolIndex(int modifyScheAddcolIndex) {
    this.modifyScheAddcolIndex = modifyScheAddcolIndex;
  }

  public int getModifyScheAltcolNameIndex() {
    return modifyScheAltcolNameIndex;
  }

  public void setModifyScheAltcolNameIndex(int modifyScheAltcolNameIndex) {
    this.modifyScheAltcolNameIndex = modifyScheAltcolNameIndex;
  }

  public int getModifyScheAltcolTypeIndex() {
    return modifyScheAltcolTypeIndex;
  }

  public void setModifyScheAltcolTypeIndex(int modifyScheAltcolTypeIndex) {
    this.modifyScheAltcolTypeIndex = modifyScheAltcolTypeIndex;
  }

  public int getModifySchemaParamIndex() {
    return modifySchemaParamIndex;
  }

  public void setModifySchemaParamIndex(int modifySchemaParamIndex) {
    this.modifySchemaParamIndex = modifySchemaParamIndex;
  }

  public int getDelSchemaIndex() {
    return delSchemaIndex;
  }

  public void setDelSchemaIndex(int delSchemaIndex) {
    this.delSchemaIndex = delSchemaIndex;
  }

  public int getDDLDirectDw1Index() {
    return DDLDirectDw1Index;
  }

  public void setDDLDirectDw1Index(int dDLDirectDw1Index) {
    DDLDirectDw1Index = dDLDirectDw1Index;
  }

  public int getDDLDirectDw2Index() {
    return DDLDirectDw2Index;
  }

  public void setDDLDirectDw2Index(int dDLDirectDw2Index) {
    DDLDirectDw2Index = dDLDirectDw2Index;
  }

  public int getCreateNgIndex() {
    return createNgIndex;
  }

  public void setCreateNgIndex(int createNgIndex) {
    this.createNgIndex = createNgIndex;
  }

  public int getModifyNgIndex() {
    return modifyNgIndex;
  }

  public void setModifyNgIndex(int modifyNgIndex) {
    this.modifyNgIndex = modifyNgIndex;
  }

  public int getDelNgIndex() {
    return delNgIndex;
  }

  public void setDelNgIndex(int delNgIndex) {
    this.delNgIndex = delNgIndex;
  }

  public int getAlterNgIndex() {
    return alterNgIndex;
  }

  public void setAlterNgIndex(int alterNgIndex) {
    this.alterNgIndex = alterNgIndex;
  }

  public int getCreateFileIndex() {
    return createFileIndex;
  }

  public void setCreateFileIndex(int createFileIndex) {
    this.createFileIndex = createFileIndex;
  }

  public int getDelFileIndex() {
    return delFileIndex;
  }

  public void setDelFileIndex(int delFileIndex) {
    this.delFileIndex = delFileIndex;
  }

  public int getGrantGlobalIndex() {
    return grantGlobalIndex;
  }

  public void setGrantGlobalIndex(int grantGlobalIndex) {
    this.grantGlobalIndex = grantGlobalIndex;
  }

  public int getGrantDBIndex() {
    return grantDBIndex;
  }

  public void setGrantDBIndex(int grantDBIndex) {
    this.grantDBIndex = grantDBIndex;
  }

  public int getGrantTblIndex() {
    return grantTblIndex;
  }

  public void setGrantTblIndex(int grantTblIndex) {
    this.grantTblIndex = grantTblIndex;
  }

  public int getGrantSchemaIndex() {
    return grantSchemaIndex;
  }

  public void setGrantSchemaIndex(int grantSchemaIndex) {
    this.grantSchemaIndex = grantSchemaIndex;
  }

  public int getGrantPartIndex() {
    return grantPartIndex;
  }

  public void setGrantPartIndex(int grantPartIndex) {
    this.grantPartIndex = grantPartIndex;
  }

  public int getGrantPartCloIndex() {
    return grantPartCloIndex;
  }

  public void setGrantPartCloIndex(int grantPartCloIndex) {
    this.grantPartCloIndex = grantPartCloIndex;
  }

  public int getGrantTblColIndex() {
    return grantTblColIndex;
  }

  public void setGrantTblColIndex(int grantTblColIndex) {
    this.grantTblColIndex = grantTblColIndex;
  }

  public int getRevokeGlobalIndex() {
    return revokeGlobalIndex;
  }

  public void setRevokeGlobalIndex(int revokeGlobalIndex) {
    this.revokeGlobalIndex = revokeGlobalIndex;
  }

  public int getRevokeDBIndex() {
    return revokeDBIndex;
  }

  public void setRevokeDBIndex(int revokeDBIndex) {
    this.revokeDBIndex = revokeDBIndex;
  }

  public int getRevokeTblIndex() {
    return revokeTblIndex;
  }

  public void setRevokeTblIndex(int revokeTblIndex) {
    this.revokeTblIndex = revokeTblIndex;
  }

  public int getRevokeSchemaIndex() {
    return revokeSchemaIndex;
  }

  public void setRevokeSchemaIndex(int revokeSchemaIndex) {
    this.revokeSchemaIndex = revokeSchemaIndex;
  }

  public int getRevokePartIndex() {
    return revokePartIndex;
  }

  public void setRevokePartIndex(int revokePartIndex) {
    this.revokePartIndex = revokePartIndex;
  }

  public int getRevokePartColIndex() {
    return revokePartColIndex;
  }

  public void setRevokePartColIndex(int revokePartColIndex) {
    this.revokePartColIndex = revokePartColIndex;
  }

  public int getRevokeTblColIndex() {
    return revokeTblColIndex;
  }

  public void setRevokeTblColIndex(int revokeTblColIndex) {
    this.revokeTblColIndex = revokeTblColIndex;
  }

  public HashMap<Long, Long> getFileCreTime() {
    return fileCreTime;
  }

  public void setFileCreTime(HashMap<Long, Long> fileCreTime) {
    this.fileCreTime = fileCreTime;
  }

  public HashMap<Long, Long> getFileCloTime() {
    return fileCloTime;
  }

  public void setFileCloTime(HashMap<Long, Long> fileCloTime) {
    this.fileCloTime = fileCloTime;
  }

  public ConcurrentHashMap<Long,String> getProbMsg() {
    return probMsg;
  }

  public void setProbMsg(ConcurrentHashMap<Long, String> probMsg) {
    this.probMsg = probMsg;
  }
  
}
