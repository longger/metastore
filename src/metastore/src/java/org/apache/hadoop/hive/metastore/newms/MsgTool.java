package org.apache.hadoop.hive.metastore.newms;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class MsgTool {
  public static int index;
  
  public static void main(String[] args) {
    
    try {
//      HandleMsg hlm = new HandleMsg("node14:3181", "newms-test", "msgTool");
      HandleMsg hlm = new HandleMsg("node13:3181", "meta-test", "msgToola");
      hlm.handle();			
      System.out.println("Message handle is completing.");
      
    } catch (MetaClientException e) {
      e.printStackTrace();
    }
  }

  
  public static class HandleMsg {
    final MetaClientConfig metaClientConfig = new MetaClientConfig();
    final ZKConfig zkConfig = new ZKConfig();
    private String localhost_name;
    private String zkaddr;
    private String topic;
    private String group;
    private MsgStatistics mst = new MsgStatistics();
    
    public HandleMsg(String zkaddr, String topic, String group) {
      this.zkaddr = zkaddr;
      this.topic = topic;
      this.group = group;
      try {
        localhost_name = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
      }
    }

    public void handle() throws MetaClientException {
      // 设置zookeeper地址
      zkConfig.zkConnect = zkaddr;
      metaClientConfig.setZkConfig(zkConfig);
      // New session factory,强烈建议使用单例
      MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
      // create consumer,强烈建议使用单例

      // 生成处理线程
      ConsumerConfig cc = new ConsumerConfig(group);
//      cc.setOffset(0);
      MessageConsumer consumer = sessionFactory.createConsumer(cc);
    
      // 订阅事件，MessageListener是事件处理接口
      consumer.subscribe(topic, 1024, new MessageListener() {

        @Override
        public Executor getExecutor() {
          return null;
        }

        @Override
        public void recieveMessages(final Message message) {
          String data = new String(message.getData());
          //System.out.println("Begin to handle " + data);
          DDLMsg msg = DDLMsg.fromJson(data);
          mst.handleMsg(msg);
          
          index++;
          if(index%10000 == 0){
            msgStatistics();
          }
          
        }
        
        public void msgStatistics(){
          
          System.out.println("This is the messages from 0 to " + index + " 's statistics:");
          System.out.println("MsgID:1001----createDb MsgCounts:" + mst.getCreateDbIndex());
          System.out.println("MsgID:1002----alterDb MsgCounts:" + mst.getAlterDbIndex());
          System.out.println("MsgID:1003----alterDbparam MsgCounts:" + mst.getAlterDbparamIndex());
          System.out.println("MsgID:1004----dropDb MsgCounts:" + mst.getAlterDbparamIndex());
          System.out.println("MsgID:1101----createTb lMsgCounts:" + mst.getCreateTblIndex());
          System.out.println("MsgID:1102----alterTblName MsgCounts:" + mst.getAlterTblNameIndex());
          System.out.println("MsgID:1103----alterTblDistri MsgCounts:" + mst.getCreateTblIndex());
          System.out.println("MsgID:1104----alterTblPart MsgCounts:" + mst.getAlterTblPartIndex());
          System.out.println("MsgID:1105----alterTblSplit MsgCounts:" + mst.getAlterTblSplitIndex());
          System.out.println("MsgID:1201----alterTblDelcol MsgCounts:" + mst.getAlterTblDelcolIndex());
          System.out.println("MsgID:1202----alterTblAddcol MsgCounts:" + mst.getAlterTblAddcolIndex());
          System.out.println("MsgID:1203----alterTblcolName MsgCounts:" + mst.getAlterTblcolNameIndex());
          System.out.println("MsgID:1204----alterTblcolType MsgCounts:" + mst.getAlterTblcolTypeIndex());
          System.out.println("MsgID:1205----alterTblcolLen MsgCounts:" + mst.getAlterTblcolLenIndex());
          System.out.println("MsgID:1206----alterTblParam MsgCounts:" + mst.getAlterTblParamIndex());
          System.out.println("MsgID:1207----dropTbl MsgCounts:" + mst.getDropTblIndex());
          System.out.println("MsgID:1208----TblBusiChange MsgCounts:" + mst.getTblBusiChangeIndex());
          System.out.println("MsgID:1301----createPart MsgCounts:" + mst.getCreatePartIndex());
          System.out.println("MsgID:1302----alterPart MsgCounts:" + mst.getAlterPartIndex());
          System.out.println("MsgID:1303----delPart MsgCounts:" + mst.getDelPartIndex());
          System.out.println("MsgID:1304----addPartFile MsgCounts:" + mst.getAddPartFileIndex());
          System.out.println("MsgID:1305----alterPartFile MsgCounts:" + mst.getAlterPartFileIndex());
          System.out.println("MsgID:1306----repFileChange MsgCounts:" + mst.getRepFileChangeIndex());
          System.out.println("MsgID:1307----staFileChange MsgCounts:" + mst.getStaFileChanfeIndex());
          System.out.println("MsgID:1308----repFileOnOff MsgCounts:" + mst.getRepFileOnOffIndex());
          System.out.println("MsgID:1309----delPartFile MsgCounts:" + mst.getDelPartFileIndex());
          System.out.println("MsgID:1310----fileUsrSetRepChan MsgCounts:" + mst.getFileUsrSetRepChanIndex());
          System.out.println("MsgID:1401----createInd MsgCounts:" + mst.getCreateIndIndex());
          System.out.println("MsgID:1402----alterInd MsgCounts:" + mst.getAlterIndIndex());
          System.out.println("MsgID:1403----alterIndParm MsgCounts:" + mst.getAlterDbparamIndex());
          System.out.println("MsgID:1404----delInd MsgCounts:" + mst.getDelIndIndex());
          System.out.println("MsgID:1405----createPartInd MsgCounts:" + mst.getCreatePartIndIndex());
          System.out.println("MsgID:1406----alterPartInd MsgCounts:" + mst.getAlterPartIndIndex());
          System.out.println("MsgID:1407----delPartInd MsgCounts:" + mst.getDelPartIndIndex());
          System.out.println("MsgID:1408----createPartIndFile MsgCounts:" + mst.getCreatePartIndFileIndex());
          System.out.println("MsgID:1409----alterPartIndFile MsgCounts:" + mst.getAlterPartIndFileIndex());
          System.out.println("MsgID:1413----delPartIndFile MsgCounts:" + mst.getDelPartIndFileIndex());
          System.out.println("MsgID:1501----createNode MsgCounts:" + mst.getCreateNodeIndex());
          System.out.println("MsgID:1502----delNode MsgCounts:" + mst.getDelNodeIndex());
          System.out.println("MsgID:1503----failNode MsgCounts:" + mst.getFailNodeIndex());
          System.out.println("MsgID:1504----backNode MsgCounts:" + mst.getBackNodeIndex());
          System.out.println("MsgID:1601----createSchema MsgCounts:" + mst.getCreateSchemaIndex());
          System.out.println("MsgID:1602----modifyScheName MsgCounts:" + mst.getModifyScheNameIndex());
          System.out.println("MsgID:1603----modifyScheDelcol MsgCounts:" + mst.getModifyScheDelcolIndex());
          System.out.println("MsgID:1604----modifyScheAddcol MsgCounts:" + mst.getModifyScheAddcolIndex());
          System.out.println("MsgID:1605----modifyScheAltcolName MsgCounts:" + mst.getModifyScheAltcolNameIndex());
          System.out.println("MsgID:1606----modifyScheAltcolType MsgCounts:" + mst.getModifyScheAltcolTypeIndex());
          System.out.println("MsgID:1607----modifySchemaParam MsgCounts:" + mst.getModifySchemaParamIndex());
          System.out.println("MsgID:1608----delSchema MsgCounts:" + mst.getDelSchemaIndex());
          System.out.println("MsgID:2001----DDLDirectDw1 MsgCounts:" + mst.getDDLDirectDw1Index());
          System.out.println("MsgID:2002----DDLDirectDw2 MsgCounts:" + mst.getDDLDirectDw2Index());
          System.out.println("MsgID:3001----createNg MsgCounts:" + mst.getCreateNgIndex());
          System.out.println("MsgID:3002----modifyNg MsgCounts:" + mst.getModifyNgIndex());
          System.out.println("MsgID:3003----delNg MsgCounts:" + mst.getDelNgIndex());
          System.out.println("MsgID:3004----alterNg MsgCounts:" + mst.getAlterNgIndex());
          System.out.println("MsgID:4001----createFile MsgCounts:" + mst.getCreateFileIndex());
          System.out.println("MsgID:4002----delFile MsgCounts:" + mst.getDelFileIndex());
          System.out.println("MsgID:5101----grantGlobal MsgCounts:" + mst.getGrantGlobalIndex());
          System.out.println("MsgID:5102----grantDB MsgCounts:" + mst.getGrantDBIndex());
          System.out.println("MsgID:5103----grantTbl MsgCounts:" + mst.getGrantTblIndex());
          System.out.println("MsgID:5104----grantSchema MsgCounts:" + mst.getGrantSchemaIndex());
          System.out.println("MsgID:5105----grantPart MsgCounts:" + mst.getGrantPartIndex());
          System.out.println("MsgID:5106----grantPartClo MsgCounts:" + mst.getGrantPartCloIndex());
          System.out.println("MsgID:5107----grantTblCol MsgCounts:" + mst.getGrantTblColIndex());
          System.out.println("MsgID:5201----revokeGlobal MsgCounts:" + mst.getRevokeGlobalIndex());
          System.out.println("MsgID:5202----revokeDB MsgCounts:" + mst.getRevokeDBIndex());
          System.out.println("MsgID:5203----revokeTbl MsgCounts:" + mst.getRevokeTblIndex());
          System.out.println("MsgID:5204----revokeSchema MsgCounts:" + mst.getRevokeSchemaIndex());
          System.out.println("MsgID:5205----revokePart MsgCounts:" + mst.getRevokePartIndex());
          System.out.println("MsgID:5206----revokePartClo MsgCounts:" + mst.getRevokePartColIndex());
          System.out.println("MsgID:5207----revokeTblCol MsgCounts:" + mst.getRevokeTblColIndex());
          
          System.out.println("The average time of a file create to delete is :" + mst.avgFileCreDelTime());
          
          ConcurrentHashMap<Long,String> probMsg = mst.getProbMsg();
          
          for(Long msgkey : probMsg.keySet()){
            System.out.println(msgkey + "----" + probMsg.get(msgkey));
          }
        }

      });
      consumer.completeSubscribe();
    }
    
  }
}
