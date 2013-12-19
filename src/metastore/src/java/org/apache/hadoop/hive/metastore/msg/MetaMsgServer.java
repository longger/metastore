package org.apache.hadoop.hive.metastore.msg;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.tools.HiveMetaTool;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class MetaMsgServer {

  public static final Log LOG = LogFactory.getLog(ObjectStore.class.getName());
  static String zkAddr = "127.0.0.1:3181";
  static Producer producer =  null;
  static int times = 3;
  static MetaMsgServer server = null;
  private static boolean initalized = false;
  private static SendThread send = new SendThread();
  private static boolean zkfailed = false;
  private static long sleepSeconds = 60l;
  static ConcurrentLinkedQueue<DDLMsg> queue = new ConcurrentLinkedQueue<DDLMsg>();

  private static ConcurrentLinkedQueue<DDLMsg> failed_queue = new ConcurrentLinkedQueue<DDLMsg>();

  static{
    send.start();
  }


  private  static void initalize() throws MetaClientException{
    server = new MetaMsgServer();
    producer.config(zkAddr);
    producer = Producer.getInstance();
    initalized = true;
    zkfailed = false;

  }
  private static void reconnect() throws MetaClientException
  {
    producer.config(zkAddr);
    producer = Producer.getInstance();
    initalized = true;
    zkfailed = false;
  }


  public static void start() throws MetaClientException{
    if(!initalized){
      initalize();

    }

  }

  public static void sendMsg(DDLMsg msg) {
    queue.add(msg);
    send.release();
  }

  //zy  是不是应该release多个
  public static void sendMsg(List<DDLMsg> msgs) {
    queue.addAll(msgs);
    send.release();
  }


  public static class SendThread extends Thread{
    private static final int MSG_SEND_BATCH=0;
    Semaphore sem  = new Semaphore(MSG_SEND_BATCH);
    @Override
    public void run() {
      // TODO Auto-generated method stub

      while(true ){
        try{
          if(queue.isEmpty()){
            LOG.debug("---in sendThread before ac");
            sem.acquire();
            LOG.debug("---in sendThread after ac");
            if(queue.isEmpty()){
              continue;
            }
          }

          if(zkfailed)
          {
            try{
              Thread.sleep(sleepSeconds*1000l);
              reconnect();
            }catch(InterruptedException e)
            {

            }catch(MetaClientException e){
              zkfailed = true;
            }

          }
          DDLMsg msg = queue.peek();
          boolean succ = sendDDLMsg(msg);
          if(!succ){
            if(!failed_queue.contains(msg)) {
              failed_queue.add(msg);
            }
          }else{

            failed_queue.remove(queue.poll());

            if(!failed_queue.isEmpty()){
              int i=0;
//              while(i++ < MSG_SEND_BATCH && !failed_queue.isEmpty()){//retry send faild msg
              while( !failed_queue.isEmpty()){//retry send faild msg,old msg should send as soon as possible.
                DDLMsg retry_msg =failed_queue.peek();
                if(!sendDDLMsg(retry_msg)){
                  break;
                }else{
                  failed_queue.poll();
                }
              }
            }
          }
        } catch (Exception e) {
          LOG.error(e,e);
        }
      }

    }

    public void release(){
      sem.release();
    }

  }


  public static String getZkAddr() {
    return zkAddr;
  }


  public static void setZkAddr(String zkAddr) {
    MetaMsgServer.zkAddr = zkAddr;
  }

  public static boolean  sendDDLMsg(DDLMsg msg) {
    String jsonMsg = "";

    jsonMsg = MSGFactory.getMsgData(msg);
    LOG.info("---zjw-- send ddl msg:"+jsonMsg);
    boolean success = false;

    success = retrySendMsg(jsonMsg, times);
    return success;
  }

  private static boolean retrySendMsg(String jsonMsg,int times){
    // FIXME: if server not initialized, just return true;
    if (!initalized) {
      return true;
    }
    if(times <= 0){
      zkfailed = true;
      return false;
    }

    boolean success = false;
    try{
      success = producer.sendMsg(jsonMsg);
    }catch(InterruptedException ie){
      LOG.error(ie,ie);
      return retrySendMsg(jsonMsg,times-1);
    } catch (MetaClientException e) {
      LOG.error(e,e);
      return retrySendMsg(jsonMsg,times-1);
    }
    return success;
  }


  public static class AsyncConsumer {
    final MetaClientConfig metaClientConfig = new MetaClientConfig();
    final ZKConfig zkConfig = new ZKConfig();
    String localhost_name;
    private  ObjectStore.MsgHandler handler ;
    public void consume() throws MetaClientException{
      //init objectstore,handler
      HiveConf hiveConf = new HiveConf(HiveMetaTool.class);
      ObjectStore ob = new ObjectStore();
      ob.setConf(hiveConf);
      handler = ob.new MsgHandler();

      try {
        localhost_name = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      //设置zookeeper地址
      zkConfig.zkConnect = MetaMsgServer.zkAddr;

      //这个地址到底是怎么初始化的。。没看懂呢。。consumer可以放在producer类一样的地方启动
//      zkConfig.zkConnect = "192.168.1.13:3181";
      metaClientConfig.setZkConfig(zkConfig);
      // New session factory,强烈建议使用单例
      MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
      // subscribed topic
      final String topic = "meta-test";
      // consumer group
      final String group = "meta-example";
      // create consumer,强烈建议使用单例

      //生成处理线程
      MessageConsumer consumer =
      sessionFactory.createConsumer(new ConsumerConfig(group));
      //订阅事件，MessageListener是事件处理接口
      consumer.subscribe(topic, 1024, new MessageListener(){

        @Override
        public Executor getExecutor() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public void recieveMessages(final Message message) {
          DDLMsg msg = new DDLMsg();
          String data = new String(message.getData());

          LOG.info("---zy--consume msg: " + data);
//          System.out.println(data);
//          msg = DDLMsg.fromJson(data);
//          if(msg.getLocalhost_name().equals(localhost_name))
//          {
//            LOG.info("---zy--local msg,no need to refresh " );
////            handler.refresh(msg);
//          }
//          else
          //just test
//          handler.refresh(msg);
        }

      }
      );
      consumer.completeSubscribe();
    }
  }


  public static class Producer {
    private static Producer instance= null;
    private final MetaClientConfig metaClientConfig = new MetaClientConfig();
    private final ZKConfig zkConfig = new ZKConfig();
    private MessageSessionFactory sessionFactory = null;
    // create producer,强烈建议使用单例
    private MessageProducer producer = null;
    // publish topic
    private final String topic = "meta-test";
    private static String  zkAddr = "127.0.0.1:3181";

    public static void config(String addr){
      zkAddr = addr;
    }

    private Producer() {
        //设置zookeeper地址

        zkConfig.zkConnect = zkAddr;
        metaClientConfig.setZkConfig(zkConfig);
        // New session factory,强烈建议使用单例
        connect();
    }

    private void connect(){
      try{
        sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        producer = sessionFactory.createProducer();
        producer.publish(topic);
      }
      catch(MetaClientException e){
        LOG.error(e.getMessage());
      }
      LOG.info("Topic '" + topic + "' has been published.");
    }

    public static Producer getInstance() throws MetaClientException {
      if(instance == null){
        instance = new Producer();
      }
      return instance;
    }

    boolean sendMsg(String msg) throws MetaClientException, InterruptedException{
        LOG.debug("in send msg:"+msg);

        if(producer == null){
          connect();
          if(producer == null){
            return false;
          }
        }
        SendResult sendResult = producer.sendMessage(new Message(topic, msg.getBytes()));
        // check result

        boolean success = sendResult.isSuccess();
        if (!success) {
            LOG.error("Send message failed,error message:" + sendResult.getErrorMessage());
        }
        else {
            LOG.debug("Send message successfully,sent to " + sendResult.getPartition());
        }
        return success;
    }
  }

  public static void main(String[] args){


    try {
      new AsyncConsumer().consume();
    } catch (MetaClientException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }


}
