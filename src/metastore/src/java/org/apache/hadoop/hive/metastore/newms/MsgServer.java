package org.apache.hadoop.hive.metastore.newms;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.hive.metastore.msg.MSGFactory;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;


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

public class MsgServer {
//	public static final Log LOG = LogFactory.getLog(ObjectStore.class.getName());
	private static NewMSConf conf;
	static Producer producer = null;
	static int times = 3;
	private static boolean initalized = false;
	private static SendThread send = new SendThread();
	private static boolean zkfailed = false;
	private static ConcurrentLinkedQueue<DDLMsg> queue = new ConcurrentLinkedQueue<DDLMsg>();
	private static ConcurrentLinkedQueue<DDLMsg> failed_queue = new ConcurrentLinkedQueue<DDLMsg>();

	public static void setConf(NewMSConf conf)
	{
		MsgServer.conf = conf;
	}
	
	public static void addMsg(DDLMsg msg) {
		queue.add(msg);
		send.release();
	}

	public static void startProducer() throws MetaClientException {
		if (!initalized) {
			initalize();
			new Thread(send).start();
		}
	}
	
	public static void startConsumer(String zkaddr, String topic, String group) throws MetaClientException
	{
		new Consumer(zkaddr, topic, group).consume();
	}

	private static void initalize() throws MetaClientException {
//		Producer.config(zkAddr);
		producer = Producer.getInstance();
		initalized = true;
		zkfailed = false;

	}

	private static void reconnect() throws MetaClientException {
//		Producer.config(zkAddr);
		producer = Producer.getInstance();
		initalized = true;
		zkfailed = false;
	}

	private static boolean sendDDLMsg(DDLMsg msg) {
		String jsonMsg = "";

		jsonMsg = MSGFactory.getMsgData(msg);
		System.out.println("---zjw-- send ddl msg:" + jsonMsg);
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
      return retrySendMsg(jsonMsg,times-1);
    } catch (MetaClientException e) {
      e.printStackTrace();
      return retrySendMsg(jsonMsg,times-1);
    }
    return success;
  }
	
	static class SendThread implements Runnable{
		Semaphore sem  = new Semaphore(0);
		public SendThread() {
		}
		
		public void release(){
      sem.release();
    }
		@Override
		public void run() {
			 while(true ){
	        try{
	          if(queue.isEmpty()){
	            sem.acquire();
	            if(queue.isEmpty()){
	              continue;
	            }
	          }

	          if(zkfailed)
	          {
	            try{
	              Thread.sleep(1*1000l);
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
	        	e.printStackTrace();
	        }
	      }

		}
		
	}
	
	public static class Producer
	{
		private static Producer instance= null;
    private final MetaClientConfig metaClientConfig = new MetaClientConfig();
    private final ZKConfig zkConfig = new ZKConfig();
    private MessageSessionFactory sessionFactory = null;
    // create producer,强烈建议使用单例
    private MessageProducer producer = null;
    // publish topic
    private static String topic = "meta-newms";
    private static String  zkAddr = conf.getZkaddr();

    //获取实例之前要先调这个方法
//    public static void config(String addr){
//      zkAddr = addr;
//    }

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
//        LOG.error(e.getMessage());
      	e.printStackTrace();
      }
      System.out.println("Topic '" + topic + "' has been published.");
    }

    public static Producer getInstance() throws MetaClientException {
      if(instance == null){
        instance = new Producer();
      }
      return instance;
    }

    boolean sendMsg(String msg) throws MetaClientException, InterruptedException{
//        LOG.debug("in send msg:"+msg);

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
            System.out.println("Send message failed,error message:" + sendResult.getErrorMessage());
        }
        else {
            System.out.println("Send message successfully,sent to " + sendResult.getPartition());
        }
        return success;
    }
	}
	

	public static class Consumer {
		final MetaClientConfig metaClientConfig = new MetaClientConfig();
		final ZKConfig zkConfig = new ZKConfig();
		private String localhost_name;
		private String zkaddr;
		private String topic;
		private String group;
		private ConcurrentLinkedQueue<DDLMsg> failedq = new ConcurrentLinkedQueue<DDLMsg>();
		private MsgProcessing mp;
		public Consumer(String zkaddr, String topic, String group) {
			this.zkaddr = zkaddr;
			this.topic = topic;
			this.group = group;
			try {
				localhost_name = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			mp = new MsgProcessing(conf);
		}

		public void consume() throws MetaClientException {
			// 设置zookeeper地址
			zkConfig.zkConnect = zkaddr;
			metaClientConfig.setZkConfig(zkConfig);
			// New session factory,强烈建议使用单例
			MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
			// create consumer,强烈建议使用单例

			// 生成处理线程
			ConsumerConfig cc = new ConsumerConfig(group);
//			cc.setConsumeFromMaxOffset();
			MessageConsumer consumer = sessionFactory.createConsumer(cc);
			
			// 订阅事件，MessageListener是事件处理接口
			consumer.subscribe(topic, 1024, new MessageListener() {

				@Override
				public Executor getExecutor() {
					// TODO Auto-generated method stub
					return null;
				}

				@Override
				public void recieveMessages(final Message message) {
					String data = new String(message.getData());
					System.out.println(data);
					int time = 0;
//					 if(data != null)
//					 return;
					DDLMsg msg = DDLMsg.fromJson(data);
					while (time <= 3) {
						if (time >= 3) {
							failedq.add(msg);
							System.out.println("handle msg failed, add msg into failed queue: "+ msg.getMsg_id());
							break;
						}
						try {
							mp.handleMsg(msg);
							if (!failedq.isEmpty()) {
								msg = failedq.poll();
								System.out.println("handle msg in failed queue: "+ msg.getMsg_id());
								time = 0;
							} else
								// 能到else一定是handlemsg没抛异常成功返回，而failedq是空的
								break;
						} catch (Exception e) {
							time++;
							try {
								Thread.sleep(1 * 1000);
							} catch (InterruptedException e2) {
							}
							e.printStackTrace();
						}
					}
				}

			});
			consumer.completeSubscribe();
		}
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
}
