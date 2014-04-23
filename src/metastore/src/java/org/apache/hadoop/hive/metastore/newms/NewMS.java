package org.apache.hadoop.hive.metastore.newms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreServerEventHandler;
import org.apache.hadoop.hive.metastore.TServerSocketKeepAlive;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.newms.NewMSConf.RedisInstance;
import org.apache.hadoop.hive.metastore.newms.NewMSConf.RedisMode;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class NewMS {
	public static Log LOG = LogFactory.getLog(NewMS.class);
	private final ZKConfig zkConfig = new ZKConfig();
	private NewMSConf conf;
	private static RPCServer rpc;
	
	public static class Option {
		String flag, opt;

		public Option(String flag, String opt) {
			this.flag = flag;
			this.opt = opt;
		}
	}

	private static List<Option> parseArgs(String[] args) {
		List<Option> optsList = new ArrayList<Option>();

		// parse the args
		for (int i = 0; i < args.length; i++) {
			System.out.println("Args " + i + ", " + args[i]);
			switch (args[i].charAt(0)) {
			case '-':
				if (args[i].length() < 2) {
          throw new IllegalArgumentException("Not a valid argument: " + args[i]);
        }
				if (args[i].charAt(1) == '-') {
					if (args[i].length() < 3) {
            throw new IllegalArgumentException("Not a valid argument: "
								+ args[i]);
          }
				} else {
					if (args.length - 1 > i) {
            if (args[i + 1].charAt(0) == '-') {
							optsList.add(new Option(args[i], null));
						} else {
							optsList.add(new Option(args[i], args[i + 1]));
							i++;
						}
          } else {
						optsList.add(new Option(args[i], null));
					}
				}
				break;
			default:
				// arg
				break;
			}
		}

		return optsList;
	}

	static class RPCServer {
		private TServer server;
		
		public void serve(){
			server.serve();
		}
		public void stop(){
			server.stop();
		}
		
		public RPCServer(NewMSConf conf) throws Throwable {
		  HiveConf hc = new HiveConf();
		  int port = conf.getRpcport();
			int minWorkerThreads = hc.getIntVar(HiveConf.ConfVars.METASTORESERVERMINTHREADS);
      int maxWorkerThreads = hc.getIntVar(HiveConf.ConfVars.METASTORESERVERMAXTHREADS);
      boolean tcpKeepAlive = hc.getBoolVar(HiveConf.ConfVars.METASTORE_TCP_KEEP_ALIVE);

      try {
        TServerTransport serverTransport = tcpKeepAlive ?
            new TServerSocketKeepAlive(port) : new TServerSocket(port);
			  //TProcessor tprocessor = new ThriftHiveMetastore.Processor<ThriftHiveMetastore.Iface>(new ThriftRPC(conf));
			  TProcessor tprocessor = new TSetIpAddressProcessor<ThriftRPC>(new ThriftRPC(conf));

			  TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverTransport)
			  .transportFactory(new TTransportFactory())
			  .protocolFactory(new TBinaryProtocol.Factory())
			  .processor(tprocessor)
			  .minWorkerThreads(minWorkerThreads)
			  .maxWorkerThreads(maxWorkerThreads);

			  server = new TThreadPoolServer(sargs);

			  LOG.info("Started the NewMS on port [" + port + "]...");
			  LOG.info("Options.minWorkerThreads = "
          + minWorkerThreads);
			  LOG.info("Options.maxWorkerThreads = "
          + maxWorkerThreads);
			  LOG.info("TCP keepalive = " + tcpKeepAlive);

			  HiveMetaStoreServerEventHandler eventHandler = new HiveMetaStoreServerEventHandler();
			  server.setServerEventHandler(eventHandler);

			  server.serve();
			} catch (Throwable x) {
			  x.printStackTrace();
			  LOG.error(StringUtils.stringifyException(x));
			}
		}
	}

	static class FidStoreTask extends TimerTask
	{
		private NewMSConf conf;
		private RedisFactory rf;
		public FidStoreTask(NewMSConf conf)
		{
			this.conf = conf;
			rf = new RedisFactory(conf);
		}
		
		@Override
		public void run() {
			Jedis jedis = null;
			int err = 0;
			try{
				jedis = rf.getDefaultInstance();
				String fid = RawStoreImp.getFid()+"";
				jedis.set("g_fid", fid);
				LOG.info("store g_fid "+ fid + " into redis.");
			}catch(JedisException e){
				LOG.warn(e, e);
				err = -1;
			}finally{
				if (err < 0) {
	        RedisFactory.putBrokenInstance(jedis);
	      } else {
	        RedisFactory.putInstance(jedis);
	      }
			}
		}
		
	}
	
	public static void main(String[] args) throws Throwable {
		NewMSConf conf = null;
		int rpcp = 0;
		int fcs = 1000;
		if (args.length >= 1) {
			List<Option> ops = parseArgs(args);
			Set<String> sentinel = null;
			List<RedisInstance> ri = null;
			RedisMode rm = null;
			String zkaddr = null;
			String ra = null;
			String mh = null, mp = null;

			for (Option o : ops) {
				if (o.flag.equals("-h")) {
					// print help message
					System.out.println("-h    : print this help.");
					System.out.println("-mh   : metastore server ip.");
					System.out.println("-mp   : metastore server port.");
					System.out.println("-rm   : redis mode, <STA for stand alone or STL for sentinel>.");
					System.out.println("-ra   : redis or sentinel addr. <host:port;host:port> ");
					System.out.println("-zka  : zkaddr <host:port>.");
					System.out.println("-rpcp : rpcp service port.");
					System.out.println("-fcs  : file cache size.");

					System.exit(0);
				}
				if (o.flag.equals("-mh")) {
					// set old metastore serverName
					if (o.opt == null) {
						System.out.println("-mh metastore server name. ");
						System.exit(0);
					}
					mh = o.opt;
				}
				if (o.flag.equals("-mp")) {
					// set old metastore serverPort
					if (o.opt == null) {
						System.out.println("-mp metastore server port. ");
						System.exit(0);
					}
					mp = o.opt;
				}
				if (o.flag.equals("-rm")) {
				  // set redis mode
					if (o.opt == null) {
						System.out.println("-rm redismode");
						System.exit(0);
					}
					if (o.opt.equals("STA")) {
            rm = RedisMode.STANDALONE;
          } else if (o.opt.equals("STL")) {
            rm = RedisMode.SENTINEL;
          } else {
						System.out.println("Invalid redis mode:" + o.opt + ", should be STA or STL.");
						System.exit(0);
					}
				}
				if (o.flag.equals("-ra")) {
					// set redis or sentinel address
					if (o.opt == null) {
						System.out.println("-ra redis or sentinel addr. <host:port;host:port>.");
						System.exit(0);
					}
					ra = o.opt;
				}
				if (o.flag.equals("-zka")) {
					// set zookeeper address
				  if (o.opt == null) {
				    System.out.println("-zka zkaddr <host:port>.");
				    System.exit(0);
				  }
					zkaddr = o.opt;
				}
				if (o.flag.equals("-rpcp")) {
					// set rpc service port
					if (o.opt == null) {
						System.out.println("-rpcp rpc service port.");
						System.exit(0);
					}
					rpcp = Integer.parseInt(o.opt);
				}
				if (o.flag.equals("-fcs")) {
				  // set file cache size
				  if (o.opt == null) {
				    System.out.println("-fcs file cache size.");
				    System.exit(0);
				  }
				  fcs = Integer.parseInt(o.opt);
				}
			}

			if (mh == null || mp == null) {
			  System.out.println("please provide old metastore host and port");
			  System.exit(0);
			}

			if (rm == null || ra == null || zkaddr == null) {
				System.out.println("please provide redis mode, address, zkaddr args.");
				System.exit(0);
			} else {
				switch (rm) {
				case SENTINEL:
					sentinel = new HashSet<String>();
					for (String s : ra.split(";")) {
            sentinel.add(s);
          }
					conf = new NewMSConf(sentinel, rm, zkaddr, mh,
							Integer.parseInt(mp), rpcp);
					break;
				case STANDALONE:
					ri = new ArrayList<RedisInstance>();
					for (String rp : ra.split(";")) {
//						System.out.println(rp);
						String[] s = rp.split(":");
						ri.add(new RedisInstance(s[0], Integer.parseInt(s[1])));
					}
					conf = new NewMSConf(ri, rm, zkaddr, mh, Integer.parseInt(mp), rpcp);
					break;
				}
				conf.setFcs(fcs);
			}
		} else {
			String zkaddr = "192.168.1.13:3181";
			// System.out.println("please provide arguments, use -h for help");
			List<RedisInstance> lr = new ArrayList<RedisInstance>();
			lr.add(new RedisInstance("localhost", 6379));
			conf = new NewMSConf(lr, RedisMode.STANDALONE, zkaddr, "node13", 10101,8111);

			// Set<String> s = new HashSet<String>();
			// s.add("localhost:26379");
			// conf = new DatabakConf(s,RedisMode.SENTINEL,zkaddr,"node13",10101,8111);
			conf.setFcs(40000);
			// System.exit(0);
		}

		// get g_fid from redis
		Jedis jedis = null;
    try {
    	jedis = new RedisFactory(conf).getDefaultInstance();
    	if (jedis == null) {
        throw new IOException("Connect to redis server failed.");
      }
    	String fid = jedis.get("g_fid");
    	if (fid != null) {
    		long id = Long.parseLong(fid);
    		synchronized (RawStoreImp.class) {
					if (RawStoreImp.getFid() < id) {
            RawStoreImp.setFID(Long.parseLong(fid));
          }
				}
    	}
    } catch (JedisException e) {
    	LOG.warn(e,e);
    	RedisFactory.putBrokenInstance(jedis);
    	throw e;
    } finally {
    	RedisFactory.putInstance(jedis);
    }

    // Add shutdown hook.
		final NewMSConf co = conf;
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        String shutdownMsg = "Shutting down newms, flush g_fid: " + RawStoreImp.getFid() + " to redis.";
        LOG.info(shutdownMsg);
        Jedis jedis = null;
        try {
        	jedis = new RedisFactory(co).getDefaultInstance();
        	if (jedis != null) {
            jedis.set("g_fid", RawStoreImp.getFid() + "");
          }
        } catch (JedisException e) {
        	LOG.warn(e,e);
        	RedisFactory.putBrokenInstance(jedis);
        } finally {
        	RedisFactory.putInstance(jedis);
        }
        
        rpc.stop();
        LOG.info("stop RPCServer.");
        
        while(!MsgServer.isQueueEmpty()){
        	LOG.info("waiting for queues in MsgServer to be empty...");
        	try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						LOG.warn(e,e);
					}
        }
      }
    });
    
    HiveConf hc = new HiveConf();
    if(hc.get("isOldWithNew") != null && hc.get("isOldWithNew").equals("true"))
    {
	    Thread t = new Thread(new Runnable(){
				@Override
				public void run() {
					try {
						HiveMetaStore.main(new String[]{});
					} catch (Throwable e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}  
				}
	    });
	    t.start();
    }
    
    Timer timer = new Timer("FidStorer",true);
    timer.schedule(new FidStoreTask(conf), 60*1000, 60*1000);
    try {
      MsgServer.setConf(conf);
      RawStoreImp.setNewMSConf(conf);
      try {
        MsgServer.startConsumer(conf.getZkaddr(), "meta-test", "newms");
        MsgServer.startProducer();
        MsgServer.startLocalConsumer();
      } catch (MetaClientException e) {
        LOG.error(e, e);
        throw new IOException("Start MsgServer failed: " + e.getMessage());
      }
      rpc = new RPCServer(conf);
      rpc.serve();
    } catch (Throwable t) {
      // Catch the exception, log it and rethrow it.
      LOG.error("NewMS Thrift Server threw an exception...", t);
      System.exit(-1);
      throw t;
    }
	}
}
