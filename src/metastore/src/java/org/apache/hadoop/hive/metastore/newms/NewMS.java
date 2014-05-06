package org.apache.hadoop.hive.metastore.newms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreServerEventHandler;
import org.apache.hadoop.hive.metastore.TServerSocketKeepAlive;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
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

public class NewMS {
	public static Log LOG = LogFactory.getLog(NewMS.class);
	private static HiveConf conf = new HiveConf();
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

		public RPCServer() throws Throwable {
		  int port = conf.getIntVar(ConfVars.NEWMS_RPC_PORT);
			int minWorkerThreads = conf.getIntVar(HiveConf.ConfVars.METASTORESERVERMINTHREADS);
      int maxWorkerThreads = conf.getIntVar(HiveConf.ConfVars.METASTORESERVERMAXTHREADS);
      boolean tcpKeepAlive = conf.getBoolVar(HiveConf.ConfVars.METASTORE_TCP_KEEP_ALIVE);

      try {
        TServerTransport serverTransport = tcpKeepAlive ?
            new TServerSocketKeepAlive(port) : new TServerSocket(port);
			  //TProcessor tprocessor = new ThriftHiveMetastore.Processor<ThriftHiveMetastore.Iface>(new ThriftRPC(conf));
			  TProcessor tprocessor = new NewMSTSetIpAddressProcessor<Iface>(new ThriftRPC().newProxy());

			  TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverTransport)
			  .processor(tprocessor)
			  .transportFactory(new TTransportFactory())
			  .protocolFactory(new TBinaryProtocol.Factory())
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

			} catch (Throwable x) {
			  x.printStackTrace();
			  LOG.error(StringUtils.stringifyException(x));
			}
		}
	}

	static class FidStoreTask extends TimerTask {
	  private final RedisFactory rf;

	  public FidStoreTask() {
	    rf = new RedisFactory();
	  }

	  @Override
	  public void run() {
	    Jedis jedis = null;
	    int err = 0;

	    try {
	      jedis = rf.getDefaultInstance();
	      String fid = RawStoreImp.getFid() + "";
	      jedis.set("g_fid", fid);
	      LOG.info("Store current g_fid " + fid + " into redis.");
	    } catch(JedisException e) {
	      LOG.warn(e, e);
	      err = -1;
	    } finally{
	      if (err < 0) {
	        RedisFactory.putBrokenInstance(jedis);
	      } else {
	        RedisFactory.putInstance(jedis);
	      }
	    }
	  }

	}

	public static void main(String[] args) throws Throwable {
		// get g_fid from redis
		Jedis jedis = null;
    try {
    	jedis = new RedisFactory().getDefaultInstance();
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
    		LOG.info("NewMS restore FID to " + id);
    	}
    } catch (JedisException e) {
    	LOG.warn(e,e);
    	RedisFactory.putBrokenInstance(jedis);
    	throw e;
    } finally {
    	RedisFactory.putInstance(jedis);
    }

    // Add shutdown hook.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        String shutdownMsg = "Shutting down newms, flush g_fid: " + RawStoreImp.getFid() + " to redis.";
        LOG.info(shutdownMsg);
        Jedis jedis = null;
        try {
        	jedis = new RedisFactory().getDefaultInstance();
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
        LOG.info("Stop RPCServer.");

        while (!MsgServer.isQueueEmpty()) {
        	LOG.info("Waiting for queues in MsgServer to be empty...");
        	try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						LOG.warn(e,e);
					}
        }
      }
    });

    if (conf.getBoolVar(ConfVars.NEWMS_IS_OLD_WITH_NEW)) {
	    Thread t = new Thread(new Runnable(){
				@Override
				public void run() {
					try {
						String uri = conf.getVar(ConfVars.METASTOREURIS);
						uri = uri.substring(uri.lastIndexOf(":") + 1);
						HiveMetaStore.main(new String[]{uri});
					} catch (Throwable e) {
						LOG.error(e, e);
					}
				}
	    });
	    t.start();
	    LOG.info("Waiting for OldMS starting ...");
	    synchronized (HiveMetaStore.isStarted) {
	      try {
	        HiveMetaStore.isStarted.wait();
	      } catch (InterruptedException e) {
	      }
	    }
	    LOG.info("OldMS service is started, starting NewMS ...");
    }

    Timer timer = new Timer("FidStorer",true);
    timer.schedule(new FidStoreTask(), 60 * 1000, 60 * 1000);
    try {
      try {
        MsgServer.startConsumer(conf.getVar(ConfVars.ZOOKEEPERADDRESS), "meta-test", "newms");
//        MsgServer.startProducer();
        MsgServer.startLocalConsumer();
      } catch (MetaClientException e) {
        LOG.error(e, e);
        throw new IOException("Start MsgServer failed: " + e.getMessage());
      }
      rpc = new RPCServer();
      rpc.serve();
    } catch (Throwable t) {
      // Catch the exception, log it and rethrow it.
      LOG.error("NewMS Thrift Server threw an exception...", t);
      System.exit(-1);
      throw t;
    }
	}
}
