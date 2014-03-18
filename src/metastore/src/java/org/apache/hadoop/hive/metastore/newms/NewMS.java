package org.apache.hadoop.hive.metastore.newms;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.newms.NewMSConf.RedisInstance;
import org.apache.hadoop.hive.metastore.newms.NewMSConf.RedisMode;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class NewMS {

	public static Log LOG = LogFactory.getLog(NewMS.class);
	final ZKConfig zkConfig = new ZKConfig();
	private NewMSConf conf;

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
				if (args[i].length() < 2)
					throw new IllegalArgumentException("Not a valid argument: " + args[i]);
				if (args[i].charAt(1) == '-') {
					if (args[i].length() < 3)
						throw new IllegalArgumentException("Not a valid argument: "
								+ args[i]);
				} else {
					if (args.length - 1 > i)
						if (args[i + 1].charAt(0) == '-') {
							optsList.add(new Option(args[i], null));
						} else {
							optsList.add(new Option(args[i], args[i + 1]));
							i++;
						}
					else {
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

	static class RPCServer implements Runnable {
		private NewMSConf conf;

		public RPCServer(NewMSConf conf) {
			this.conf = conf;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub

			LOG.info("RPCServer start at port:" + conf.getRpcport());

			TProcessor tprocessor = new ThriftHiveMetastore.Processor<ThriftHiveMetastore.Iface>(new ThriftRPC(conf));
			try {
				TServerTransport tt = new TServerSocket(conf.getRpcport());
				TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(tt)
						.transportFactory(new TTransportFactory())
						.protocolFactory(new TBinaryProtocol.Factory()).minWorkerThreads(5)
						.processor(tprocessor).maxWorkerThreads(100);
				TServer server = new TThreadPoolServer(sargs);
				server.serve();
			} catch (TTransportException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) {

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
					// set serverPort
					if (o.opt == null) {
						System.out.println("-mh metastore server ip. ");
						System.exit(0);
					}
					mh = o.opt;
				}
				if (o.flag.equals("-mp")) {
					// set serverPort
					if (o.opt == null) {
						System.out.println("-mp metastore server port. ");
						System.exit(0);
					}
					mp = o.opt;
				}
				if (o.flag.equals("-rm")) {
					if (o.opt == null) {
						System.out.println("-rm redismode");
						System.exit(0);
					}
					if (o.opt.equals("STA"))
						rm = RedisMode.STANDALONE;
					else if (o.opt.equals("STL"))
						rm = RedisMode.SENTINEL;
					else {
						System.out.println("wrong redis mode:" + o.opt
								+ ", should be STA or STL");
						System.exit(0);
					}
				}
				if (o.flag.equals("-ra")) {
					// set serverPort
					if (o.opt == null) {
						System.out
								.println("-ra redis or sentinel addr. <host:port;host:port> ");
						System.exit(0);
					}
					ra = o.opt;
				}
				if (o.flag.equals("-zka")) {
					// set redis server name
					if (o.opt == null) {
						System.out.println("-zka zkaddr <host:port>.");
						System.exit(0);
					}
					zkaddr = o.opt;
				}
				if (o.flag.equals("-rpcp")) {
					// set rpc service port
					if (o.opt == null) {
						System.out.println("-rpcp rpc service port ");
						System.exit(0);
					}
					rpcp = Integer.parseInt(o.opt);
				}

				if (o.flag.equals("-fcs")) {
					if (o.opt == null) {
						System.out.println("-fcs file cache size.");
						System.exit(0);
					}
					fcs = Integer.parseInt(o.opt);
				}
			}

			if (mh == null || mp == null) {
				System.out.println("please provide ms host and ms port");
				System.exit(0);
			}

			if (rm == null || ra == null || zkaddr == null) {
				System.out.println("please provide enough args.");
				System.exit(0);
			} else {
				switch (rm) {
				case SENTINEL:
					sentinel = new HashSet<String>();
					for (String s : ra.split(";"))
						sentinel.add(s);
					conf = new NewMSConf(sentinel, rm, zkaddr, mh,
							Integer.parseInt(mp), rpcp);
					break;
				case STANDALONE:
					ri = new ArrayList<RedisInstance>();
					for (String rp : ra.split(";")) {
						System.out.println(rp);
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
		
		new Thread(new RPCServer(conf)).start();
		MsgServer.setConf(conf);
		RawStoreImp.setNewMSConf(conf);
		try {
			MsgServer.startConsumer(conf.getZkaddr(), "meta-test", "newms");
		} catch (MetaClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//get g_fid from redis
		Jedis jedis = null;
    try{
    	jedis = new RedisFactory(conf).getDefaultInstance();
    	String fid = jedis.get("g_fid");
    	if(fid == null)
    		RawStoreImp.setFID(0l);
    	else
    		RawStoreImp.setFID(Long.parseLong(fid));
    	
    }catch(JedisConnectionException e){
    	LOG.warn(e,e);
    	RedisFactory.putBrokenInstance(jedis);
    	jedis = null;
    }finally{
    	RedisFactory.putInstance(jedis);
    }
		 // Add shutdown hook.
		final NewMSConf co = conf; 
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        String shutdownMsg = "Shutting down newms, store g_fid: "+RawStoreImp.getFid()+" in redis.";
        LOG.info(shutdownMsg);
        Jedis jedis = null;
        try{
        	jedis = new RedisFactory(co).getDefaultInstance();
        	jedis.set("g_fid",RawStoreImp.getFid()+"");
        	
        }catch(JedisConnectionException e){
        	LOG.warn(e,e);
        	RedisFactory.putBrokenInstance(jedis);
        	jedis = null;
        }finally{
        	RedisFactory.putInstance(jedis);
        }
      }
    });
	}

}
