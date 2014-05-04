package org.apache.hadoop.hive.metastore.newms;


import org.apache.hadoop.hive.conf.HiveConf;
//import org.apache.hadoop.hive.conf.HiveConf.RedisMode;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;


public class RedisFactory {
	private static HiveConf conf = new HiveConf();
	private static JedisSentinelPool jsp = null;
	private static JedisPool jp = null;
	private static HostAndPort ri = null;
	private final JedisPoolConfig config;

	public RedisFactory(){
		config = new JedisPoolConfig();
		config.setBlockWhenExhausted(false);
		config.setMaxTotal(2000);
	}

	public void destroy() {
	  switch (conf.getRedisMode()) {
	  case STANDALONE:
	    if (jp != null) {
        jp.destroy();
      }
	    break;
	  case SENTINEL:
	    if (jsp != null) {
        jsp.destroy();
      }
	    break;
	  }
	}

	// 从配置文件中读取redis的地址和端口,以此创建jedis对象
	public synchronized Jedis getDefaultInstance() {
		switch (conf.getRedisMode()) {
		case STANDALONE:

			if (ri == null) {
				ri = conf.getRedisHP();
				if (ri != null) {
          jp = new JedisPool(config, ri.getHost(), ri.getPort());
        }
			}
			if (jp != null) {
        return jp.getResource();
      }
		case SENTINEL:
		{
			Jedis r;
			if (jsp != null) {
        r = jsp.getResource();
      } else {
				jsp = new JedisSentinelPool("mymaster", conf.getSentinel(), config);
				r = jsp.getResource();
			}
			return r;
		}
		}
		return null;
	}

	public synchronized static Jedis putInstance(Jedis j) {
		if (j == null) {
      return null;
    }
		switch (conf.getRedisMode()) {
		case STANDALONE:
			jp.returnResource(j);
			break;
		case SENTINEL:
			jsp.returnResource(j);
			break;
		}
		return null;
	}

	public synchronized static Jedis putBrokenInstance(Jedis j) {
		if (j == null) {
      return null;
    }
		switch (conf.getRedisMode()) {
		case STANDALONE:
			jp.returnBrokenResource(j);
			break;
		case SENTINEL:
			jsp.returnBrokenResource(j);
		}
		return null;
	}
}
