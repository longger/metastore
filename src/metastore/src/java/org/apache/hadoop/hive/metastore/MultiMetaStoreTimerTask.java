package org.apache.hadoop.hive.metastore;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.newms.CacheStore;
import org.apache.hadoop.hive.metastore.newms.RawStoreImp;
import org.apache.hadoop.hive.metastore.newms.RedisFactory;
import org.apache.hadoop.hive.metastore.newms.ThriftRPC;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

public class MultiMetaStoreTimerTask extends TimerTask {
  private final Log LOG = LogFactory.getLog(MultiMetaStoreTimerTask.class);
  private final HiveConf conf;
  public int period;
  public int failoverTO;
  private Jedis jedis;
  private final String hbkey;
  private final String serverName;
  private final int serverPort;
  private long lastFailoverCheckTs = System.currentTimeMillis();

  public MultiMetaStoreTimerTask(HiveConf conf, int period, int failoverTO) throws JedisException,
    UnknownHostException {
    super();
    this.conf = conf;
    this.period = period;
    this.failoverTO = failoverTO;

    // write HB message to redis
    jedis = new RedisFactory().getDefaultInstance();
    if (jedis == null) {
      throw new JedisException("Get default jedis instance failed.");
    }

    serverName = InetAddress.getLocalHost().getHostName();
    serverPort = conf.getIntVar(HiveConf.ConfVars.NEWMS_RPC_PORT);
    hbkey = "ms.hb." + serverName + ":" + serverPort;
    Pipeline pi = jedis.pipelined();
    pi.set(hbkey, "1");
    pi.expire(hbkey, period + 5);
    pi.sync();

    // determine the ID of ourself, register ourself
    String self = serverName + ":" + serverPort;
    Long sid;
    if (jedis.zrank("ms.active", self) == null) {
      sid = jedis.incr("ms.next.serverid");
      // FIXME: if two servers start with the same port, fail!
      jedis.zadd("ms.active", sid, self);
    }

    // reget the sid
    sid = jedis.zscore("ms.active", self).longValue();
    DiskManager.serverId = sid;
    LOG.info("Got ServerID " + sid + " for Server " + self);

    // try to register self as master
    tryPromoteAsMaster(jedis, sid);

    jedis = RedisFactory.putInstance(jedis);
  }

  private void changeToMaster(Jedis jedis) {
    if (DiskManager.role == DiskManager.Role.MASTER) {
      return;
    }
    String fid = jedis.get("g_fid");
    if (fid != null) {
      long id = Long.parseLong(fid);
      long oid = id;

      if (conf.getBoolVar(HiveConf.ConfVars.NEWMS_IS_OLD_WITH_NEW)) {
        oid = ObjectStore.getFID();
      }
      synchronized (RawStoreImp.class) {
        if (RawStoreImp.getFid() < id) {
          if (oid >= id) {
            RawStoreImp.setFID(oid);
          } else {
            RawStoreImp.setFID(id);
          }
        }
      }
      LOG.info("ChangeToMaster restore FID to " + id);
      // reload the memory hash map
      if (ThriftRPC.dm.rs instanceof RawStoreImp) {
        CacheStore cs = ((RawStoreImp)ThriftRPC.dm.rs).getCs();
        cs.reloadHashMap();
      }
    }
    DiskManager.role = DiskManager.Role.MASTER;
  }

  private void changeToSlave(Jedis jedis) {
    if (DiskManager.role == DiskManager.Role.SLAVE) {
      return;
    }
    boolean isOldMaster = (DiskManager.role == DiskManager.Role.MASTER);
    DiskManager.role = DiskManager.Role.SLAVE;
    LOG.info("ChangeToSlave for serverId=" + DiskManager.serverId +
        ", isOldMaster=" + isOldMaster);
  }

  private void tryPromoteAsMaster(Jedis jedis, long sid) {
    Long res = jedis.setnx("ms.master", "" + sid);
    String masterId = jedis.get("ms.master");
    if (res == 1) {
      if (masterId == null) {
        LOG.info("Internal error: ms.master missing after we setnx? (master)");
        changeToSlave(jedis);
      } else {
        LOG.info("Hoo, I am the master now: " + masterId);
        changeToMaster(jedis);
        jedis.expire("ms.master", failoverTO);
      }
    } else {
      // sadly, we try to update ms.master failed: we are the master or there are other lucky guy
      if (masterId == null) {
        LOG.info("Internal error: ms.master missing after we setnx? (slave)");
        changeToSlave(jedis);
      } else {
        try {
          Long mid = Long.parseLong(masterId);
          if (mid != DiskManager.serverId) {
            LOG.info("Hoo, I am the slave  now: master is " + masterId);
            changeToSlave(jedis);
          } else {
            LOG.info("I am the master " + masterId);
            changeToMaster(jedis);
          }
        } catch (Exception e) {
          LOG.error(e, e);
        }
      }
    }
  }

  private void tryUpdateMaster(Jedis jedis) {
    String masterId = jedis.get("ms.master");
    if (masterId == null) {
      LOG.info("No master registered yet, ourself into slave mode and wait for lucky guy.");
      changeToSlave(jedis);
    } else {
      LOG.debug("Find master at serverId=" + masterId);
      try {
        Long mid = Long.parseLong(masterId);
        if (mid == DiskManager.serverId) {
          changeToMaster(jedis);
        } else {
          changeToSlave(jedis);
        }
      } catch (Exception e) {
        LOG.error(e, e);
      }
    }
  }

  private void updateMasterTTL(Jedis jedis) {
    String masterId = jedis.get("ms.master");
    if (masterId != null) {
      Long mid = Long.parseLong(masterId);
      if (mid == DiskManager.serverId) {
        jedis.expire("ms.master", failoverTO);
      }
    }
  }

  @Override
  public void run() {

    try {
      if (jedis == null) {
        jedis = new RedisFactory().getDefaultInstance();
      }
      if (jedis == null) {
        LOG.info("Redis cluster down?");
      } else {
        // update HB message
        Pipeline pi = jedis.pipelined();
        pi.set(hbkey, "1");
        pi.expire(hbkey, period + 5);
        pi.sync();

        // try to update self if i am the master
        updateMasterTTL(jedis);

        // try to register self as master, and update DM Role
        if ((System.currentTimeMillis() - lastFailoverCheckTs) / 1000 >= failoverTO) {
          tryPromoteAsMaster(jedis, DiskManager.serverId);
          lastFailoverCheckTs = System.currentTimeMillis();
        } else {
          tryUpdateMaster(jedis);
        }
      }
    } catch (Exception e) {
      jedis = RedisFactory.putBrokenInstance(jedis);
    } finally {
      jedis = RedisFactory.putInstance(jedis);
    }
  }
}
