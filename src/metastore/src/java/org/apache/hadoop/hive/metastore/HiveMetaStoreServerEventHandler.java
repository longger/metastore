package org.apache.hadoop.hive.metastore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.DiskManager.DMProfile;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler.MSSessionState;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class HiveMetaStoreServerEventHandler implements TServerEventHandler {
  public static final Log LOG = LogFactory.getLog(HiveMetaStoreServerEventHandler.class);
  public static final Map<Long, HiveMetaStoreServerContext> sessions = new ConcurrentHashMap<Long, HiveMetaStoreServerContext>();
  public static final Map<String, AtomicLong> perIPConns = new ConcurrentHashMap<String, AtomicLong>();

  public static HiveMetaStoreServerContext getServerContext(Long sessionId) {
    return sessions.get(sessionId);
  }

  @Override
  public void preServe() {
    // TODO Auto-generated method stub

  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    if (input.getTransport() instanceof TSocket) {
      String ip = ((TSocket)input.getTransport()).getSocket().getInetAddress().toString();
      AtomicLong conns = perIPConns.get(ip);

      if (conns == null) {
        synchronized (perIPConns) {
          if (perIPConns.get(ip) == null) {
            conns = new AtomicLong(1);
            perIPConns.put(ip, conns);
          } else {
            perIPConns.get(ip).incrementAndGet();
          }
        }
      } else {
        conns.incrementAndGet();
      }
    }
    HiveMetaStoreServerContext sc = new HiveMetaStoreServerContext();
    sessions.put(sc.getSessionId(), sc);
    DMProfile.newConn.incrementAndGet();
    LOG.info("Receive a new connection, set its sessionId to " + sc.getSessionId());
    return sc;
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    DMProfile.delConn.incrementAndGet();
    sessions.remove(serverContext);
    if (input.getTransport() instanceof TSocket) {
      String ip = ((TSocket)input.getTransport()).getSocket().getInetAddress().toString();
      synchronized (perIPConns) {
        AtomicLong conns = perIPConns.get(ip);
        if (conns != null) {
          conns.decrementAndGet();
        }
        perIPConns.put(ip, conns);
      }
    }
  }

  @Override
  public void processContext(ServerContext serverContext, TTransport inputTransport,
      TTransport outputTransport) {
    HiveMetaStoreServerContext sc = (HiveMetaStoreServerContext)serverContext;
    DMProfile.query.incrementAndGet();
    // set the session ID to this thread?
    MSSessionState msss = new MSSessionState();
    msss.setSessionid(sc.getSessionId());
  }

}
