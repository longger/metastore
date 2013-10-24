package org.apache.hadoop.hive.metastore.metalog;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;

public class MetaStoreClientManager {

  public static HashMap<String, IMetaStoreClient> dbCliMap  = new HashMap<String, IMetaStoreClient>();

  private static Log LOG = LogFactory.getLog(MetaStoreClientManager.class);

  private static IMetaStoreClient topClient;

  private static MetaStoreClientManager instance ;

  private static HiveConf conf = null;

  private final RefreshClientThread refresh = null;

  private MetaStoreClientManager(HiveConf config) {
    conf = config;
    RefreshClientThread refresh = new RefreshClientThread();
    refresh.setDaemon(true);
    refresh.start();
  }

  public static MetaStoreClientManager getInstance(HiveConf conf) {
    if(instance == null){
      instance = new MetaStoreClientManager(conf);
    }
    return instance;
  }


  public static HashMap<String, IMetaStoreClient> getDbCliMap() {
    return dbCliMap;
  }

  public static void setDbCliMap(HashMap<String, IMetaStoreClient> dbCliMap) {
    MetaStoreClientManager.dbCliMap = dbCliMap;
  }

  public static IMetaStoreClient getTopClient() {
    return topClient;
  }

  public static void setTopClient(IMetaStoreClient topClient) {
    MetaStoreClientManager.topClient = topClient;
  }


  public static class RefreshClientThread extends Thread {

    @Override
    public void run() {
      for( String databaseName : dbCliMap.keySet() ){
        IMetaStoreClient client = dbCliMap.get(databaseName);
        if(client == null){
          try {
            Database db = topClient.getDatabase(databaseName);
            HMSHandler.topdcli = new HiveMetaStoreClient(
                db.getParameters().get(HiveConf.ConfVars.METAZOOKEEPERSTOREURIS),
                HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES),
                conf.getIntVar(ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY),
                null);
            // do authentication here!
            String user_name = conf.getVar(HiveConf.ConfVars.HIVE_USER);
            String passwd = conf.getVar(HiveConf.ConfVars.HIVE_USERPWD);
            HMSHandler.topdcli.authentication(user_name, passwd);
            } catch (MetaException me) {
              LOG.info("Connect to "+databaseName+" Attribution failed!");
            } catch (NoSuchObjectException e) {
              LOG.info("User authentication failed: NoSuchUser?");
              LOG.error(e,e);
//              throw new MetaException(e.getMessage());
            } catch (TException e) {
              LOG.info("User authentication failed with unknown TException!");
              LOG.error(e,e);
//              throw new MetaException(e.getMessage());
            }
        }
      }
    }
  }
}
