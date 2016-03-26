package org.apache.hadoop.hive.metastore.zk;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class MetaConstants {

  /** Cluster is standalone or pseudo-distributed */
  public static final boolean CLUSTER_IS_LOCAL = false;

  /** Cluster is fully-distributed */
  public static final boolean CLUSTER_IS_DISTRIBUTED = true;

  /** Default value for cluster distributed mode */
  public static final boolean DEFAULT_CLUSTER_DISTRIBUTED = CLUSTER_IS_LOCAL;

  /** The delay when re-trying a socket operation in a loop (HBASE-4712) */
  public static final int SOCKET_RETRY_WAIT_MS = 200;

  /** Host name of the local machine */
  public static final String LOCALHOST = "localhost";

  /** Cluster is in distributed mode or not */
  public static final String CLUSTER_DISTRIBUTED = "hbase.cluster.distributed";

  public static final String HBASE_CONFIG_READ_ZOOKEEPER_CONFIG =
      "meta.config.read.zookeeper.config";
  /** Parameter name for port master listens on. */
  public static final String MASTER_PORT = "meta.master.port";

  /** default port that the master listens on */
  public static final int DEFAULT_MASTER_PORT = 60000;

  /** default port for master web api */
  public static final int DEFAULT_MASTER_INFOPORT = 60010;

  /** Configuration key for master web API port */
  public static final String MASTER_INFO_PORT = "hbase.master.info.port";

  /** Parameter name for the master type being backup (waits for primary to go inactive). */
  public static final String MASTER_TYPE_BACKUP = "hbase.master.backup";

  /** by default every master is a possible primary master unless the conf explicitly overrides it */
  public static final boolean DEFAULT_MASTER_TYPE_BACKUP = false;

  /** Name of ZooKeeper quorum configuration parameter. */
  public static final String ZOOKEEPER_QUORUM = "meta.zookeeper.quorum";

  /** Name of ZooKeeper config file in conf/ directory. */
  public static final String ZOOKEEPER_CONFIG_NAME = "zoo.cfg";

  /** Common prefix of ZooKeeper configuration properties */
  public static final String ZK_CFG_PROPERTY_PREFIX =
      "meta.zookeeper.property.";

  public static final int ZK_CFG_PROPERTY_PREFIX_LEN =
      ZK_CFG_PROPERTY_PREFIX.length();

  /**
   * The ZK client port key in the ZK properties map. The name reflects the
   * fact that this is not an HBase configuration key.
   */
  public static final String CLIENT_PORT_STR = "clientPort";

  /** Parameter name for the client port that the zookeeper listens on */
  public static final String ZOOKEEPER_CLIENT_PORT =
      ZK_CFG_PROPERTY_PREFIX + CLIENT_PORT_STR;

  /** Default client port that the zookeeper listens on */
  public static final int DEFAULT_ZOOKEPER_CLIENT_PORT = 2181;

  public static final int DEFAULT_ZOOKEPER_CHECK_RETRY = 5;

  /** Parameter name for the wait time for the recoverable zookeeper */
  public static final String ZOOKEEPER_RECOVERABLE_WAITTIME = "hbase.zookeeper.recoverable.waittime";

  /** Default wait time for the recoverable zookeeper */
  public static final long DEFAULT_ZOOKEPER_RECOVERABLE_WAITIME = 10000;

  /** Parameter name for the root dir in ZK for this cluster */
  public static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/iie-metastore";
  public static final String DEFAULT_ZOOKEEPER_ZNODE_LOG = "/iie-metastore-log";

  public static final String DEFAULT_TOP_DATABASE = "topdb";
  /**
   * Parameter name for the limit on concurrent client-side zookeeper
   * connections
   */
  public static final String ZOOKEEPER_MAX_CLIENT_CNXNS =
      ZK_CFG_PROPERTY_PREFIX + "maxClientCnxns";

  /** Parameter name for the ZK data directory */
  public static final String ZOOKEEPER_DATA_DIR =
      ZK_CFG_PROPERTY_PREFIX + "dataDir";

  /** Default limit on concurrent client-side zookeeper connections */
  public static final int DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS = 300;

  /** Configuration key for ZooKeeper session timeout */
  public static final String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout";

  /** Default value for ZooKeeper session timeout */
  public static final int DEFAULT_ZK_SESSION_TIMEOUT = 60 * 1000;

  /** Configuration key for whether to use ZK.multi */
  public static final String ZOOKEEPER_USEMULTI = "hbase.zookeeper.useMulti";



}
