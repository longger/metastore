package org.apache.hadoop.hive.metastore.ha;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.zk.MasterAddressTracker;
import org.apache.hadoop.hive.metastore.zk.MetaConstants;
import org.apache.hadoop.hive.metastore.zk.ServerName;
import org.apache.hadoop.hive.metastore.zk.ZooKeeperListener;
import org.apache.hadoop.hive.metastore.zk.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

/**
* HMaster is the "master server" for HBase. An HBase cluster has one active
* master.  If many masters are started, all compete.  Whichever wins goes on to
* run the cluster.  All others park themselves in their constructor until
* master or cluster shutdown or until the active master loses its lease in
* zookeeper.  Thereafter, all running master jostle to take over master role.
*
* <p>The Master can be asked shutdown the cluster. See {@link #shutdown()}.  In
* this case it will tell all regionservers to go down and then wait on them
* all reporting in that they are down.  This master will then shut itself down.
*
* <p>You can also shutdown just this master.  Call {@link #stopMaster()}.
*
* @see Watcher
*/
@InterfaceAudience.Private
@SuppressWarnings("deprecation")



/**
 * meta.zookeeper.quorum=127.0.0.1
 * meta.zookeeper.property.clientPort=3181
 *
 * MetaMaster.
 *
 */
public class MetaMaster  extends HasThread implements Abortable{
 private static final Log LOG = LogFactory.getLog(HMaster.class.getName());

 // MASTER is name of the webapp and the attribute name used stuffing this
 //instance into web context.
 public static final String MASTER = "master";

 // The configuration for the Master
 private final HiveConf conf;
 // server for the web ui
// private InfoServer infoServer;

 // Our zk client.
 private final ZooKeeperWatcher zooKeeper;
 // Manager and zk listener for master election
 private ActiveMasterManager activeMasterManager;
 // master address manager and watcher
 private MasterAddressTracker masterAddressManager;

 // This flag is for stopping this Master instance.  Its set when we are
 // stopping or aborting
 private volatile boolean stopped = false;
 // Set on abort -- usually failure of our zk session.
 private volatile boolean abort = false;
 // flag set after we become the active master (used for testing)
 private volatile boolean isActiveMaster = false;

 // flag set after we complete initialization once active,
 // it is not private since it's used in unit tests
 volatile boolean initialized = false;

 // flag set after we complete assignMeta.
 private volatile boolean serverShutdownHandlerEnabled = false;

 private final ServerName serverName;

 // Time stamps for when a hmaster was started and when it became active
 private long masterStartTime;
 private long masterActiveTime;


 /** The following is used in master recovery scenario to re-register listeners */
 private List<ZooKeeperListener> registeredZKListenersBeforeRecovery;


 /**
  * Initializes the HMaster. The steps are as follows:
  * <p>
  * <ol>
  * <li>Initialize HMaster RPC and address
  * <li>Connect to ZooKeeper.
  * </ol>
  * <p>
  * Remaining steps of initialization occur in {@link #run()} so that they
  * run in their own thread rather than within the context of the constructor.
  * @throws InterruptedException
  */
 public MetaMaster(final HiveConf conf)
 throws IOException, KeeperException, InterruptedException {
   this.conf = new HiveConf(conf);

   String local_attribution_uri = conf.getVar(ConfVars.METASTORELOCALURIS);
   this.serverName = new ServerName(local_attribution_uri);

   this.zooKeeper = new ZooKeeperWatcher(conf,null, MASTER , this, true);

 }



 /**
  * Stall startup if we are designated a backup master; i.e. we want someone
  * else to become the master before proceeding.
  * @param c configuration
  * @param amm
  * @throws InterruptedException
  */
 private static void stallIfBackupMaster(final Configuration c,
     final ActiveMasterManager amm)
 throws InterruptedException {
   // If we're a backup master, stall until a primary to writes his address
   if (!c.getBoolean(MetaConstants.MASTER_TYPE_BACKUP,
     MetaConstants.DEFAULT_MASTER_TYPE_BACKUP)) {
     return;
   }
   LOG.debug("HMaster started in backup mode.  " +
     "Stalling until master znode is written.");
   // This will only be a minute or so while the cluster starts up,
   // so don't worry about setting watches on the parent znode
   while (!amm.isActiveMaster()) {
     LOG.debug("Waiting for master address ZNode to be written " +
       "(Also watching cluster state node)");
     Thread.sleep(
       c.getInt(MetaConstants.ZK_SESSION_TIMEOUT, MetaConstants.DEFAULT_ZK_SESSION_TIMEOUT));
   }

 }


 /**
  * Main processing loop for the HMaster.
  * <ol>
  * <li>Block until becoming active master
  * <li>Finish initialization via finishInitialization(MonitoredTask)
  * <li>Enter loop until we are stopped
  * <li>Stop services and perform cleanup once stopped
  * </ol>
  */
 @Override
 public void run() {
   MonitoredTask startupStatus =
     TaskMonitor.get().createStatus("Master startup");
   startupStatus.setDescription("Master startup");
   masterStartTime = System.currentTimeMillis();
   try {
     this.registeredZKListenersBeforeRecovery = this.zooKeeper.getListeners();
     this.masterAddressManager = new MasterAddressTracker(getZooKeeperWatcher(), this);
     this.masterAddressManager.start();

     // Put up info server.
//     int port = this.conf.getInt("hbase.master.info.port", 60010);
//     if (port >= 0) {
//       String a = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
//       this.infoServer = new InfoServer(MASTER, a, port, false, this.conf);
//       this.infoServer.addServlet("status", "/master-status", MasterStatusServlet.class);
//       this.infoServer.addServlet("dump", "/dump", MasterDumpServlet.class);
//       this.infoServer.setAttribute(MASTER, this);
//       this.infoServer.start();
//     }

     /*
      * Block on becoming the active master.
      *
      * We race with other masters to write our address into ZooKeeper.  If we
      * succeed, we are the primary/active master and finish initialization.
      *
      * If we do not succeed, there is another active master and we should
      * now wait until it dies to try and become the next active master.  If we
      * do not succeed on our first attempt, this is no longer a cluster startup.
      */
     becomeActiveMaster(startupStatus);

     // We are either the active master or we were asked to shutdown
     if (!this.stopped) {
       finishInitialization(startupStatus, false);

     }
   } catch (Throwable t) {
     // HBASE-5680: Likely hadoop23 vs hadoop 20.x/1.x incompatibility
     if (t instanceof NoClassDefFoundError &&
         t.getMessage().contains("org/apache/hadoop/hdfs/protocol/FSConstants$SafeModeAction")) {
         // improved error message for this special case
         abort("HBase is having a problem with its Hadoop jars.  You may need to "
             + "recompile HBase against Hadoop version "
             +  org.apache.hadoop.util.VersionInfo.getVersion()
             + " or change your hadoop jars to start properly", t);
     } else {
       abort("Unhandled exception. Starting shutdown.", t);
     }
   } finally {
     startupStatus.cleanup();

     // Stop services started for both backup and active masters
     if (this.activeMasterManager != null) {
      this.activeMasterManager.stop();
    }

     this.zooKeeper.close();
   }
   LOG.info("HMaster main thread exiting");
 }

 /**
  * Try becoming active master.
  * @param startupStatus
  * @return True if we could successfully become the active master.
  * @throws InterruptedException
  */
 private boolean becomeActiveMaster(MonitoredTask startupStatus)
 throws InterruptedException {
   // TODO: This is wrong!!!! Should have new servername if we restart ourselves,
   // if we come back to life.
   this.activeMasterManager = new ActiveMasterManager(zooKeeper, this.serverName,
       this);
   this.zooKeeper.registerListener(activeMasterManager);
   stallIfBackupMaster(this.conf, this.activeMasterManager);

   return this.activeMasterManager.blockUntilBecomingActiveMaster(startupStatus);
 }

 /**
  * Initialize all ZK based system trackers.
  * @throws IOException
  * @throws InterruptedException
  */
 void initializeZKBasedSystemTrackers() throws IOException,
     InterruptedException, KeeperException {
   this.activeMasterManager = new ActiveMasterManager(this.zooKeeper, this.serverName, this);
   this.zooKeeper.registerListener(activeMasterManager);

   LOG.info("Server active/primary master=" + this.serverName +
       ", sessionid=0x" +
       Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()));
 }


 /**
  * Finish initialization of HMaster after becoming the primary master.
  *
  * <ol>
  * <li>Initialize master components - file system manager, server manager,
  *     assignment manager, region server tracker, catalog tracker, etc</li>
  * <li>Start necessary service threads - rpc server, info server,
  *     executor services, etc</li>
  * <li>Set cluster as UP in ZooKeeper</li>
  * <li>Wait for RegionServers to check-in</li>
  * <li>Split logs and perform data recovery, if necessary</li>
  * <li>Ensure assignment of meta regions<li>
  * <li>Handle either fresh cluster start or master failover</li>
  * </ol>
  *
  * @param masterRecovery
  *
  * @throws IOException
  * @throws InterruptedException
  * @throws KeeperException
  */
 private void finishInitialization(MonitoredTask status, boolean masterRecovery)
 throws IOException, InterruptedException, KeeperException {

   isActiveMaster = true;

   /*
    * We are active master now... go initialize components we need to run.
    * Note, there may be dross in zk from previous runs; it'll get addressed
    * below after we determine if cluster startup or failover.
    */

   status.setStatus("Initializing Master file system");

   this.masterActiveTime = System.currentTimeMillis();


   status.setStatus("Initializing ZK system trackers");
   initializeZKBasedSystemTrackers();



   status.markComplete("Initialization successful");
   LOG.info("Master has completed initialization");
   initialized = true;

 }



 public Configuration getConfiguration() {
   return this.conf;
 }

 /**
  * Get the ZK wrapper object - needed by master_jsp.java
  * @return the zookeeper wrapper
  */
 public ZooKeeperWatcher getZooKeeperWatcher() {
   return this.zooKeeper;
 }

 public ActiveMasterManager getActiveMasterManager() {
   return this.activeMasterManager;
 }

 public MasterAddressTracker getMasterAddressManager() {
   return this.masterAddressManager;
 }



 public boolean isMasterRunning() {
   return !isStopped();
 }

 public void abort(final String msg, final Throwable t) {

   if (abortNow(msg, t)) {
     if (t != null) {
      LOG.fatal(msg, t);
    } else {
      LOG.fatal(msg);
    }
     this.abort = true;
     stop("Aborting");
   }
 }

 /**
  * We do the following in a different thread.  If it is not completed
  * in time, we will time it out and assume it is not easy to recover.
  *
  * 1. Create a new ZK session. (since our current one is expired)
  * 2. Try to become a primary master again
  * 3. Initialize all ZK based system trackers.
  * 4. Assign meta. (they are already assigned, but we need to update our
  * internal memory state to reflect it)
  * 5. Process any RIT if any during the process of our recovery.
  *
  * @return True if we could successfully recover from ZK session expiry.
  * @throws InterruptedException
  * @throws IOException
  * @throws KeeperException
  * @throws ExecutionException
  */
 private boolean tryRecoveringExpiredZKSession() throws InterruptedException,
     IOException, KeeperException, ExecutionException {

   this.zooKeeper.unregisterAllListeners();
   // add back listeners which were registered before master initialization
   // because they won't be added back in below Master re-initialization code
   if (this.registeredZKListenersBeforeRecovery != null) {
     for (ZooKeeperListener curListener : this.registeredZKListenersBeforeRecovery) {
       this.zooKeeper.registerListener(curListener);
     }
   }

   this.zooKeeper.reconnectAfterExpiration();

   Callable<Boolean> callable = new Callable<Boolean> () {
     public Boolean call() throws InterruptedException,
         IOException, KeeperException {
       MonitoredTask status =
         TaskMonitor.get().createStatus("Recovering expired ZK session");
       try {
         if (!becomeActiveMaster(status)) {
           return Boolean.FALSE;
         }
         serverShutdownHandlerEnabled = false;
         initialized = false;
         finishInitialization(status, true);
         return !stopped;
       } finally {
         status.cleanup();
       }
     }
   };

   long timeout =
     conf.getLong("hbase.master.zksession.recover.timeout", 300000);
   java.util.concurrent.ExecutorService executor =
     Executors.newSingleThreadExecutor();
   Future<Boolean> result = executor.submit(callable);
   executor.shutdown();
   if (executor.awaitTermination(timeout, TimeUnit.MILLISECONDS)
       && result.isDone()) {
     Boolean recovered = result.get();
     if (recovered != null) {
       return recovered.booleanValue();
     }
   }
   executor.shutdownNow();
   return false;
 }

 /**
  * Check to see if the current trigger for abort is due to ZooKeeper session
  * expiry, and If yes, whether we can recover from ZK session expiry.
  *
  * @param msg Original abort message
  * @param t   The cause for current abort request
  * @return true if we should proceed with abort operation, false other wise.
  */
 private boolean abortNow(final String msg, final Throwable t) {
   if (!this.isActiveMaster || this.stopped) {
     return true;
   }
   if (t != null && t instanceof KeeperException.SessionExpiredException) {
     try {
       LOG.info("Primary Master trying to recover from ZooKeeper session " +
           "expiry.");
       return !tryRecoveringExpiredZKSession();
     } catch (Throwable newT) {
       LOG.error("Primary master encountered unexpected exception while " +
           "trying to recover from ZooKeeper session" +
           " expiry. Proceeding with server abort.", newT);
     }
   }
   return true;
 }

 public ZooKeeperWatcher getZooKeeper() {
   return zooKeeper;
 }


 public ServerName getServerName() {
   return this.serverName;
 }



 public void stop(final String why) {
   LOG.info(why);
   this.stopped = true;

   // If we are a backup master, we need to interrupt wait
   if (this.activeMasterManager != null) {
     synchronized (this.activeMasterManager.clusterHasActiveMaster) {
       this.activeMasterManager.clusterHasActiveMaster.notifyAll();
     }
   }

 }

 public boolean isStopped() {
   return this.stopped;
 }

 public boolean isAborted() {
   return this.abort;
 }

 void checkInitialized() throws PleaseHoldException {
   if (!this.initialized) {
     throw new PleaseHoldException("Master is initializing");
   }
 }

 /**
  * Report whether this master is currently the active master or not.
  * If not active master, we are parked on ZK waiting to become active.
  *
  * This method is used for testing.
  *
  * @return true if active master, false if not.
  */
 public boolean isActiveMaster() {
   return isActiveMaster;
 }

 /**
  * Report whether this master has completed with its initialization and is
  * ready.  If ready, the master is also the active master.  A standby master
  * is never ready.
  *
  * This method is used for testing.
  *
  * @return true if master is ready to go, false if not.
  */
 public boolean isInitialized() {
   return initialized;
 }

 /**
  * ServerShutdownHandlerEnabled is set false before completing
  * assignMeta to prevent processing of ServerShutdownHandler.
  * @return true if assignMeta has completed;
  */
 public boolean isServerShutdownHandlerEnabled() {
   return this.serverShutdownHandlerEnabled;
 }



 /**
  * Utility for constructing an instance of the passed HMaster class.
  * @param masterClass
  * @param conf
  * @return HMaster instance.
  */
 public static HMaster constructMaster(Class<? extends HMaster> masterClass,
     final Configuration conf)  {
   try {
     Constructor<? extends HMaster> c =
       masterClass.getConstructor(Configuration.class);
     return c.newInstance(conf);
   } catch (InvocationTargetException ite) {
     Throwable target = ite.getTargetException() != null?
       ite.getTargetException(): ite;
     if (target.getCause() != null) {
      target = target.getCause();
    }
     throw new RuntimeException("Failed construction of Master: " +
       masterClass.toString(), target);
   } catch (Exception e) {
     throw new RuntimeException("Failed construction of Master: " +
       masterClass.toString() + ((e.getCause() != null)?
         e.getCause().getMessage(): ""), e);
   }
 }




}

