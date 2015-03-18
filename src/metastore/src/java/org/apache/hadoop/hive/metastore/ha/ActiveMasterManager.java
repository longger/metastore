package org.apache.hadoop.hive.metastore.ha;



import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.metastore.zk.MasterAddressTracker;
import org.apache.hadoop.hive.metastore.zk.ServerName;
import org.apache.hadoop.hive.metastore.zk.ZKUtil;
import org.apache.hadoop.hive.metastore.zk.ZNodeClearer;
import org.apache.hadoop.hive.metastore.zk.ZooKeeperListener;
import org.apache.hadoop.hive.metastore.zk.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;


/**
* Handles everything on master-side related to master election.
*
* <p>Listens and responds to ZooKeeper notifications on the master znode,
* both <code>nodeCreated</code> and <code>nodeDeleted</code>.
*
* <p>Contains blocking methods which will hold up backup masters, waiting
* for the active master to fail.
*
* <p>This class is instantiated in the HMaster constructor and the method
* #blockUntilBecomingActiveMaster() is called to wait until becoming
* the active master of the cluster.
*/
@InterfaceAudience.Private
public class ActiveMasterManager extends ZooKeeperListener {
 private static final Log LOG = LogFactory.getLog(ActiveMasterManager.class);

 final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);
 final AtomicBoolean clusterShutDown = new AtomicBoolean(false);

 private final ServerName sn;
// private final Server master;
 private final MetaMaster master;

 /**
  * @param watcher
  * @param sn ServerName
  * @param master In an instance of a Master.
  */
 ActiveMasterManager(ZooKeeperWatcher watcher, ServerName sn, MetaMaster master) {
   super(watcher);
   this.sn = sn;
   this.master = master;
 }

 @Override
 public void nodeCreated(String path) {
   handle(path);
 }

 @Override
 public void nodeDeleted(String path) {

   // We need to keep track of the cluster's shutdown status while
   // we wait on the current master. We consider that, if the cluster
   // was already in a "shutdown" state when we started, that this master
   // is part of a new cluster that was started shortly after the old cluster
   // shut down, so that state is now irrelevant. This means that the shutdown
   // state must be set while we wait on the active master in order
   // to shutdown this master. See HBASE-8519.
   if(path.equals(watcher.baseZNode) && !master.isStopped()) {
     clusterShutDown.set(true);
   }

   handle(path);
 }

 void handle(final String path) {
   if (path.equals(watcher.getMasterAddressZNode()) && !master.isStopped()) {
     handleMasterNodeChange();
   }
 }

 /**
  * Handle a change in the master node.  Doesn't matter whether this was called
  * from a nodeCreated or nodeDeleted event because there are no guarantees
  * that the current state of the master node matches the event at the time of
  * our next ZK request.
  *
  * <p>Uses the watchAndCheckExists method which watches the master address node
  * regardless of whether it exists or not.  If it does exist (there is an
  * active master), it returns true.  Otherwise it returns false.
  *
  * <p>A watcher is set which guarantees that this method will get called again if
  * there is another change in the master node.
  */
 private void handleMasterNodeChange() {
   // Watch the node and check if it exists.
   try {
     synchronized(clusterHasActiveMaster) {
       if (ZKUtil.watchAndCheckExists(watcher, watcher.getMasterAddressZNode())) {
         // A master node exists, there is an active master
         LOG.debug("A master is now available");
         clusterHasActiveMaster.set(true);
       } else {
         // Node is no longer there, cluster does not have an active master
         LOG.debug("No master available. Notifying waiting threads");
         clusterHasActiveMaster.set(false);
         // Notify any thread waiting to become the active master
         clusterHasActiveMaster.notifyAll();
       }
     }
   } catch (KeeperException ke) {
     master.abort("Received an unexpected KeeperException, aborting", ke);
   }
 }

 /**
  * Block until becoming the active master.
  *
  * Method blocks until there is not another active master and our attempt
  * to become the new active master is successful.
  *
  * This also makes sure that we are watching the master znode so will be
  * notified if another master dies.
  * @param startupStatus
  * @return True if no issue becoming active master else false if another
  * master was running or if some other problem (zookeeper, stop flag has been
  * set on this Master)
  */
 boolean blockUntilBecomingActiveMaster(MonitoredTask startupStatus) {
   while (true) {
     startupStatus.setStatus("Trying to register in ZK as active master");
     // Try to become the active master, watch if there is another master.
     // Write out our ServerName as versioned bytes.
     try {
       String backupZNode =this.watcher.backupMasterAddressesZNode;
//           ZKUtil.joinZNode(this.watcher.backupMasterAddressesZNode, new String(sn.toByteArray(sn)));
       if (MasterAddressTracker.setMasterAddress(this.watcher,
           this.watcher.getMasterAddressZNode(), this.sn)) {

         // If we were a backup master before, delete our ZNode from the backup
         // master directory since we are the active now)
         if (ZKUtil.checkExists(this.watcher, backupZNode) != -1) {
           LOG.info("Deleting ZNode for " + backupZNode + " from backup master directory");
           ZKUtil.deleteNodeFailSilent(this.watcher, backupZNode);
         }
         // Save the znode in a file, this will allow to check if we crash in the launch scripts
         ZNodeClearer.writeMyEphemeralNodeOnDisk(new String(sn.toByteArray(sn)));

         // We are the master, return
         startupStatus.setStatus("Successfully registered as active master.");
         this.clusterHasActiveMaster.set(true);
         LOG.info("Registered Active Master=" + this.sn);
         return true;
       }

       // There is another active master running elsewhere or this is a restart
       // and the master ephemeral node has not expired yet.
       this.clusterHasActiveMaster.set(true);

       /*
       * Add a ZNode for ourselves in the backup master directory since we are
       * not the active master.
       *
       * If we become the active master later, ActiveMasterManager will delete
       * this node explicitly.  If we crash before then, ZooKeeper will delete
       * this node for us since it is ephemeral.
       */
       LOG.info("Adding ZNode for " + backupZNode + " in backup master directory");
       MasterAddressTracker.setMasterAddress(this.watcher, backupZNode, this.sn);

       String msg;
       byte[] bytes =
         ZKUtil.getDataAndWatch(this.watcher, this.watcher.getMasterAddressZNode());
       if (bytes == null) {
         msg = ("A master was detected, but went down before its address " +
           "could be read.  Attempting to become the next active master");
       } else {
         ServerName currentMaster;
         currentMaster = ServerName.parseFrom(bytes);

         if (ServerName.isSameHostnameAndPort(currentMaster, this.sn)) {
           msg = ("Current master has this master's address, " +
             currentMaster + "; master was restarted? Deleting node.");
           // Hurry along the expiration of the znode.
           ZKUtil.deleteNode(this.watcher, this.watcher.getMasterAddressZNode());

           // We may have failed to delete the znode at the previous step, but
           //  we delete the file anyway: a second attempt to delete the znode is likely to fail again.
           ZNodeClearer.deleteMyEphemeralNodeOnDisk();
         } else {
           msg = "Another master is the active master, " + currentMaster +
             "; waiting to become the next active master";
         }
       }
       LOG.info(msg);
       startupStatus.setStatus(msg);
     } catch (KeeperException ke) {
       master.abort("Received an unexpected KeeperException, aborting", ke);
       return false;
     }
     synchronized (this.clusterHasActiveMaster) {

       LOG.info("master1:"+this.clusterHasActiveMaster.get() +"======stop:"+ !this.master.isStopped());
       while (this.clusterHasActiveMaster.get() && !this.master.isStopped()) {
         try {
           LOG.info("before ##======wait");
           this.clusterHasActiveMaster.wait();
           LOG.info("after  ##======wait");
         } catch (InterruptedException e) {
           // We expect to be interrupted when a master dies,
           //  will fall out if so
           LOG.debug("Interrupted waiting for master to die", e);
         }
         catch(Exception e) {
           LOG.error(e, e);
         }
       }
       LOG.info("master2:"+this.clusterHasActiveMaster.get() +"======stop:"+ !this.master.isStopped());
       if (clusterShutDown.get()) {
         this.master.stop(
           "Cluster went down before this master became active");
       }
       if (this.master.isStopped()) {
         return false;
       }
       // there is no active master so we can try to become active master again
     }
   }
 }

 /**
  * @return True if cluster has an active master.
  */
 public boolean isActiveMaster() {
   try {
     if (ZKUtil.checkExists(watcher, watcher.getMasterAddressZNode()) >= 0) {
       return true;
     }
   }
   catch (KeeperException ke) {
     LOG.info("Received an unexpected KeeperException when checking " +
         "isActiveMaster : "+ ke);
   }
   return false;
 }

 public void stop() {
   try {
     // If our address is in ZK, delete it on our way out
     ServerName activeMaster = null;
     try {
       activeMaster = MasterAddressTracker.getMasterAddress(this.watcher);
     } catch (IOException e) {
       LOG.warn("Failed get of master address: " + e.toString());
     }
     if (activeMaster != null &&  activeMaster.equals(this.sn)) {
       ZKUtil.deleteNode(watcher, watcher.getMasterAddressZNode());
       // We may have failed to delete the znode at the previous step, but
       //  we delete the file anyway: a second attempt to delete the znode is likely to fail again.
       ZNodeClearer.deleteMyEphemeralNodeOnDisk();
     }
   } catch (KeeperException e) {
     LOG.error(this.watcher.prefix("Error deleting our own master address node"), e);
   }
 }
}
