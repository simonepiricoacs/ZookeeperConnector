package it.water.connectors.zookeeper.cluster;

import it.water.connectors.zookeeper.api.ZookeeperClient;
import it.water.connectors.zookeeper.api.ZookeeperConnectorSystemApi;
import it.water.connectors.zookeeper.model.ZKData;
import it.water.core.api.interceptors.OnActivate;
import it.water.core.api.interceptors.OnDeactivate;
import it.water.core.api.registry.ComponentRegistry;
import it.water.core.api.service.cluster.*;
import it.water.core.interceptors.annotations.FrameworkComponent;
import it.water.core.interceptors.annotations.Inject;
import lombok.Setter;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheBuilder;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * @Author Aristide Cittadino
 * Cluster Coordinator Client implementation made with Zookeeper.
 * This class exploits apache curator recipes to perform cluster coordination.
 */
@FrameworkComponent
public class ZKClusterCoordinatorClient implements ClusterCoordinatorClient, ClusterObserver, ZookeeperClient {
    private Logger logger = LoggerFactory.getLogger(ZKClusterCoordinatorClient.class);

    @Inject
    @Setter
    private ClusterNodeOptions clusterNodeOptions;
    @Inject
    @Setter
    private ZookeeperConnectorSystemApi zookeeperConnectorSystemApi;
    private Set<ClusterObserver> clusterObservers;
    private CuratorCache clusterCuratorCache;
    private CuratorCacheListener clusterCacheListener;
    private Map<String, ClusterNodeInfo> peers;
    private boolean started;
    /**
     * The thread performing the current (asynchronous) cluster registration, kept so that an
     * unregister can wait for it instead of racing it. Volatile: written by the caller of
     * {@code registerToCluster}, read by whoever shuts the node down.
     */
    private volatile Thread registrationThread;

    /**
     * How long an unregister waits for an in-flight registration. Generous for the normal case (node
     * created in milliseconds) and short enough not to stall a shutdown.
     */
    private static final long REGISTRATION_JOIN_TIMEOUT_MS = 5_000L;

    public ZKClusterCoordinatorClient() {
        this.peers = new HashMap<>();
        this.clusterObservers = new HashSet<>();
    }

    @OnActivate
    public void onActivate(ComponentRegistry componentRegistry) {
        this.zookeeperConnectorSystemApi = componentRegistry.findComponent(ZookeeperConnectorSystemApi.class, null);
        //registering to be notified when everything is set up
        this.zookeeperConnectorSystemApi.addZookeeperClient(this);
    }

    @Override
    public void onConnectionOpened(ZookeeperConnectorSystemApi zookeeperConnectorSystemApi, ClusterNodeOptions clusterNodeOptions) {
        //do nothing since cluster registration is done by the framework lifecycle
        this.clusterNodeOptions = clusterNodeOptions;
        this.zookeeperConnectorSystemApi = zookeeperConnectorSystemApi;
    }

    @Override
    public void onConnectionClosed() {
        //the peer node must be deleted while the connection is still usable: closing the cache first
        //left the delete to fail silently, so an orderly shutdown kept the node visible to the other
        //peers until its ZooKeeper session expired (session.timeout.ms, 30s by default) - long enough
        //for the cluster to keep routing work to a node that is already gone
        this.unregisterToCluster();
        this.started = false;
        this.unsubscribeToClusterEvents(this);
        //Disconnects from zookeeper
        this.clusterCuratorCache.listenable().removeListener(clusterCacheListener);
        this.clusterCuratorCache.close();
    }

    @Override
    public void awaitConnection() throws InterruptedException {
        this.zookeeperConnectorSystemApi.awaitRegistration();
        await().atMost(30, SECONDS).until(this::isStarted);
    }

    @Override
    public boolean isStarted() {
        return this.started;
    }

    @OnDeactivate
    public void onDeactivate() {
        this.onConnectionClosed();
    }

    @Override
    public void subscribeToClusterEvents(ClusterObserver clusterObserver) {
        this.clusterObservers.add(clusterObserver);
    }

    @Override
    public void unsubscribeToClusterEvents(ClusterObserver clusterObserver) {
        this.clusterObservers.remove(clusterObserver);
    }

    @Override
    public void registerForLeadership(String leadershipServiceKey) {
        doClusterOperation(() -> this.zookeeperConnectorSystemApi.registerLeadershipComponent(leadershipServiceKey));
    }

    @Override
    public void unregisterForLeadership(String leadershipServiceKey) {
        doClusterOperation(() -> this.zookeeperConnectorSystemApi.unregisterLeadershipComponent(leadershipServiceKey));
    }

    @Override
    public boolean registerToCluster() {
        //await for connection ready
        Thread thread = new Thread(() -> {
            await().atMost(30, SECONDS).until(() -> this.zookeeperConnectorSystemApi.getZookeeperCuratorClient() != null && this.zookeeperConnectorSystemApi.getZookeeperCuratorClient().getState().equals(CuratorFrameworkState.STARTED));
            if (this.zookeeperConnectorSystemApi.getZookeeperCuratorClient().getState().equals(CuratorFrameworkState.STARTED)) {
                try {
                    ZKData zkData = new ZKData();
                    zkData.addParam(ClusterNodeOptions.NODE_ID_FIELD_NAME, clusterNodeOptions.getNodeId().getBytes());
                    zkData.addParam(ClusterNodeOptions.HOST_FIELD_NAME, clusterNodeOptions.getHost().getBytes());
                    zkData.addParam(ClusterNodeOptions.IP_FIELD_NAME, clusterNodeOptions.getIp().getBytes());
                    zkData.addParam(ClusterNodeOptions.LAYER_FIELD_NAME, clusterNodeOptions.getLayer().getBytes());
                    zkData.addParam(ClusterNodeOptions.CLUSTER_MODE_FIELD_NAME, "true".getBytes());
                    zkData.addParam(ClusterNodeOptions.IP_REGISTRATION_FIELD_NAME, String.valueOf(clusterNodeOptions.useIpInClusterRegistration()).getBytes());
                    if (logger.isDebugEnabled())
                        logger.debug("Registering Container info on zookeeper with nodeId: {} layer: {} data: \n {}", this.clusterNodeOptions.getNodeId(), this.clusterNodeOptions.getLayer(), new String(zkData.getBytes()));
                    this.zookeeperConnectorSystemApi.createEphemeral(zookeeperConnectorSystemApi.getPeerPath(clusterNodeOptions.getNodeId()), zkData.getBytes(), true);
                    this.started = true;
                    this.subscribeToClusterEvents(this);
                    this.registerClusterEventListener();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            } else {
                logger.warn("Cluster is not started yet! please check your configuration!");
            }
        });
        this.registrationThread = thread;
        thread.start();
        return true;
    }

    @Override
    public boolean unregisterToCluster() {
        //a registration in flight must be allowed to finish first: it creates the ephemeral node from
        //its own thread, so deleting before it completes deletes nothing and the node is created right
        //after - surviving until the session expires. Waiting here makes the two ordered.
        awaitPendingRegistration();
        return Boolean.TRUE.equals(doClusterOperation(() -> {
            try {
                this.zookeeperConnectorSystemApi.delete(zookeeperConnectorSystemApi.getPeerPath(clusterNodeOptions.getNodeId()));
                return true;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            return false;
        }));
    }

    /**
     * Joins the registration thread, if one is still running, so that an unregister can never overtake
     * the create it is supposed to undo.
     * <p>
     * Bounded by {@link #REGISTRATION_JOIN_TIMEOUT_MS} rather than by the registration's own 30s
     * connection budget: in the normal case the node is created in milliseconds, and a shutdown must not
     * hang waiting for a connection that is evidently not coming. If the wait does expire the delete is
     * attempted anyway - same behaviour as before, no worse.
     */
    private void awaitPendingRegistration() {
        Thread pending = this.registrationThread;
        if (pending == null || !pending.isAlive()) {
            return;
        }
        try {
            pending.join(REGISTRATION_JOIN_TIMEOUT_MS);
            if (pending.isAlive()) {
                logger.warn("Cluster registration still in flight after {}ms, unregistering anyway", REGISTRATION_JOIN_TIMEOUT_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void registerClusterEventListener() {
        doClusterOperation(() -> {
            try {
                if (!this.zookeeperConnectorSystemApi.pathExists(this.zookeeperConnectorSystemApi.getClusterPath())) {
                    this.zookeeperConnectorSystemApi.createPersistent(this.zookeeperConnectorSystemApi.getClusterPath(), new byte[]{}, true);
                }
                CuratorCacheBuilder curatorCacheBuilder = CuratorCache.builder(this.zookeeperConnectorSystemApi.getZookeeperCuratorClient(), this.zookeeperConnectorSystemApi.getClusterPath());
                this.clusterCuratorCache = curatorCacheBuilder.build();
                this.clusterCacheListener = CuratorCacheListener.builder().forAll(this::processClusterEvent).build();
                this.clusterCuratorCache.listenable().addListener(clusterCacheListener);
                this.clusterCuratorCache.start();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        });
    }

    @SuppressWarnings("null")
    private void processClusterEvent(CuratorCacheListener.Type type, ChildData oldData, ChildData newData) {
        AtomicReference<ClusterEvent> atomicClusterEvent = new AtomicReference<>();
        boolean dataIsPresent = newData != null && newData.getData() != null;
        AtomicReference<byte[]> data = new AtomicReference<>();
        data.set(dataIsPresent ? newData.getData() : new byte[0]);
        switch (type) {
            case NODE_CREATED -> atomicClusterEvent.set(ClusterEvent.PEER_CONNECTED);
            case NODE_CHANGED -> atomicClusterEvent.set(ClusterEvent.PEER_INFO_CHANGED);
            case NODE_DELETED -> {
                atomicClusterEvent.set(ClusterEvent.PEER_DISCONNECTED);
                dataIsPresent = oldData != null && oldData.getData() != null;
                data.set(dataIsPresent ? oldData.getData() : new byte[0]);
            }
        }
        ZKData zkData = dataIsPresent ? ZKData.fromBytes(data.get()) : new ZKData();
        clusterObservers.forEach(observer -> observer.onClusterEvent(atomicClusterEvent.get(), new ZKClusterNodeInfo(zkData), data.get()));
    }

    @Override
    public boolean checkClusterLeadershipFor(String leadershipServiceKey) {
        return Boolean.TRUE.equals(doClusterOperation(() -> this.zookeeperConnectorSystemApi.isLeader(leadershipServiceKey)));
    }

    @Override
    public boolean peerStillExists(ClusterNodeInfo clusterNodeInfo) {
        return Boolean.TRUE.equals(doClusterOperation(() -> {
            try {
                String peerPath = this.zookeeperConnectorSystemApi.getPeerPath(clusterNodeInfo.getNodeId());
                return this.zookeeperConnectorSystemApi.pathExists(peerPath);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            return false;
        }));
    }

    @Override
    public synchronized Collection<ClusterNodeInfo> getPeerNodes() {
        return Collections.unmodifiableCollection(this.peers.values());
    }

    @Override
    public void onClusterEvent(ClusterEvent clusterEvent, ClusterNodeInfo clusterNodeInfo, byte[] bytes) {
        String nodeKey = clusterNodeInfo.getLayer() + "$$" + clusterNodeInfo.getNodeId();
        switch (clusterEvent) {
            case PEER_CONNECTED -> this.peers.put(nodeKey, clusterNodeInfo);
            case PEER_DISCONNECTED -> this.peers.remove(nodeKey);
            case PEER_INFO_CHANGED -> this.peers.put(nodeKey, clusterNodeInfo);
            case PEER_CUSTOM_EVENT -> {/*do nothing*/}
            case PEER_ERROR -> {/* do nothing*/}
            case PEER_DATA_EVENT -> {/*do nothing*/}
        }
    }

    /**
     * Running cluster operation
     *
     * @param runnable
     */
    private void doClusterOperation(Runnable runnable) {
        try {
            this.awaitConnection();
            runnable.run();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * running cluster operation
     *
     * @param callable
     * @param <T>
     * @return
     */
    private <T> T doClusterOperation(Callable<T> callable) {
        try {
            this.awaitConnection();
            return callable.call();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
        return null;
    }

}
