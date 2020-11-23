/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class has the control logic for the Leader.
 */
public class Leader {
    private static final Logger LOG = LoggerFactory.getLogger(Leader.class);

    static final private boolean nodelay = System.getProperty("leader.nodelay", "true").equals("true");

    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }

    static public class Proposal extends SyncedLearnerTracker {
        public QuorumPacket packet;
        public Request request;

        @Override
        public String toString() {
            return packet.getType() + ", " + packet.getZxid() + ", " + request;
        }
    }

    // Throttle when there are too many concurrent snapshots being sent to observers
    private static final String MAX_CONCURRENT_SNAPSHOTS = "zookeeper.leader.maxConcurrentSnapshots";
    private static final int maxConcurrentSnapshots;
    private static final String MAX_CONCURRENT_SNAPSHOT_TIMEOUT = "zookeeper.leader.maxConcurrentSnapshotTimeout";
    private static final long maxConcurrentSnapshotTimeout;

    static {
        maxConcurrentSnapshots = Integer.getInteger(MAX_CONCURRENT_SNAPSHOTS, 10);
        LOG.info(MAX_CONCURRENT_SNAPSHOTS + " = " + maxConcurrentSnapshots);
        maxConcurrentSnapshotTimeout = Long.getLong(MAX_CONCURRENT_SNAPSHOT_TIMEOUT, 5);
        LOG.info(MAX_CONCURRENT_SNAPSHOT_TIMEOUT + " = " + maxConcurrentSnapshotTimeout);
    }

    private final LearnerSnapshotThrottler learnerSnapshotThrottler;

    final LeaderZooKeeperServer zk;

    final QuorumPeer self;

    // VisibleForTesting
    protected boolean quorumFormed = false;

    // the follower acceptor thread
    volatile LearnerCnxAcceptor cnxAcceptor = null;

    // list of all the followers
    private final HashSet<LearnerHandler> learners =
            new HashSet<LearnerHandler>();

    private final BufferStats proposalStats;

    public BufferStats getProposalStats() {
        return proposalStats;
    }

    public LearnerSnapshotThrottler createLearnerSnapshotThrottler(
            int maxConcurrentSnapshots, long maxConcurrentSnapshotTimeout) {
        return new LearnerSnapshotThrottler(
                maxConcurrentSnapshots, maxConcurrentSnapshotTimeout);
    }

    /**
     * Returns a copy of the current learner snapshot
     */
    public List<LearnerHandler> getLearners() {
        synchronized (learners) {
            return new ArrayList<LearnerHandler>(learners);
        }
    }

    // list of followers that are ready to follow (i.e synced with the leader)
    private final HashSet<LearnerHandler> forwardingFollowers =
            new HashSet<LearnerHandler>();

    /**
     * Returns a copy of the current forwarding follower snapshot
     */
    public List<LearnerHandler> getForwardingFollowers() {
        synchronized (forwardingFollowers) {
            return new ArrayList<LearnerHandler>(forwardingFollowers);
        }
    }

    private void addForwardingFollower(LearnerHandler lh) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.add(lh);
        }
    }

    private final HashSet<LearnerHandler> observingLearners =
            new HashSet<LearnerHandler>();

    /**
     * Returns a copy of the current observer snapshot
     */
    public List<LearnerHandler> getObservingLearners() {
        synchronized (observingLearners) {
            return new ArrayList<LearnerHandler>(observingLearners);
        }
    }

    private void addObserverLearnerHandler(LearnerHandler lh) {
        synchronized (observingLearners) {
            observingLearners.add(lh);
        }
    }

    // Pending sync requests. Must access under 'this' lock.
    private final HashMap<Long, List<LearnerSyncRequest>> pendingSyncs =
            new HashMap<Long, List<LearnerSyncRequest>>();

    synchronized public int getNumPendingSyncs() {
        return pendingSyncs.size();
    }

    //Follower counter
    final AtomicLong followerCounter = new AtomicLong(-1);

    /**
     * 将对等添加到领导者
     *
     * @param learner instance of learner handle
     */
    void addLearnerHandler(LearnerHandler learner) {
        synchronized (learners) {
            learners.add(learner);
        }
    }

    /**
     * Remove the learner from the learner list
     *
     * @param peer
     */
    void removeLearnerHandler(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.remove(peer);
        }
        synchronized (learners) {
            learners.remove(peer);
        }
        synchronized (observingLearners) {
            observingLearners.remove(peer);
        }
    }

    boolean isLearnerSynced(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            return forwardingFollowers.contains(peer);
        }
    }


    /**
     * Returns true if a quorum in qv is connected and synced with the leader
     * and false otherwise
     *
     * @param qv, a QuorumVerifier
     */
    public boolean isQuorumSynced(QuorumVerifier qv) {
        HashSet<Long> ids = new HashSet<Long>();
        if (qv.getVotingMembers().containsKey(self.getId()))
            ids.add(self.getId());
        synchronized (forwardingFollowers) {
            for (LearnerHandler learnerHandler : forwardingFollowers) {
                if (learnerHandler.synced() && qv.getVotingMembers().containsKey(learnerHandler.getSid())) {
                    ids.add(learnerHandler.getSid());
                }
            }
        }
        return qv.containsQuorum(ids);
    }

    private final ServerSocket ss;

    Leader(QuorumPeer self, LeaderZooKeeperServer zk) throws IOException {
        this.self = self;
        this.proposalStats = new BufferStats();
        try {
            if (self.shouldUsePortUnification() || self.isSslQuorum()) {
                boolean allowInsecureConnection = self.shouldUsePortUnification();
                if (self.getQuorumListenOnAllIPs()) {
                    ss = new UnifiedServerSocket(self.getX509Util(), allowInsecureConnection, self.getQuorumAddress().getPort());
                } else {
                    ss = new UnifiedServerSocket(self.getX509Util(), allowInsecureConnection);
                }
            } else {
                if (self.getQuorumListenOnAllIPs()) {
                    ss = new ServerSocket(self.getQuorumAddress().getPort());
                } else {
                    ss = new ServerSocket();
                }
            }
            ss.setReuseAddress(true);
            if (!self.getQuorumListenOnAllIPs()) {
                ss.bind(self.getQuorumAddress());
            }
        } catch (BindException e) {
            if (self.getQuorumListenOnAllIPs()) {
                LOG.error("Couldn't bind to port " + self.getQuorumAddress().getPort(), e);
            } else {
                LOG.error("Couldn't bind to " + self.getQuorumAddress(), e);
            }
            throw e;
        }
        this.zk = zk;
        this.learnerSnapshotThrottler = createLearnerSnapshotThrottler(
                maxConcurrentSnapshots, maxConcurrentSnapshotTimeout);
    }

    /**
     * This message is for follower to expect diff
     */
    final static int DIFF = 13;

    /**
     * This is for follower to truncate its logs
     */
    final static int TRUNC = 14;

    /**
     * This is for follower to download the snapshots
     */
    final static int SNAP = 15;

    /**
     * This tells the leader that the connecting peer is actually an observer
     */
    final static int OBSERVERINFO = 16;

    /**
     * This message type is sent by the leader to indicate it's zxid and if
     * needed, its database.
     */
    final static int NEWLEADER = 10;

    /**
     * This message type is sent by a follower to pass the last zxid. This is here
     * for backward compatibility purposes.
     */
    final static int FOLLOWERINFO = 11;

    /**
     * This message type is sent by the leader to indicate that the follower is
     * now uptodate andt can start responding to clients.
     */
    final static int UPTODATE = 12;

    /**
     * This message is the first that a follower receives from the leader.
     * It has the protocol version and the epoch of the leader.
     */
    public static final int LEADERINFO = 17;

    /**
     * This message is used by the follow to ack a proposed epoch.
     */
    public static final int ACKEPOCH = 18;

    /**
     * This message type is sent to a leader to request and mutation operation.
     * The payload will consist of a request header followed by a request.
     */
    final static int REQUEST = 1;

    /**
     * This message type is sent by a leader to propose a mutation.
     */
    public final static int PROPOSAL = 2;

    /**
     * This message type is sent by a follower after it has synced a proposal.
     */
    final static int ACK = 3;

    /**
     * This message type is sent by a leader to commit a proposal and cause
     * followers to start serving the corresponding data.
     */
    final static int COMMIT = 4;

    /**
     * This message type is enchanged between follower and leader (initiated by
     * follower) to determine liveliness.
     */
    final static int PING = 5;

    /**
     * This message type is to validate a session that should be active.
     */
    final static int REVALIDATE = 6;

    /**
     * This message is a reply to a synchronize command flushing the pipe
     * between the leader and the follower.
     */
    final static int SYNC = 7;

    /**
     * This message type informs observers of a committed proposal.
     */
    final static int INFORM = 8;

    /**
     * Similar to COMMIT, only for a reconfig operation.
     */
    final static int COMMITANDACTIVATE = 9;

    /**
     * Similar to INFORM, only for a reconfig operation.
     */
    final static int INFORMANDACTIVATE = 19;

    final ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();

    private final ConcurrentLinkedQueue<Proposal> toBeApplied = new ConcurrentLinkedQueue<Proposal>();

    // VisibleForTesting
    protected final Proposal newLeaderProposal = new Proposal();

    class LearnerCnxAcceptor extends ZooKeeperCriticalThread {
        private volatile boolean stop = false;

        public LearnerCnxAcceptor() {
            super("LearnerCnxAcceptor-" + ss.getLocalSocketAddress(), zk
                    .getZooKeeperServerListener());
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    Socket s = null;
                    boolean error = false;
                    try {
                        // 阻塞，ObServer和Following将轮询给Leader发送获取数据的消息，因此Leader这里阻塞等待来自ObServer和Following的请求
                        s = ss.accept();

                        // 从initLimit开始，一旦在LearnerHandler中处理了ack，就切换到syncLimit
                        s.setSoTimeout(self.tickTime * self.initLimit);
                        s.setTcpNoDelay(nodelay);

                        BufferedInputStream is = new BufferedInputStream(
                                s.getInputStream());
                        // 处理具体的请求
                        LearnerHandler fh = new LearnerHandler(s, is, Leader.this);
                        // Zookeeper设计Leader选择采用异步方式处理请求，这里启动LearnerHandler处理器线程
                        fh.start();
                    } catch (SocketException e) {
                        error = true;
                        if (stop) {
                            LOG.info("exception while shutting down acceptor: "
                                    + e);
                            // 当Leader.shutdown（）调用ss.close（）时，对accept的调用将引发异常。我们赶上并将stop设置为true
                            stop = true;
                        } else {
                            throw e;
                        }
                    } catch (SaslException e) {
                        LOG.error("Exception while connecting to quorum learner", e);
                        error = true;
                    } catch (Exception e) {
                        error = true;
                        throw e;
                    } finally {
                        // Don't leak sockets on errors
                        if (error && s != null && !s.isClosed()) {
                            try {
                                s.close();
                            } catch (IOException e) {
                                LOG.warn("Error closing socket", e);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while accepting follower", e.getMessage());
                handleException(this.getName(), e);
            }
        }

        public void halt() {
            stop = true;
        }
    }

    StateSummary leaderStateSummary;

    long epoch = -1;
    boolean waitingForNewEpoch = true;

    // when a reconfig occurs where the leader is removed or becomes an observer, 
    // it does not commit ops after committing the reconfig
    boolean allowedToCommit = true;

    /**
     * 该方法是Leader的主要功能
     *
     * @throws IOException
     * @throws InterruptedException
     */
    void lead() throws IOException, InterruptedException {
        self.end_fle = Time.currentElapsedTime();
        long electionTimeTaken = self.end_fle - self.start_fle;
        // 设定选举时间
        self.setElectionTimeTaken(electionTimeTaken);
        LOG.info("LEADING - LEADER ELECTION TOOK - {} {}", electionTimeTaken,
                QuorumPeer.FLE_TIME_UNIT);
        self.start_fle = 0;
        self.end_fle = 0;

        zk.registerJMX(new LeaderBean(this, zk), self.jmxLocalPeerBean);

        try {
            self.tick.set(0);
            // 回复本地数据
            zk.loadData();

            // Leader状态摘要
            leaderStateSummary = new StateSummary(self.getCurrentEpoch(), zk.getLastProcessedZxid());

            // 等待新观察者的连接请求的启动线程。
            cnxAcceptor = new LearnerCnxAcceptor();
            cnxAcceptor.start();

            // 计算newEpoch
            long epoch = getEpochToPropose(self.getId(), self.getAcceptedEpoch());

            zk.setZxid(ZxidUtils.makeZxid(epoch, 0));

            synchronized (this) {
                lastProposed = zk.getZxid();
            }

            // 构建即将广播的newEpoch 包
            newLeaderProposal.packet = new QuorumPacket(NEWLEADER, zk.getZxid(),
                    null, null);


            if ((newLeaderProposal.packet.getZxid() & 0xffffffffL) != 0) {
                LOG.info("NEWLEADER proposal has Zxid of "
                        + Long.toHexString(newLeaderProposal.packet.getZxid()));
            }

            QuorumVerifier lastSeenQV = self.getLastSeenQuorumVerifier();
            QuorumVerifier curQV = self.getQuorumVerifier();
            if (curQV.getVersion() == 0 && curQV.getVersion() == lastSeenQV.getVersion()) {
                /**
                 *                  这是在ZOOKEEPER-1783中添加的。初始配置的版本为0（用户未明确指定；配置文件中缺少版本会解释为version = 0）。
                 *
                 *                  建立配置后，我们想增加其版本，以便它优先于其他尚未建立的初始配置（例如，尝试加入集成的服务器的配置，系统视图，而不是完整配置）。
                 *                  我们选择将新版本设置为NEWLEADER消息之一。但是，在我们做到这一点之前必须在新版本上达成一致，
                 *                  因此，我们只能在发送/接收UPTODATE时更改版本，在发送/接收NEWLEADER时不能更改版本。
                 *                  换句话说，我们无法在此处更改curQV，因为它已提交了仲裁验证程序，并且对于我们要使用的新版本仍未达成协议。
                 *                  相反，我们使用与NEWLEADER消息一起发送的lastSeenQuorumVerifier
                 *
                 *                  因此，这是让关注者了解新版本的好方法。 （使用 NEWLEADER发送 lastSeenQuorumVerifier的原始原因是，
                 *                  领导者在开始提议操作之前完成了它发现的所有可能未提交的重新配置，在这里，我们重新使用相同的代码路径就新版本达成共识数。
                 *
                 *                  重要的是，必须在领导者执行waitForEpochAck之前完成，因此，在LearnerHandlers从其waitForEpochAck返回之前
                 *                  ，在构造包含领导者的上次法定数量验证者的NEWLEADER消息之前，我们将在下面进行更改
                 */
                try {
                    QuorumVerifier newQV = self.configFromString(curQV.toString());
                    newQV.setVersion(zk.getZxid());
                    self.setLastSeenQuorumVerifier(newQV, true);
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }

            newLeaderProposal.addQuorumVerifier(self.getQuorumVerifier());
            if (self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier().getVersion()) {
                newLeaderProposal.addQuorumVerifier(self.getLastSeenQuorumVerifier());
            }

            // Leader 在这里阻塞，等待新时代newEpoch，被超过半数的节点同步，直到超过半数以上节点同步了newEpoch信息后，才会唤醒Leader线程继续往下执行
            // 这一步的目的是，所有节点统一（同步）一个Epoch时代
            waitForEpochAck(self.getId(), leaderStateSummary);
            self.setCurrentEpoch(epoch);

            try {
                // 等到收到确认新Leader身份的ACK，新Leader被选举出来后，同步节点（Follower、Observer）将发送一条确认身份的消息（ack）给新Leader，
                // Leader将会统计收到的消息数量，ack过半后集群开始进行数据同步
                // 这一步的目的是，所有节点统一（同步）同连接新Leader
                waitForNewLeaderAck(self.getId(), zk.getZxid());
            } catch (InterruptedException e) {
                // 等待一定数量的关注者，仅与sid同步
                shutdown("Waiting for a quorum of followers, only synced with sids: [ "
                        + newLeaderProposal.ackSetsToString() + " ]");
                HashSet<Long> followerSet = new HashSet<Long>();

                for (LearnerHandler f : getLearners()) {
                    if (self.getQuorumVerifier().getVotingMembers().containsKey(f.getSid())) {
                        followerSet.add(f.getSid());
                    }
                }
                boolean initTicksShouldBeIncreased = true;
                for (Proposal.QuorumVerifierAcksetPair qvAckset : newLeaderProposal.qvAcksetPairs) {
                    if (!qvAckset.getQuorumVerifier().containsQuorum(followerSet)) {
                        initTicksShouldBeIncreased = false;
                        break;
                    }
                }
                if (initTicksShouldBeIncreased) {
                    LOG.warn("Enough followers present. " +
                            "Perhaps the initTicks need to be increased.");
                }
                return;
            }

            startZkServer();

            /**
             * 警告：请勿在实际集群上将其用于QA测试。专门用于验证quorum 可以处理
             * ZOOKEEPER-1277中标识的较低的32位翻转问题。
             * 如果没有此选项，将需要很长的时间（例如一个月左右）才能看到导致翻转发生的40亿次写入*。
             *
             * 该字段允许您覆盖服务器的zxid。通常，您需要将其设置为0xfffffff0之类的值，然后启动仲裁，运行一些操作并查看重新选举。
             */
            String initialZxid = System.getProperty("zookeeper.testingonly.initialZxid");
            if (initialZxid != null) {
                long zxid = Long.parseLong(initialZxid);
                zk.setZxid((zk.getZxid() & 0xffffffff00000000L) | zxid);
            }

            if (!System.getProperty("zookeeper.leaderServes", "yes").equals("no")) {
                self.setZooKeeperServer(zk);
            }

            self.adminServer.setZooKeeperServer(zk);

            // 切都进行了，只需开始计算ticks
            // 警告：我在此notifyAll（）调用将通知的同步块上找不到任何等待语句，因此我将其注释掉
            //synchronized (this) {
            //    notifyAll();
            //}
            // 我们对两次tick进行ping操作，因此我们仅每隔一次迭代更新一次滴答
            boolean tickSkip = true;
            // 如果不为null，则关闭此领导者
            String shutdownMessage = null;

            while (true) {
                synchronized (this) {
                    long start = Time.currentElapsedTime();
                    long cur = start;
                    long end = start + self.tickTime / 2;
                    while (cur < end) {
                        wait(end - cur);
                        cur = Time.currentElapsedTime();
                    }

                    if (!tickSkip) {
                        self.tick.incrementAndGet();
                    }

                    // 我们使用SyncedLearnerTracker实例来跟踪已同步的学习者，以确保我们仍具有当前（以及潜在的下一个未决）视图的法定人数。
                    // 启动同步跟踪器
                    SyncedLearnerTracker syncedAckSet = new SyncedLearnerTracker();
                    syncedAckSet.addQuorumVerifier(self.getQuorumVerifier());
                    if (self.getLastSeenQuorumVerifier() != null
                            && self.getLastSeenQuorumVerifier().getVersion() > self
                            .getQuorumVerifier().getVersion()) {
                        syncedAckSet.addQuorumVerifier(self
                                .getLastSeenQuorumVerifier());
                    }

                    syncedAckSet.addAck(self.getId());

                    for (LearnerHandler f : getLearners()) {
                        if (f.synced()) {
                            syncedAckSet.addAck(f.getSid());
                        }
                    }

                    // 检查领导者的运行状态
                    if (!this.isRunning()) {
                        // 设置关机标志
                        shutdownMessage = "Unexpected internal error";
                        break;
                    }

                    if (!tickSkip && !syncedAckSet.hasAllQuorums()) {
                        // 上次提交的和/或上一次建议的配置的仲裁丢失，设置关闭标志
                        shutdownMessage = "Not sufficient followers synced, only synced with sids: [ "
                                + syncedAckSet.ackSetsToString() + " ]";
                        break;
                    }
                    tickSkip = !tickSkip;
                }
                // 每隔一定时间发送一次心跳
                for (LearnerHandler f : getLearners()) {
                    f.ping();
                }
            }
            if (shutdownMessage != null) {
                shutdown(shutdownMessage);
                // 领导
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    boolean isShutdown;

    /**
     * Close down all the LearnerHandlers
     */
    void shutdown(String reason) {
        LOG.info("Shutting down");

        if (isShutdown) {
            return;
        }

        LOG.info("Shutdown called",
                new Exception("shutdown Leader! reason: " + reason));

        if (cnxAcceptor != null) {
            cnxAcceptor.halt();
        }

        // NIO should not accept conenctions
        self.setZooKeeperServer(null);
        self.adminServer.setZooKeeperServer(null);
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during close", e);
        }
        self.closeAllConnections();
        // shutdown the previous zk
        if (zk != null) {
            zk.shutdown();
        }
        synchronized (learners) {
            for (Iterator<LearnerHandler> it = learners.iterator(); it
                    .hasNext(); ) {
                LearnerHandler f = it.next();
                it.remove();
                f.shutdown();
            }
        }
        isShutdown = true;
    }

    /**
     * In a reconfig operation, this method attempts to find the best leader for next configuration.
     * If the current leader is a voter in the next configuartion, then it remains the leader.
     * Otherwise, choose one of the new voters that acked the reconfiguartion, such that it is as
     * up-to-date as possible, i.e., acked as many outstanding proposals as possible.
     *
     * @param reconfigProposal
     * @param zxid             of the reconfigProposal
     * @return server if of the designated leader
     */

    private long getDesignatedLeader(Proposal reconfigProposal, long zxid) {
        //new configuration
        Proposal.QuorumVerifierAcksetPair newQVAcksetPair = reconfigProposal.qvAcksetPairs.get(reconfigProposal.qvAcksetPairs.size() - 1);

        //check if I'm in the new configuration with the same quorum address -
        // if so, I'll remain the leader
        if (newQVAcksetPair.getQuorumVerifier().getVotingMembers().containsKey(self.getId()) &&
                newQVAcksetPair.getQuorumVerifier().getVotingMembers().get(self.getId()).addr.equals(self.getQuorumAddress())) {
            return self.getId();
        }
        // start with an initial set of candidates that are voters from new config that
        // acknowledged the reconfig op (there must be a quorum). Choose one of them as
        // current leader candidate
        HashSet<Long> candidates = new HashSet<Long>(newQVAcksetPair.getAckset());
        candidates.remove(self.getId()); // if we're here, I shouldn't be the leader
        long curCandidate = candidates.iterator().next();

        //go over outstanding ops in order, and try to find a candidate that acked the most ops.
        //this way it will be the most up-to-date and we'll minimize the number of ops that get dropped

        long curZxid = zxid + 1;
        Proposal p = outstandingProposals.get(curZxid);

        while (p != null && !candidates.isEmpty()) {
            for (Proposal.QuorumVerifierAcksetPair qvAckset : p.qvAcksetPairs) {
                //reduce the set of candidates to those that acknowledged p
                candidates.retainAll(qvAckset.getAckset());
                //no candidate acked p, return the best candidate found so far
                if (candidates.isEmpty()) return curCandidate;
                //update the current candidate, and if it is the only one remaining, return it
                curCandidate = candidates.iterator().next();
                if (candidates.size() == 1) return curCandidate;
            }
            curZxid++;
            p = outstandingProposals.get(curZxid);
        }

        return curCandidate;
    }

    /**
     * @return True if committed, otherwise false.
     **/
    synchronized public boolean tryToCommit(Proposal p, long zxid, SocketAddress followerAddr) {
        // make sure that ops are committed in order. With reconfigurations it is now possible
        // that different operations wait for different sets of acks, and we still want to enforce
        // that they are committed in order. Currently we only permit one outstanding reconfiguration
        // such that the reconfiguration and subsequent outstanding ops proposed while the reconfig is
        // pending all wait for a quorum of old and new config, so it's not possible to get enough acks
        // for an operation without getting enough acks for preceding ops. But in the future if multiple
        // concurrent reconfigs are allowed, this can happen.
        if (outstandingProposals.containsKey(zxid - 1)) return false;

        // in order to be committed, a proposal must be accepted by a quorum.
        //
        // getting a quorum from all necessary configurations.
        if (!p.hasAllQuorums()) {
            return false;
        }

        // commit proposals in order
        if (zxid != lastCommitted + 1) {
            LOG.warn("Commiting zxid 0x" + Long.toHexString(zxid)
                    + " from " + followerAddr + " not first!");
            LOG.warn("First is "
                    + (lastCommitted + 1));
        }

        outstandingProposals.remove(zxid);

        if (p.request != null) {
            toBeApplied.add(p);
        }

        if (p.request == null) {
            LOG.warn("Going to commmit null: " + p);
        } else if (p.request.getHdr().getType() == OpCode.reconfig) {
            LOG.debug("Committing a reconfiguration! " + outstandingProposals.size());

            //if this server is voter in new config with the same quorum address, 
            //then it will remain the leader
            //otherwise an up-to-date follower will be designated as leader. This saves
            //leader election time, unless the designated leader fails                             
            Long designatedLeader = getDesignatedLeader(p, zxid);
            //LOG.warn("designated leader is: " + designatedLeader);

            QuorumVerifier newQV = p.qvAcksetPairs.get(p.qvAcksetPairs.size() - 1).getQuorumVerifier();

            self.processReconfig(newQV, designatedLeader, zk.getZxid(), true);

            if (designatedLeader != self.getId()) {
                allowedToCommit = false;
            }

            // we're sending the designated leader, and if the leader is changing the followers are 
            // responsible for closing the connection - this way we are sure that at least a majority of them 
            // receive the commit message.
            commitAndActivate(zxid, designatedLeader);
            informAndActivate(p, designatedLeader);
            //turnOffFollowers();
        } else {
            commit(zxid);
            inform(p);
        }
        zk.commitProcessor.commit(p.request);
        if (pendingSyncs.containsKey(zxid)) {
            for (LearnerSyncRequest r : pendingSyncs.remove(zxid)) {
                sendSync(r);
            }
        }

        return true;
    }

    /**
     * 保留领导者收到的特定建议的计数
     *
     * @param zxid,        the zxid of the proposal sent out
     * @param sid,         the id of the server that sent the ack
     * @param followerAddr
     */
    synchronized public void processAck(long sid, long zxid, SocketAddress followerAddr) {
        if (!allowedToCommit) return; // last op committed was a leader change - from now on 
        // the new leader should commit
        if (LOG.isTraceEnabled()) {
            LOG.trace("Ack zxid: 0x{}", Long.toHexString(zxid));
            for (Proposal p : outstandingProposals.values()) {
                long packetZxid = p.packet.getZxid();
                LOG.trace("outstanding proposal: 0x{}",
                        Long.toHexString(packetZxid));
            }
            LOG.trace("outstanding proposals all");
        }

        if ((zxid & 0xffffffffL) == 0) {
            /*
             * We no longer process NEWLEADER ack with this method. However,
             * the learner sends an ack back to the leader after it gets
             * UPTODATE, so we just ignore the message.
             */
            return;
        }


        if (outstandingProposals.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("outstanding is 0");
            }
            return;
        }
        if (lastCommitted >= zxid) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        Long.toHexString(lastCommitted), Long.toHexString(zxid));
            }
            // The proposal has already been committed
            return;
        }
        Proposal p = outstandingProposals.get(zxid);
        if (p == null) {
            LOG.warn("Trying to commit future proposal: zxid 0x{} from {}",
                    Long.toHexString(zxid), followerAddr);
            return;
        }

        p.addAck(sid);        
        /*if (LOG.isDebugEnabled()) {
            LOG.debug("Count for zxid: 0x{} is {}",
                    Long.toHexString(zxid), p.ackSet.size());
        }*/

        boolean hasCommitted = tryToCommit(p, zxid, followerAddr);

        // If p is a reconfiguration, multiple other operations may be ready to be committed,
        // since operations wait for different sets of acks.
        // Currently we only permit one outstanding reconfiguration at a time
        // such that the reconfiguration and subsequent outstanding ops proposed while the reconfig is
        // pending all wait for a quorum of old and new config, so its not possible to get enough acks
        // for an operation without getting enough acks for preceding ops. But in the future if multiple
        // concurrent reconfigs are allowed, this can happen and then we need to check whether some pending
        // ops may already have enough acks and can be committed, which is what this code does.

        if (hasCommitted && p.request != null && p.request.getHdr().getType() == OpCode.reconfig) {
            long curZxid = zxid;
            while (allowedToCommit && hasCommitted && p != null) {
                curZxid++;
                p = outstandingProposals.get(curZxid);
                if (p != null) hasCommitted = tryToCommit(p, curZxid, null);
            }
        }
    }

    static class ToBeAppliedRequestProcessor implements RequestProcessor {
        private final RequestProcessor next;

        private final Leader leader;

        /**
         * This request processor simply maintains the toBeApplied list. For
         * this to work next must be a FinalRequestProcessor and
         * FinalRequestProcessor.processRequest MUST process the request
         * synchronously!
         *
         * @param next a reference to the FinalRequestProcessor
         */
        ToBeAppliedRequestProcessor(RequestProcessor next, Leader leader) {
            if (!(next instanceof FinalRequestProcessor)) {
                throw new RuntimeException(ToBeAppliedRequestProcessor.class
                        .getName()
                        + " must be connected to "
                        + FinalRequestProcessor.class.getName()
                        + " not "
                        + next.getClass().getName());
            }
            this.leader = leader;
            this.next = next;
        }

        /*
         * (non-Javadoc)
         *
         * @see org.apache.zookeeper.server.RequestProcessor#processRequest(org.apache.zookeeper.server.Request)
         */
        public void processRequest(Request request) throws RequestProcessorException {
            next.processRequest(request);

            // The only requests that should be on toBeApplied are write
            // requests, for which we will have a hdr. We can't simply use
            // request.zxid here because that is set on read requests to equal
            // the zxid of the last write op.
            if (request.getHdr() != null) {
                long zxid = request.getHdr().getZxid();
                Iterator<Proposal> iter = leader.toBeApplied.iterator();
                if (iter.hasNext()) {
                    Proposal p = iter.next();
                    if (p.request != null && p.request.zxid == zxid) {
                        iter.remove();
                        return;
                    }
                }
                LOG.error("Committed request not found on toBeApplied: "
                        + request);
            }
        }

        /*
         * (non-Javadoc)
         *
         * @see org.apache.zookeeper.server.RequestProcessor#shutdown()
         */
        public void shutdown() {
            LOG.info("Shutting down");
            next.shutdown();
        }
    }

    /**
     * send a packet to all the followers ready to follow
     *
     * @param qp the packet to be sent
     */
    void sendPacket(QuorumPacket qp) {
        synchronized (forwardingFollowers) {
            for (LearnerHandler f : forwardingFollowers) {
                f.queuePacket(qp);
            }
        }
    }

    /**
     * send a packet to all observers
     */
    void sendObserverPacket(QuorumPacket qp) {
        for (LearnerHandler f : getObservingLearners()) {
            f.queuePacket(qp);
        }
    }

    long lastCommitted = -1;

    /**
     * Create a commit packet and send it to all the members of the quorum
     *
     * @param zxid
     */
    public void commit(long zxid) {
        synchronized (this) {
            lastCommitted = zxid;
        }
        QuorumPacket qp = new QuorumPacket(Leader.COMMIT, zxid, null, null);
        sendPacket(qp);
    }

    //commit and send some info
    public void commitAndActivate(long zxid, long designatedLeader) {
        synchronized (this) {
            lastCommitted = zxid;
        }

        byte data[] = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putLong(designatedLeader);

        QuorumPacket qp = new QuorumPacket(Leader.COMMITANDACTIVATE, zxid, data, null);
        sendPacket(qp);
    }

    /**
     * Create an inform packet and send it to all observers.
     */
    public void inform(Proposal proposal) {
        QuorumPacket qp = new QuorumPacket(Leader.INFORM, proposal.request.zxid,
                proposal.packet.getData(), null);
        sendObserverPacket(qp);
    }


    /**
     * Create an inform&activate packet and send it to all observers.
     */
    public void informAndActivate(Proposal proposal, long designatedLeader) {
        byte[] proposalData = proposal.packet.getData();
        byte[] data = new byte[proposalData.length + 8];
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putLong(designatedLeader);
        buffer.put(proposalData);

        QuorumPacket qp = new QuorumPacket(Leader.INFORMANDACTIVATE, proposal.request.zxid, data, null);
        sendObserverPacket(qp);
    }

    long lastProposed;


    /**
     * Returns the current epoch of the leader.
     *
     * @return
     */
    public long getEpoch() {
        return ZxidUtils.getEpochFromZxid(lastProposed);
    }

    @SuppressWarnings("serial")
    public static class XidRolloverException extends Exception {
        public XidRolloverException(String message) {
            super(message);
        }
    }

    /**
     * create a proposal and send it out to all the members
     *
     * @param request
     * @return the proposal that is queued to send to all the members
     */
    public Proposal propose(Request request) throws XidRolloverException {
        /**
         * Address the rollover issue. All lower 32bits set indicate a new leader
         * election. Force a re-election instead. See ZOOKEEPER-1277
         */
        if ((request.zxid & 0xffffffffL) == 0xffffffffL) {
            String msg =
                    "zxid lower 32 bits have rolled over, forcing re-election, and therefore new epoch start";
            shutdown(msg);
            throw new XidRolloverException(msg);
        }

        byte[] data = SerializeUtils.serializeRequest(request);
        proposalStats.setLastBufferSize(data.length);
        QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, data, null);

        Proposal p = new Proposal();
        p.packet = pp;
        p.request = request;

        synchronized (this) {
            p.addQuorumVerifier(self.getQuorumVerifier());

            if (request.getHdr().getType() == OpCode.reconfig) {
                self.setLastSeenQuorumVerifier(request.qv, true);
            }

            if (self.getQuorumVerifier().getVersion() < self.getLastSeenQuorumVerifier().getVersion()) {
                p.addQuorumVerifier(self.getLastSeenQuorumVerifier());
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Proposing:: " + request);
            }

            lastProposed = p.packet.getZxid();
            outstandingProposals.put(lastProposed, p);
            sendPacket(pp);
        }
        return p;
    }

    public LearnerSnapshotThrottler getLearnerSnapshotThrottler() {
        return learnerSnapshotThrottler;
    }

    /**
     * Process sync requests
     *
     * @param r the request
     */

    synchronized public void processSync(LearnerSyncRequest r) {
        if (outstandingProposals.isEmpty()) {
            sendSync(r);
        } else {
            List<LearnerSyncRequest> l = pendingSyncs.get(lastProposed);
            if (l == null) {
                l = new ArrayList<LearnerSyncRequest>();
            }
            l.add(r);
            pendingSyncs.put(lastProposed, l);
        }
    }

    /**
     * Sends a sync message to the appropriate server
     */
    public void sendSync(LearnerSyncRequest r) {
        QuorumPacket qp = new QuorumPacket(Leader.SYNC, 0, null, null);
        r.fh.queuePacket(qp);
    }

    /**
     * lets the leader know that a follower is capable of following and is done
     * syncing
     *
     * @param handler handler of the follower
     * @return last proposed zxid
     * @throws InterruptedException
     */
    synchronized public long startForwarding(LearnerHandler handler,
                                             long lastSeenZxid) {
        // Queue up any outstanding requests enabling the receipt of
        // new requests
        if (lastProposed > lastSeenZxid) {
            for (Proposal p : toBeApplied) {
                if (p.packet.getZxid() <= lastSeenZxid) {
                    continue;
                }
                handler.queuePacket(p.packet);
                // Since the proposal has been committed we need to send the
                // commit message also
                QuorumPacket qp = new QuorumPacket(Leader.COMMIT, p.packet
                        .getZxid(), null, null);
                handler.queuePacket(qp);
            }
            // Only participant need to get outstanding proposals
            if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
                List<Long> zxids = new ArrayList<Long>(outstandingProposals.keySet());
                Collections.sort(zxids);
                for (Long zxid : zxids) {
                    if (zxid <= lastSeenZxid) {
                        continue;
                    }
                    handler.queuePacket(outstandingProposals.get(zxid).packet);
                }
            }
        }
        if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
            addForwardingFollower(handler);
        } else {
            addObserverLearnerHandler(handler);
        }

        return lastProposed;
    }

    // VisibleForTesting
    protected final Set<Long> connectingFollowers = new HashSet<Long>();

    public long getEpochToPropose(long sid, long lastAcceptedEpoch) throws InterruptedException, IOException {
        synchronized (connectingFollowers) {
            if (!waitingForNewEpoch) {
                return epoch;
            }
            if (lastAcceptedEpoch >= epoch) {
                epoch = lastAcceptedEpoch + 1;
            }
            if (isParticipant(sid)) {
                connectingFollowers.add(sid);
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            if (connectingFollowers.contains(self.getId()) &&
                    verifier.containsQuorum(connectingFollowers)) {
                waitingForNewEpoch = false;
                self.setAcceptedEpoch(epoch);
                connectingFollowers.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                while (waitingForNewEpoch && cur < end) {
                    connectingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (waitingForNewEpoch) {
                    throw new InterruptedException("Timeout while waiting for epoch from quorum");
                }
            }
            return epoch;
        }
    }

    // VisibleForTesting
    protected final Set<Long> electingFollowers = new HashSet<Long>();
    // VisibleForTesting
    protected boolean electionFinished = false;

    public void waitForEpochAck(long id, StateSummary ss) throws IOException, InterruptedException {
        synchronized (electingFollowers) {
            if (electionFinished) {
                return;
            }
            if (ss.getCurrentEpoch() != -1) {
                if (ss.isMoreRecentThan(leaderStateSummary)) {
                    // 追随者领先于领导者，领导者摘要
                    throw new IOException("Follower is ahead of the leader, leader summary: "
                            + leaderStateSummary.getCurrentEpoch()
                            + " (current epoch), "
                            + leaderStateSummary.getLastZxid()
                            + " (last zxid)");
                }
                // 这里是在验证Follower是否选举了（投票）当前节点作为Ledaer
                if (isParticipant(id)) {
                    // 增加计数
                    electingFollowers.add(id);
                }
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            // 验证是否超过半数节点投票当前节点
            if (electingFollowers.contains(self.getId()) && verifier.containsQuorum(electingFollowers)) {
                // 超过半数节点时，将唤醒所有Follower线程
                electionFinished = true;
                electingFollowers.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                // 否则将会阻塞，直到确认数过半
                while (!electionFinished && cur < end) {
                    electingFollowers.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!electionFinished) {
                    // 等待纪元被仲裁确认时超时
                    throw new InterruptedException("Timeout while waiting for epoch to be acked by quorum");
                }
            }
        }
    }

    /**
     * Return a list of sid in set as string
     */
    private String getSidSetString(Set<Long> sidSet) {
        StringBuilder sids = new StringBuilder();
        Iterator<Long> iter = sidSet.iterator();
        while (iter.hasNext()) {
            sids.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            sids.append(",");
        }
        return sids.toString();
    }

    /**
     * 启动Leader ZooKeeper服务器并将zxid初始化为新纪元
     */
    private synchronized void startZkServer() {
        // 将lastCommitted和Db的zxid更新为代表新纪元的值
        lastCommitted = zk.getZxid();
        LOG.info("Have quorum of supporters, sids: [ "
                        + newLeaderProposal.ackSetsToString()
                        + " ]; starting up and setting last processed zxid: 0x{}",
                Long.toHexString(zk.getZxid()));

        /*
         * ZOOKEEPER-1324。
         * 领导将新配置发送给NEWLEADER消息中的其他人（请参阅构造NEWLEADER消息的LearnerHandler），
         * 一旦具有足够的 ack，我们必须执行以下代码，以便将配置应用于自身
         */
        QuorumVerifier newQV = self.getLastSeenQuorumVerifier();

        Long designatedLeader = getDesignatedLeader(newLeaderProposal, zk.getZxid());

        self.processReconfig(newQV, designatedLeader, zk.getZxid(), true);
        if (designatedLeader != self.getId()) {
            allowedToCommit = false;
        }

        zk.startup();
        /*
         * 在此处更新选举投票，以确保*合奏的所有成员向启动的新服务器报告相同的投票，并向合奏发送领导者选举通知。
         *
         * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
         */
        self.updateElectionVote(getEpoch());

        zk.getZKDatabase().setlastProcessedZxid(zk.getZxid());
    }

    /**
     * 在给定的sid后面处理NEW LEADER，并等待直到领导者收到足够的确认。
     *
     * @param sid
     * @throws InterruptedException
     */
    public void waitForNewLeaderAck(long sid, long zxid)
            throws InterruptedException {

        synchronized (newLeaderProposal.qvAcksetPairs) {

            if (quorumFormed) {
                return;
            }

            long currentZxid = newLeaderProposal.packet.getZxid();
            if (zxid != currentZxid) {
                LOG.error("NEWLEADER ACK from sid: " + sid
                        + " is from a different epoch - current 0x"
                        + Long.toHexString(currentZxid) + " receieved 0x"
                        + Long.toHexString(zxid));
                return;
            }

            // 治理是在做统计工作，统计以与新Leader建立连接的节点数量，过半节点确认新Leader的身份后才会放行，否则一直阻塞
            newLeaderProposal.addAck(sid);
            /*
             * 请注意，addAck已经检查了学习者是否为参与者。
             */
            if (newLeaderProposal.hasAllQuorums()) {
                quorumFormed = true;
                newLeaderProposal.qvAcksetPairs.notifyAll();
            } else {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + self.getInitLimit() * self.getTickTime();
                // 等待确认信Leader的消息
                while (!quorumFormed && cur < end) {
                    newLeaderProposal.qvAcksetPairs.wait(end - cur);
                    cur = Time.currentElapsedTime();
                }
                if (!quorumFormed) {
                    throw new InterruptedException(
                            "Timeout while waiting for NEWLEADER to be acked by quorum");
                }
            }
        }
    }

    /**
     * Get string representation of a given packet type
     *
     * @param packetType
     * @return string representing the packet type
     */
    public static String getPacketType(int packetType) {
        switch (packetType) {
            case DIFF:
                return "DIFF";
            case TRUNC:
                return "TRUNC";
            case SNAP:
                return "SNAP";
            case OBSERVERINFO:
                return "OBSERVERINFO";
            case NEWLEADER:
                return "NEWLEADER";
            case FOLLOWERINFO:
                return "FOLLOWERINFO";
            case UPTODATE:
                return "UPTODATE";
            case LEADERINFO:
                return "LEADERINFO";
            case ACKEPOCH:
                return "ACKEPOCH";
            case REQUEST:
                return "REQUEST";
            case PROPOSAL:
                return "PROPOSAL";
            case ACK:
                return "ACK";
            case COMMIT:
                return "COMMIT";
            case COMMITANDACTIVATE:
                return "COMMITANDACTIVATE";
            case PING:
                return "PING";
            case REVALIDATE:
                return "REVALIDATE";
            case SYNC:
                return "SYNC";
            case INFORM:
                return "INFORM";
            case INFORMANDACTIVATE:
                return "INFORMANDACTIVATE";
            default:
                return "UNKNOWN";
        }
    }

    private boolean isRunning() {
        return self.isRunning() && zk.isRunning();
    }

    private boolean isParticipant(long sid) {
        return self.getQuorumVerifier().getVotingMembers().containsKey(sid);
    }
}
