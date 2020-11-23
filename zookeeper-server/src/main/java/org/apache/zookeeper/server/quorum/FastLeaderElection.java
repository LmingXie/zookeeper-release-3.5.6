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

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 * <p>
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */


public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    final static int maxNotificationInterval = 60000;

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;


    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public final static int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         */
        long leader;

        /*
         * zxid of the proposed leader
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * current state of sender
         */
        QuorumPeer.ServerState state;

        /*
         * Address of sender
         */
        long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */
        long peerEpoch;
    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
               long leader,
               long zxid,
               long electionEpoch,
               ServerState state,
               long sid,
               long peerEpoch,
               byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         */
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * Current state;
         */
        QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */
        long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         */
        byte[] configData = dummyData;

        /*
         * Leader epoch
         */
        long peerEpoch;
    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * 消息处理程序的多线程实现。 Messenger 实现两个子类：WorkReceiver和WorkSender。
     * 从名称中可以明显看出每个的功能。每个都会产生一个新线程。
     */

    protected class Messenger {

        /**
         * 在方法run（）上从QuorumCnxManager实例接收消息，并处理此类消息。
         */

        class WorkerReceiver extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive 睡觉就睡
                    try {
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) continue;

                        // 当前协议和前两个协议都至少发送28个字节
                        if (response.buffer.capacity() < 28) {
                            LOG.error("Got a short response: " + response.buffer.capacity());
                            continue;
                        }

                        // 这是ZK-107之前的向后兼容模式用于协议的版本，其中我们没有发送对等纪元通过对等纪元和版本，消息变为40字节
                        boolean backCompatibility28 = (response.buffer.capacity() == 28);

                        // 这是没有版本信息的向后兼容模式
                        boolean backCompatibility40 = (response.buffer.capacity() == 40);

                        response.buffer.clear();

                        // 实例化通知并设置其属性
                        Notification n = new Notification();

                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        // 版本兼容
                        if (!backCompatibility28) {
                            rpeerepoch = response.buffer.getLong();
                            if (!backCompatibility40) {
                                /*
                                 * Version added in 3.4.6
                                 */

                                version = response.buffer.getInt();
                            } else {
                                LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                            }
                        } else {
                            LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                            rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                        }

                        // 验证者
                        QuorumVerifier rqv = null;

                        // 检查我们是否有包含config的版本。如果是这样，请从消息中提取配置信息。
                        if (version > 0x1) {
                            int configLength = response.buffer.getInt();
                            byte b[] = new byte[configLength];

                            response.buffer.get(b);

                            synchronized (self) {
                                try {
                                    rqv = self.configFromString(new String(b));
                                    QuorumVerifier curQV = self.getQuorumVerifier();
                                    // 收到版本 > 当前节点版本
                                    if (rqv.getVersion() > curQV.getVersion()) {
                                        LOG.info("{} Received version: {} my version: {}", self.getId(),
                                                Long.toHexString(rqv.getVersion()),
                                                Long.toHexString(self.getQuorumVerifier().getVersion()));

                                        // 对等状态：仍然处于LOOKING选举状态
                                        if (self.getPeerState() == ServerState.LOOKING) {
                                            LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                            // 进程重新配置
                                            self.processReconfig(rqv, null, null, false);

                                            if (!rqv.equals(curQV)) {
                                                // 重新开始领导人选举
                                                LOG.info("restarting leader election");
                                                self.shuttingDownLE = true;
                                                self.getElectionAlg().shutdown();
                                                break;
                                            }
                                        } else {
                                            LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                        }
                                    }
                                } catch (IOException e) {
                                    LOG.error("Something went wrong while processing config received from {}", response.sid);
                                } catch (ConfigException e) {
                                    LOG.error("Something went wrong while processing config received from {}", response.sid);
                                }
                            }
                        } else {
                            LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                        }

                        /*
                         * 如果来自非投票服务器（例如观察者或非投票关注者），请立即做出响应。
                         */
                        if (!validVoter(response.sid)) {
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            ToSend notmsg = new ToSend(ToSend.mType.notification,
                                    current.getId(),
                                    current.getZxid(),
                                    logicalclock.get(),
                                    self.getPeerState(),
                                    response.sid,
                                    current.getPeerEpoch(),
                                    qv.toString().getBytes());

                            sendqueue.offer(notmsg);
                        } else {
                            // 接收新讯息
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Receive new notification message. My id = "
                                        + self.getId());
                            }

                            // 发送此消息的对等节点状态
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (rstate) {
                                case 0:
                                    ackstate = QuorumPeer.ServerState.LOOKING;
                                    break;
                                case 1:
                                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                                    break;
                                case 2:
                                    ackstate = QuorumPeer.ServerState.LEADING;
                                    break;
                                case 3:
                                    ackstate = QuorumPeer.ServerState.OBSERVING;
                                    break;
                                default:
                                    continue;
                            }

                            n.leader = rleader;
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * 打印通知信息
                             */
                            if (LOG.isInfoEnabled()) {
                                printNotification(n);
                            }

                            /*
                             * 如果此服务器正在寻找，则发送建议的领导者
                             */

                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                recvqueue.offer(n);

                                /*
                                 * 如果发送此消息的对等方也在寻找并且其逻辑时钟落后，则发送回通知。
                                 */
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                        && (n.electionEpoch < logicalclock.get())) {
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification,
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock.get(),
                                            self.getPeerState(),
                                            response.sid,
                                            v.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * 如果该服务器没有在看，但是发送了ack的服务器正在看，则将其认为是领导者的服务器发回。
                                 */
                                Vote current = self.getCurrentVote();
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (LOG.isDebugEnabled()) {
                                        // 正在发送新通知
                                        LOG.debug("Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}",
                                                self.getId(),
                                                response.sid,
                                                Long.toHexString(current.getZxid()),
                                                current.getId(),
                                                Long.toHexString(self.getQuorumVerifier().getVersion()));
                                    }

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                            ToSend.mType.notification,
                                            current.getId(),
                                            current.getZxid(),
                                            current.getElectionEpoch(),
                                            self.getPeerState(),
                                            response.sid,
                                            current.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message" +
                                e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }

        /**
         * 该工作人员仅使要发送的消息出队，并将其排队在管理者的队列中。
         */

        class WorkerSender extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    try {
                        // 采用轮询提取sendqueue中的数据，sendqueue存放所有参与 Leader选举节点
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (m == null) continue;

                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * 通过运行run()一旦有发送新邮件时调用。
             *
             * @param m message to send
             */
            void process(ToSend m) {
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(),
                        m.leader,
                        m.zxid,
                        m.electionEpoch,
                        m.peerEpoch,
                        m.configData);

                manager.toSend(m.sid, requestBuffer);

            }
        }

        WorkerSender ws;
        WorkerReceiver wr;
        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Messenger类的构造函数。
         *
         * @param manager Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            this.ws = new WorkerSender(manager);

            this.wsThread = new Thread(this.ws,
                    "WorkerSender[1=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            this.wr = new WorkerReceiver(manager);

            this.wrThread = new Thread(this.wr,
                    "WorkerReceiver[1=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * 启动WorkerSender和WorkerReceiver的实例
         */
        void start() {
            this.wsThread.start();
            this.wrThread.start();
        }

        /**
         * 停止WorkerSender和WorkerReceiver的实例
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    /**
     * 记录当前节点所处时代
     */
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state,
                               long leader,
                               long zxid,
                               long electionEpoch,
                               long epoch) {
        byte requestBytes[] = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state,
                               long leader,
                               long zxid,
                               long electionEpoch,
                               long epoch,
                               byte[] configData) {
        byte requestBytes[] = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * 建筑通知包发送
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * FastLeaderElection的构造。
     * 它有两个参数，一个是QuorumPeer对象实例化这个对象，另一个是连接管理器。
     * <p>
     * 这样的物体应该在动物园管理员服务的实例过程中创建仅通过每个对等体一次。
     *
     * @param self    创建该对象的QuorumPeer
     * @param manager Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * 此方法启动发送方和接收方线程。
     */
    public void start() {
        this.messenger.start();
    }

    /**
     * 这种方法是通过构造函数调用。
     * <p>
     * 因为它是必须在这个类的构造函数中的任何对象的启动过程的一部分，它可能是最好保持作为一个单独的方法。
     * 因为我们目前有一个构造函数，它并非绝对必要把它分开。
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();
        this.messenger = new Messenger(manager);
    }

    private void leaveInstance(Vote v) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}",
                    v.getId(), Long.toHexString(v.getZxid()), self.getId(), self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;

    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * 默认广播消息给所有参与选举的节点，提议自己做Leader
     */
    private void sendNotifications() {
        // 获取当前和下一步配置的参与选举节点
        for (long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();
            // 构建通知消息，通知当前节点提议那个节点作为Leader
            ToSend notmsg = new ToSend(ToSend.mType.notification,
                    // 默认提议自己作为Leader
                    proposedLeader,
                    proposedZxid,
                    logicalclock.get(),
                    QuorumPeer.ServerState.LOOKING,
                    sid,
                    proposedEpoch, qv.toString().getBytes());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending Notification: " + proposedLeader + " (n.leader), 0x" +
                        Long.toHexString(proposedZxid) + " (n.zxid), 0x" + Long.toHexString(logicalclock.get()) +
                        " (n.round), " + sid + " (recipient), " + self.getId() +
                        " (1), 0x" + Long.toHexString(proposedEpoch) + " (n.peerEpoch)");
            }
            // 添加到消息发送队列，WorkerSender线程将负责消息发送
            sendqueue.offer(notmsg);
        }
    }

    private void printNotification(Notification n) {
        LOG.info("Notification: "
                + Long.toHexString(n.version) + " (message format version), "
                + n.leader + " (n.leader), 0x"
                + Long.toHexString(n.zxid) + " (n.zxid), 0x"
                + Long.toHexString(n.electionEpoch) + " (n.round), " + n.state
                + " (n.state), " + n.sid + " (n.sid), 0x"
                + Long.toHexString(n.peerEpoch) + " (n.peerEPoch), "
                + self.getPeerState() + " (my state)"
                + (n.qv != null ? (Long.toHexString(n.qv.getVersion()) + " (n.config version)") : ""));
    }


    /**
     * 检查一对（服务器ID，zxid）成功我们目前的投票。
     *
     * @param id   Server identifier
     * @param zxid Last zxid observed by the issuer of this vote
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" +
                Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));
        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /*
         * 如果以下三种情况之一成立，我们将返回true：
         * 1- 新纪元更高（接收到的投票消息中，时代比本地节点的时代更高）
         * 2- 新纪元与当前纪元相同，但新zxid更高（时代相同时，比较zxid（事务id））
         * 3- 新时期与当前时期相同，新zxid与当前zxid相同，但是服务器ID更高。（时代和zxid相同时，比较sid，即先启动的将被获选）
         */
        return ((newEpoch > curEpoch) ||
                ((newEpoch == curEpoch) &&
                        ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * Termination predicate.
     * 给定一组投票，确定是否有足够的权力宣布选举回合结束。
     *
     * @param votes Set of votes
     * @param vote  Identifier of the vote received last
     */
    protected boolean termPredicate(Map<Long, Vote> votes, Vote vote) {
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        // 将 当前节点 添加到 voteSet，addQuorumVerifier() 将统计对应节点的得票数
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        // 选出最后的验证者
        if (self.getLastSeenQuorumVerifier() != null
                && self.getLastSeenQuorumVerifier().getVersion() > self
                .getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * 统计每个节点的得票数。
         *
         * 这里实则是通过一个双重循环，统计每个参选节点的得票情况。
         *
         * 有时，对等体将根据时间对服务器使用不同的zxid。
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                // addAck() 查找统计当前循环节点的得票数
                voteSet.addAck(entry.getKey());
            }
        }

        // 是否超过半数节点进行投票
        return voteSet.hasAllQuorums();
    }

    /**
     * 在这种情况下有民选的领导者，并支持这位领导人的法定人数，我们要检查如果领导者投票，并获得确认，它是领先的。
     * 我们需要这种检查，以避免同业保持选举遍地已崩溃同行，它不再是领先的。
     *
     * @param votes         set of votes
     * @param leader        leader id
     * @param electionEpoch epoch id
     */
    protected boolean checkLeader(
            Map<Long, Vote> votes,
            long leader,
            long electionEpoch) {

        boolean predicate = true;

        /*
         * 如果其他所有人都认为我是领导者，那么我必须是领导者。
         *
         * 其他两项检查只是针对我不是领导者的情况。
         * 如果我不是领导者，并且没有收到来自领导者的消息指出领导者，则为假。
         *
         * 有人投票你不是Leader，则Leader与可能是别的节点，因此返回false
         */
        if (leader != self.getId()) {
            if (votes.get(leader) == null) predicate = false;
            else if (votes.get(leader).getState() != ServerState.LEADING) predicate = false;
        } else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    synchronized void updateProposal(long leader, long zxid, long epoch) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x"
                    + Long.toHexString(zxid) + " (newzxid), " + proposedLeader
                    + " (oldleader), 0x" + Long.toHexString(proposedZxid) + " (oldzxid)");
        }
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    synchronized public Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * 返回服务器标识符的初始表决值。
     *
     * @return long
     */
    private long getInitId() {
        if (self.getQuorumVerifier().getVotingMembers().containsKey(self.getId()))
            return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            try {
                return self.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        else return Long.MIN_VALUE;
    }

    /**
     * 开始新一轮的领导人选举。每当我们的QuorumPeer 将其状态更改为LOOKING时，就会调用此方法，并且将通知发送给所有其他对等方。
     */
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(
                    self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        if (self.start_fle == 0) {
            self.start_fle = Time.currentElapsedTime();
        }
        try {
            // 记录接收到的所有节点的，投票结果
            HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();
            // 在选出Leader之后接收到的选举消息
            HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = finalizeWait;

            synchronized (this) {
                // 逻辑时钟
                logicalclock.incrementAndGet();
                // 设置默认 提议的Leader、zxid、时代
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info("New election. My id =  " + self.getId() +
                    ", proposed zxid=0x" + Long.toHexString(proposedZxid));
            // 默认广播消息给所有参与选举的节点，提议选举自己作为Leader
            sendNotifications();

            /*
             * 在其中交换通知直到找到领导者的循环
             */
            while ((self.getPeerState() == ServerState.LOOKING) &&
                    (!stop)) {
                /*
                 * 从队列中删除下一个通知，在2次终止时间后超时
                 *
                 * n是来自其他的对等节点的 投票（提议）信息
                 */
                Notification n = recvqueue.poll(notTimeout,
                        TimeUnit.MILLISECONDS);

                /*
                 * 如果未收到足够的信息，则发送更多通知。
                 * 否则，将处理新的通知。
                 */
                if (n == null) {
                    // 当所有消息都发送完毕后，重复发送最后一条消息
                    if (manager.haveDelivered()) {
                        sendNotifications();
                    }
                    // 否则尝试与所有参选节点建立连接
                    else {
                        manager.connectAll();
                    }

                    /*
                     * 超时时间，采用指数退避策略
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = (tmpTimeOut < maxNotificationInterval ?
                            tmpTimeOut : maxNotificationInterval);
                    LOG.info("Notification time out: " + notTimeout);


                }
                /*
                 * 仅当投票来自当前或下一个投票视图中的副本时，才能进行投票。
                 *
                 * 验证 提议节点 以及 提议的Leader节点 是否属于 当前轮次的选举中
                 */
                else if (validVoter(n.sid) && validVoter(n.leader)) {
                    switch (n.state) {
                        case LOOKING:
                            // 如果接收的electionEpoch（选举时代） > 当前节点所处选举时代 则替换并发送消息，下文将之称为“选票节点”
                            if (n.electionEpoch > logicalclock.get()) {
                                // 更新本地 选举时代
                                logicalclock.set(n.electionEpoch);
                                recvset.clear();

                                // 比较投票节点与本地节点的 electionEpoch（选举时代）、zxid、sid
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                        getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                    // 当投票节点获胜时，将 本地选票结果 更改成 选举投票节点 作为Leader
                                    updateProposal(n.leader, n.zxid, n.peerEpoch);
                                }
                                // 否则仍然投票上一轮结果（自己）作为Leader
                                else {
                                    updateProposal(getInitId(),
                                            getInitLastLoggedZxid(),
                                            getPeerEpoch());
                                }
                                // 广播消息
                                sendNotifications();
                            }
                            // 小于当前节点的electionEpoch（选举时代）时（接收到来自之前选举时代的节点发送过来的消息），直接跳过
                            else if (n.electionEpoch < logicalclock.get()) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x"
                                            + Long.toHexString(n.electionEpoch)
                                            + ", logicalclock=0x" + Long.toHexString(logicalclock.get()));
                                }
                                break;
                            }
                            // 相同时代时，判断选举节点是否获胜，即比较zxid、sid
                            else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                    proposedLeader, proposedZxid, proposedEpoch)) {
                                // 选举节点 获胜时，当前节点将跟随其进行投票
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                                sendNotifications();
                            }

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Adding vote: from=" + n.sid +
                                        ", proposed leader=" + n.leader +
                                        ", proposed zxid=0x" + Long.toHexString(n.zxid) +
                                        ", proposed election epoch=0x" + Long.toHexString(n.electionEpoch));
                            }

                            // 如果版本为LOOKING，则无需在意
                            // 记录选票节点的选举结果
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                            // 超过半数节点参与投票后，可结束选举
                            if (termPredicate(recvset,
                                    new Vote(proposedLeader, proposedZxid,
                                            logicalclock.get(), proposedEpoch))) {

                                /*
                                    这里再次充recvqueue 拉取，若又有新的投票过来，则会判断投票是否相同。
                                    如果为不同投票，则又添加回recvqueue，break，回到最初的循环，继续处理投票结果。

                                    这里再次获取新投票的目的？
                                    选举超过半数节点完成投票，仍然只能证明proposedLeader节点有概率当选Leader，因此当接收到新的消息后，直接break结束。
                                 */
                                while ((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                    // 验证提议的领导者是否有任何变化
                                    if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                            proposedLeader, proposedZxid, proposedEpoch)) {
                                        recvqueue.put(n);
                                        break;
                                    }
                                }

                                /*
                                 * n==null，说明没有再接收到节点的投票，可以确定最终此轮的投票Leader的结果就是proposedLeader
                                 */
                                if (n == null) {
                                    // 判断当前节点是否为Leader，进而更改自身状态
                                    self.setPeerState((proposedLeader == self.getId()) ?
                                            ServerState.LEADING : learningState());
                                    Vote endVote = new Vote(proposedLeader,
                                            proposedZxid, logicalclock.get(),
                                            proposedEpoch);
                                    // 清除 recvqueue
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }
                            break;
                        case OBSERVING:
                            LOG.debug("Notification from observer: " + n.sid);
                            break;
                        case FOLLOWING:
                        case LEADING:
                            /*
                             * 同时考虑来自同一纪元的所有通知。
                             *
                             * 当前节点已经成为Leader，此时收到来自其他节点的投票信息
                             */
                            if (n.electionEpoch == logicalclock.get()) {
                                recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                                if (termPredicate(recvset, new Vote(n.version, n.leader,
                                        n.zxid, n.electionEpoch, n.peerEpoch, n.state))
                                        // 判断投票的Leader 是不是自己，不是自己则需要重新计算选票
                                        && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                    // 投票Leader是自己的情况
                                    self.setPeerState((n.leader == self.getId()) ?
                                            ServerState.LEADING : learningState());
                                    Vote endVote = new Vote(n.leader,
                                            n.zxid, n.electionEpoch, n.peerEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }

                            /*
                             * 重新计算选票
                             */
                            outofelection.put(n.sid, new Vote(n.version, n.leader,
                                    n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                            if (termPredicate(outofelection, new Vote(n.version, n.leader,
                                    n.zxid, n.electionEpoch, n.peerEpoch, n.state))
                                    // 新投票的Leader获胜时，将会更改Leader
                                    && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                synchronized (this) {
                                    logicalclock.set(n.electionEpoch);
                                    self.setPeerState((n.leader == self.getId()) ?
                                            ServerState.LEADING : learningState());
                                }
                                Vote endVote = new Vote(n.leader, n.zxid,
                                        n.electionEpoch, n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                            break;
                        default:
                            LOG.warn("Notification state unrecoginized: " + n.state
                                    + " (n.state), " + n.sid + " (n.sid)");
                            break;
                    }
                } else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(
                            self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}",
                    manager.getConnectionThreadCount());
        }
    }

    /**
     * 检查给定sid是否在当前或下一个投票视图中表示
     *
     * @param sid Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }
}
