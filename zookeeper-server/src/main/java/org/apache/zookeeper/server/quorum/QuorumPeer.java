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

import org.apache.zookeeper.KeeperException.BadArgumentsException;
import org.apache.zookeeper.common.AtomicFileWritingIdiom;
import org.apache.zookeeper.common.AtomicFileWritingIdiom.WriterStatement;
import org.apache.zookeeper.common.QuorumX509Util;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.admin.AdminServerFactory;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.auth.*;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class manages the quorum protocol. There are three states this server
 * can be in:
 * <ol>
 * <li>Leader election - each server will elect a leader (proposing itself as a
 * leader initially).</li>
 * <li>Follower - the server will synchronize with the leader and replicate any
 * transactions.</li>
 * <li>Leader - the server will process requests and forward them to followers.
 * A majority of followers must log the request before it can be accepted.
 * </ol>
 * <p>
 * This class will setup a datagram socket that will always respond with its
 * view of the current leader. The response will take the form of:
 *
 * <pre>
 * int xid;
 *
 * long 1;
 *
 * long leader_id;
 *
 * long leader_zxid;
 * </pre>
 * <p>
 * The request for the current leader will consist solely of an xid: int xid;
 */
public class QuorumPeer extends ZooKeeperThread implements QuorumStats.Provider {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeer.class);

    private QuorumBean jmxQuorumBean;
    LocalPeerBean jmxLocalPeerBean;
    private Map<Long, RemotePeerBean> jmxRemotePeerBean;
    LeaderElectionBean jmxLeaderElectionBean;

    /**
     * QuorumCnxManager通过AtomicReference进行保存，以确保更新的跨线程可见性。
     * 请参见setLastSeenQuorumVerifier（）上的实现注释。
     */
    private AtomicReference<QuorumCnxManager> qcmRef = new AtomicReference<>();

    QuorumAuthServer authServer;
    QuorumAuthLearner authLearner;

    /**
     * ZKDatabase是quorumpeer的顶层部件，其将在所有后面实例化的zookeeperservers使用。
     * 此外，一旦在启动时创建的，只有扔掉一个截断消息的情况下，从领导者
     */
    private ZKDatabase zkDb;

    public static final class AddressTuple {
        public final InetSocketAddress quorumAddr;
        public final InetSocketAddress electionAddr;
        public final InetSocketAddress clientAddr;

        public AddressTuple(InetSocketAddress quorumAddr, InetSocketAddress electionAddr, InetSocketAddress clientAddr) {
            this.quorumAddr = quorumAddr;
            this.electionAddr = electionAddr;
            this.clientAddr = clientAddr;
        }
    }

    public static class QuorumServer {
        public InetSocketAddress addr = null;

        public InetSocketAddress electionAddr = null;

        public InetSocketAddress clientAddr = null;

        public long id;

        public String hostname;

        public LearnerType type = LearnerType.PARTICIPANT;

        private List<InetSocketAddress> myAddrs;

        public QuorumServer(long id, InetSocketAddress addr,
                            InetSocketAddress electionAddr, InetSocketAddress clientAddr) {
            this(id, addr, electionAddr, clientAddr, LearnerType.PARTICIPANT);
        }

        public QuorumServer(long id, InetSocketAddress addr,
                            InetSocketAddress electionAddr) {
            this(id, addr, electionAddr, (InetSocketAddress) null, LearnerType.PARTICIPANT);
        }

        // VisibleForTesting
        public QuorumServer(long id, InetSocketAddress addr) {
            this(id, addr, (InetSocketAddress) null, (InetSocketAddress) null, LearnerType.PARTICIPANT);
        }

        public long getId() {
            return id;
        }

        /**
         * 对服务器地址和选举地址执行DNS查找。
         * <p>
         * 如果DNS查找失败，则this.addr和eletementAddr保持不变。
         */
        public void recreateSocketAddresses() {
            if (this.addr == null) {
                LOG.warn("Server address has not been initialized");
                return;
            }
            if (this.electionAddr == null) {
                LOG.warn("Election address has not been initialized");
                return;
            }
            String host = this.addr.getHostString();
            InetAddress address = null;
            try {
                address = InetAddress.getByName(host);
            } catch (UnknownHostException ex) {
                LOG.warn("Failed to resolve address: {}", host, ex);
                return;
            }
            LOG.debug("Resolved address for {}: {}", host, address);
            int port = this.addr.getPort();
            this.addr = new InetSocketAddress(address, port);
            port = this.electionAddr.getPort();
            this.electionAddr = new InetSocketAddress(address, port);
        }

        private void setType(String s) throws ConfigException {
            if (s.toLowerCase().equals("observer")) {
                type = LearnerType.OBSERVER;
            } else if (s.toLowerCase().equals("participant")) {
                type = LearnerType.PARTICIPANT;
            } else {
                throw new ConfigException("Unrecognised peertype: " + s);
            }
        }

        private static String[] splitWithLeadingHostname(String s)
                throws ConfigException {
            /* Does it start with an IPv6 literal? */
            if (s.startsWith("[")) {
                int i = s.indexOf("]:");
                if (i < 0) {
                    throw new ConfigException(s + " starts with '[' but has no matching ']:'");
                }

                String[] sa = s.substring(i + 2).split(":");
                String[] nsa = new String[sa.length + 1];
                nsa[0] = s.substring(1, i);
                System.arraycopy(sa, 0, nsa, 1, sa.length);

                return nsa;
            } else {
                return s.split(":");
            }
        }

        private static final String wrongFormat = " does not have the form server_config or server_config;client_config" +
                " where server_config is host:port:port or host:port:port:type and client_config is port or host:port";

        public QuorumServer(long sid, String addressStr) throws ConfigException {
            // LOG.warn("sid = " + sid + " addressStr = " + addressStr);
            this.id = sid;
            String serverClientParts[] = addressStr.split(";");
            String serverParts[] = splitWithLeadingHostname(serverClientParts[0]);
            if ((serverClientParts.length > 2) || (serverParts.length < 3)
                    || (serverParts.length > 4)) {
                throw new ConfigException(addressStr + wrongFormat);
            }

            if (serverClientParts.length == 2) {
                //LOG.warn("ClientParts: " + serverClientParts[1]);
                String clientParts[] = splitWithLeadingHostname(serverClientParts[1]);
                if (clientParts.length > 2) {
                    throw new ConfigException(addressStr + wrongFormat);
                }

                // is client_config a host:port or just a port
                hostname = (clientParts.length == 2) ? clientParts[0] : "0.0.0.0";
                try {
                    clientAddr = new InetSocketAddress(hostname,
                            Integer.parseInt(clientParts[clientParts.length - 1]));
                    //LOG.warn("Set clientAddr to " + clientAddr);
                } catch (NumberFormatException e) {
                    throw new ConfigException("Address unresolved: " + hostname + ":" + clientParts[clientParts.length - 1]);
                }
            }

            // server_config should be either host:port:port or host:port:port:type
            try {
                addr = new InetSocketAddress(serverParts[0],
                        Integer.parseInt(serverParts[1]));
            } catch (NumberFormatException e) {
                throw new ConfigException("Address unresolved: " + serverParts[0] + ":" + serverParts[1]);
            }
            try {
                electionAddr = new InetSocketAddress(serverParts[0],
                        Integer.parseInt(serverParts[2]));
            } catch (NumberFormatException e) {
                throw new ConfigException("Address unresolved: " + serverParts[0] + ":" + serverParts[2]);
            }

            if (addr.getPort() == electionAddr.getPort()) {
                throw new ConfigException(
                        "Client and election port must be different! Please update the configuration file on server." + sid);
            }

            if (serverParts.length == 4) {
                setType(serverParts[3]);
            }

            this.hostname = serverParts[0];

            setMyAddrs();
        }

        public QuorumServer(long id, InetSocketAddress addr,
                            InetSocketAddress electionAddr, LearnerType type) {
            this(id, addr, electionAddr, (InetSocketAddress) null, type);
        }

        public QuorumServer(long id, InetSocketAddress addr,
                            InetSocketAddress electionAddr, InetSocketAddress clientAddr, LearnerType type) {
            this.id = id;
            this.addr = addr;
            this.electionAddr = electionAddr;
            this.type = type;
            this.clientAddr = clientAddr;

            setMyAddrs();
        }

        private void setMyAddrs() {
            this.myAddrs = new ArrayList<InetSocketAddress>();
            this.myAddrs.add(this.addr);
            this.myAddrs.add(this.clientAddr);
            this.myAddrs.add(this.electionAddr);
            this.myAddrs = excludedSpecialAddresses(this.myAddrs);
        }

        private static String delimitedHostString(InetSocketAddress addr) {
            String host = addr.getHostString();
            if (host.contains(":")) {
                return "[" + host + "]";
            } else {
                return host;
            }
        }

        public String toString() {
            StringWriter sw = new StringWriter();
            //addr should never be null, but just to make sure
            if (addr != null) {
                sw.append(delimitedHostString(addr));
                sw.append(":");
                sw.append(String.valueOf(addr.getPort()));
            }
            if (electionAddr != null) {
                sw.append(":");
                sw.append(String.valueOf(electionAddr.getPort()));
            }
            if (type == LearnerType.OBSERVER) sw.append(":observer");
            else if (type == LearnerType.PARTICIPANT) sw.append(":participant");
            if (clientAddr != null) {
                sw.append(";");
                sw.append(delimitedHostString(clientAddr));
                sw.append(":");
                sw.append(String.valueOf(clientAddr.getPort()));
            }
            return sw.toString();
        }

        public int hashCode() {
            assert false : "hashCode not designed";
            return 42; // any arbitrary constant will do
        }

        private boolean checkAddressesEqual(InetSocketAddress addr1, InetSocketAddress addr2) {
            if ((addr1 == null && addr2 != null) ||
                    (addr1 != null && addr2 == null) ||
                    (addr1 != null && addr2 != null && !addr1.equals(addr2))) return false;
            return true;
        }

        public boolean equals(Object o) {
            if (!(o instanceof QuorumServer)) return false;
            QuorumServer qs = (QuorumServer) o;
            if ((qs.id != id) || (qs.type != type)) return false;
            if (!checkAddressesEqual(addr, qs.addr)) return false;
            if (!checkAddressesEqual(electionAddr, qs.electionAddr)) return false;
            if (!checkAddressesEqual(clientAddr, qs.clientAddr)) return false;
            return true;
        }

        public void checkAddressDuplicate(QuorumServer s) throws BadArgumentsException {
            List<InetSocketAddress> otherAddrs = new ArrayList<InetSocketAddress>();
            otherAddrs.add(s.addr);
            otherAddrs.add(s.clientAddr);
            otherAddrs.add(s.electionAddr);
            otherAddrs = excludedSpecialAddresses(otherAddrs);

            for (InetSocketAddress my : this.myAddrs) {

                for (InetSocketAddress other : otherAddrs) {
                    if (my.equals(other)) {
                        String error = String.format("%s of server.%d conflicts %s of server.%d", my, this.id, other, s.id);
                        throw new BadArgumentsException(error);
                    }
                }
            }
        }

        private List<InetSocketAddress> excludedSpecialAddresses(List<InetSocketAddress> addrs) {
            List<InetSocketAddress> included = new ArrayList<InetSocketAddress>();
            InetAddress wcAddr = new InetSocketAddress(0).getAddress();

            for (InetSocketAddress addr : addrs) {
                if (addr == null) {
                    continue;
                }
                InetAddress inetaddr = addr.getAddress();

                if (inetaddr == null ||
                        inetaddr.equals(wcAddr) || // wildCard address(0.0.0.0)
                        inetaddr.isLoopbackAddress()) { // loopback address(localhost/127.0.0.1)
                    continue;
                }
                included.add(addr);
            }
            return included;
        }
    }


    public enum ServerState {
        LOOKING, FOLLOWING, LEADING, OBSERVING;
    }

    /*
     * A peer can either be participating, which implies that it is willing to
     * both vote in instances of consensus and to elect or become a Leader, or
     * it may be observing in which case it isn't.
     *
     * We need this distinction to decide which ServerState to move to when
     * conditions change (e.g. which state to become after LOOKING).
     */
    public enum LearnerType {
        PARTICIPANT, OBSERVER;
    }

    /*
     * 为了使观察者没有标识符，我们至少对于QuorumCnxManager需要一个通用标识符。
     * 我们使用以下常量作为此类通用标识符的值。
     */
    static final long OBSERVER_ID = Long.MAX_VALUE;

    /*
     * Record leader election time
     */
    public long start_fle, end_fle; // fle = fast leader election
    public static final String FLE_TIME_UNIT = "MS";

    /*
     * Default value of peer is participant
     */
    private LearnerType learnerType = LearnerType.PARTICIPANT;

    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * Sets the LearnerType
     */
    public void setLearnerType(LearnerType p) {
        learnerType = p;
    }

    protected synchronized void setConfigFileName(String s) {
        configFilename = s;
    }

    private String configFilename = null;

    public int getQuorumSize() {
        return getVotingView().size();
    }

    /**
     * QuorumVerifier implementation; default (majority).
     */

    //last committed quorum verifier
    private QuorumVerifier quorumVerifier;

    //last proposed quorum verifier
    private QuorumVerifier lastSeenQuorumVerifier = null;

    // Lock object that guard access to quorumVerifier and lastSeenQuorumVerifier.
    final Object QV_LOCK = new Object();


    /**
     * My id
     */
    private long myid;


    /**
     * get the id of this quorum peer.
     */
    public long getId() {
        return myid;
    }

    // VisibleForTesting
    void setId(long id) {
        this.myid = id;
    }

    private boolean sslQuorum;
    private boolean shouldUsePortUnification;

    public boolean isSslQuorum() {
        return sslQuorum;
    }

    public boolean shouldUsePortUnification() {
        return shouldUsePortUnification;
    }

    private final QuorumX509Util x509Util;

    QuorumX509Util getX509Util() {
        return x509Util;
    }

    /**
     * 我认为领导者目前就是这个人。
     */
    volatile private Vote currentVote;

    public synchronized Vote getCurrentVote() {
        return currentVote;
    }

    public synchronized void setCurrentVote(Vote v) {
        currentVote = v;
    }

    private volatile boolean running = true;

    /**
     * The number of milliseconds of each tick
     */
    protected int tickTime;

    /**
     * Whether learners in this quorum should create new sessions as local.
     * False by default to preserve existing behavior.
     */
    protected boolean localSessionsEnabled = false;

    /**
     * Whether learners in this quorum should upgrade local sessions to
     * global. Only matters if local sessions are enabled.
     */
    protected boolean localSessionsUpgradingEnabled = true;

    /**
     * Minimum number of milliseconds to allow for session timeout.
     * A value of -1 indicates unset, use default.
     */
    protected int minSessionTimeout = -1;

    /**
     * Maximum number of milliseconds to allow for session timeout.
     * A value of -1 indicates unset, use default.
     */
    protected int maxSessionTimeout = -1;

    /**
     * The number of ticks that the initial synchronization phase can take
     */
    protected int initLimit;

    /**
     * The number of ticks that can pass between sending a request and getting
     * an acknowledgment
     */
    protected int syncLimit;

    /**
     * Enables/Disables sync request processor. This option is enabled
     * by default and is to be used with observers.
     */
    protected boolean syncEnabled = true;

    /**
     * The current tick
     */
    protected AtomicInteger tick = new AtomicInteger();

    /**
     * Whether or not to listen on all IPs for the two quorum ports
     * (broadcast and fast leader election).
     */
    protected boolean quorumListenOnAllIPs = false;

    /**
     * Keeps time taken for leader election in milliseconds. Sets the value to
     * this variable only after the completion of leader election.
     */
    private long electionTimeTaken = -1;

    /**
     * Enable/Disables quorum authentication using sasl. Defaulting to false.
     */
    protected boolean quorumSaslEnableAuth;

    /**
     * If this is false, quorum peer server will accept another quorum peer client
     * connection even if the authentication did not succeed. This can be used while
     * upgrading ZooKeeper server. Defaulting to false (required).
     */
    protected boolean quorumServerSaslAuthRequired;

    /**
     * If this is false, quorum peer learner will talk to quorum peer server
     * without authentication. This can be used while upgrading ZooKeeper
     * server. Defaulting to false (required).
     */
    protected boolean quorumLearnerSaslAuthRequired;

    /**
     * Kerberos quorum service principal. Defaulting to 'zkquorum/localhost'.
     */
    protected String quorumServicePrincipal;

    /**
     * Quorum learner login context name in jaas-conf file to read the kerberos
     * security details. Defaulting to 'QuorumLearner'.
     */
    protected String quorumLearnerLoginContext;

    /**
     * Quorum server login context name in jaas-conf file to read the kerberos
     * security details. Defaulting to 'QuorumServer'.
     */
    protected String quorumServerLoginContext;

    // TODO: need to tune the default value of thread size
    private static final int QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE = 20;
    /**
     * The maximum number of threads to allow in the connectionExecutors thread
     * pool which will be used to initiate quorum server connections.
     */
    protected int quorumCnxnThreadsSize = QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE;

    /**
     * @deprecated As of release 3.4.0, this class has been deprecated, since
     * it is used with one of the udp-based versions of leader election, which
     * we are also deprecating.
     * <p>
     * This class simply responds to requests for the current leader of this
     * node.
     * <p>
     * The request contains just an xid generated by the requestor.
     * <p>
     * The response has the xid, the id of this server, the id of the leader,
     * and the zxid of the leader.
     */
    @Deprecated
    class ResponderThread extends ZooKeeperThread {
        ResponderThread() {
            super("ResponderThread");
        }

        volatile boolean running = true;

        @Override
        public void run() {
            try {
                byte b[] = new byte[36];
                ByteBuffer responseBuffer = ByteBuffer.wrap(b);
                DatagramPacket packet = new DatagramPacket(b, b.length);
                while (running) {
                    udpSocket.receive(packet);
                    if (packet.getLength() != 4) {
                        LOG.warn("Got more than just an xid! Len = "
                                + packet.getLength());
                    } else {
                        responseBuffer.clear();
                        responseBuffer.getInt(); // Skip the xid
                        responseBuffer.putLong(1);
                        Vote current = getCurrentVote();
                        switch (getPeerState()) {
                            case LOOKING:
                                responseBuffer.putLong(current.getId());
                                responseBuffer.putLong(current.getZxid());
                                break;
                            case LEADING:
                                responseBuffer.putLong(1);
                                try {
                                    long proposed;
                                    synchronized (leader) {
                                        proposed = leader.lastProposed;
                                    }
                                    responseBuffer.putLong(proposed);
                                } catch (NullPointerException npe) {
                                    // This can happen in state transitions,
                                    // just ignore the request
                                }
                                break;
                            case FOLLOWING:
                                responseBuffer.putLong(current.getId());
                                try {
                                    responseBuffer.putLong(follower.getZxid());
                                } catch (NullPointerException npe) {
                                    // This can happen in state transitions,
                                    // just ignore the request
                                }
                                break;
                            case OBSERVING:
                                // Do nothing, Observers keep themselves to
                                // themselves.
                                break;
                        }
                        packet.setData(b);
                        udpSocket.send(packet);
                    }
                    packet.setLength(b.length);
                }
            } catch (RuntimeException e) {
                LOG.warn("Unexpected runtime exception in ResponderThread", e);
            } catch (IOException e) {
                LOG.warn("Unexpected IO exception in ResponderThread", e);
            } finally {
                LOG.warn("QuorumPeer responder thread exited");
            }
        }
    }

    private ServerState state = ServerState.LOOKING;

    private boolean reconfigFlag = false; // indicates that a reconfig just committed

    public synchronized void setPeerState(ServerState newState) {
        state = newState;
    }

    public synchronized void reconfigFlagSet() {
        reconfigFlag = true;
    }

    public synchronized void reconfigFlagClear() {
        reconfigFlag = false;
    }

    public synchronized boolean isReconfigStateChange() {
        return reconfigFlag;
    }

    public synchronized ServerState getPeerState() {
        return state;
    }

    DatagramSocket udpSocket;

    private final AtomicReference<AddressTuple> myAddrs = new AtomicReference<>();

    /**
     * Resolves hostname for a given server ID.
     * <p>
     * This method resolves hostname for a given server ID in both quorumVerifer
     * and lastSeenQuorumVerifier. If the server ID matches the local server ID,
     * it also updates myAddrs.
     */
    public void recreateSocketAddresses(long id) {
        QuorumVerifier qv = getQuorumVerifier();
        if (qv != null) {
            QuorumServer qs = qv.getAllMembers().get(id);
            if (qs != null) {
                qs.recreateSocketAddresses();
                if (id == getId()) {
                    setAddrs(qs.addr, qs.electionAddr, qs.clientAddr);
                }
            }
        }
        qv = getLastSeenQuorumVerifier();
        if (qv != null) {
            QuorumServer qs = qv.getAllMembers().get(id);
            if (qs != null) {
                qs.recreateSocketAddresses();
            }
        }
    }

    private AddressTuple getAddrs() {
        AddressTuple addrs = myAddrs.get();
        if (addrs != null) {
            return addrs;
        }
        try {
            synchronized (QV_LOCK) {
                addrs = myAddrs.get();
                while (addrs == null) {
                    QV_LOCK.wait();
                    addrs = myAddrs.get();
                }
                return addrs;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public InetSocketAddress getQuorumAddress() {
        return getAddrs().quorumAddr;
    }

    public InetSocketAddress getElectionAddress() {
        return getAddrs().electionAddr;
    }

    public InetSocketAddress getClientAddress() {
        final AddressTuple addrs = myAddrs.get();
        return (addrs == null) ? null : addrs.clientAddr;
    }

    private void setAddrs(InetSocketAddress quorumAddr, InetSocketAddress electionAddr, InetSocketAddress clientAddr) {
        synchronized (QV_LOCK) {
            myAddrs.set(new AddressTuple(quorumAddr, electionAddr, clientAddr));
            QV_LOCK.notifyAll();
        }
    }

    private int electionType;

    Election electionAlg;

    ServerCnxnFactory cnxnFactory;
    ServerCnxnFactory secureCnxnFactory;

    private FileTxnSnapLog logFactory = null;

    private final QuorumStats quorumStats;

    AdminServer adminServer;

    public static QuorumPeer testingQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }

    public QuorumPeer() throws SaslException {
        super("QuorumPeer");
        quorumStats = new QuorumStats(this);
        jmxRemotePeerBean = new HashMap<Long, RemotePeerBean>();
        adminServer = AdminServerFactory.createAdminServer();
        x509Util = new QuorumX509Util();
        initialize();
    }

    /**
     * For backward compatibility purposes, we instantiate QuorumMaj by default.
     */

    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File dataDir,
                      File dataLogDir, int electionType,
                      long myid, int tickTime, int initLimit, int syncLimit,
                      ServerCnxnFactory cnxnFactory) throws IOException {
        this(quorumPeers, dataDir, dataLogDir, electionType, 1, tickTime,
                initLimit, syncLimit, false, cnxnFactory,
                new QuorumMaj(quorumPeers));
    }

    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File dataDir,
                      File dataLogDir, int electionType,
                      long myid, int tickTime, int initLimit, int syncLimit,
                      boolean quorumListenOnAllIPs,
                      ServerCnxnFactory cnxnFactory,
                      QuorumVerifier quorumConfig) throws IOException {
        this();
        this.cnxnFactory = cnxnFactory;
        this.electionType = electionType;
        this.myid = myid;
        this.tickTime = tickTime;
        this.initLimit = initLimit;
        this.syncLimit = syncLimit;
        this.quorumListenOnAllIPs = quorumListenOnAllIPs;
        this.logFactory = new FileTxnSnapLog(dataLogDir, dataDir);
        this.zkDb = new ZKDatabase(this.logFactory);
        if (quorumConfig == null) quorumConfig = new QuorumMaj(quorumPeers);
        setQuorumVerifier(quorumConfig, false);
        adminServer = AdminServerFactory.createAdminServer();
    }

    public void initialize() throws SaslException {
        // 初始 仲裁授权 server & learner
        if (isQuorumSaslAuthEnabled()) {
            Set<String> authzHosts = new HashSet<String>();
            for (QuorumServer qs : getView().values()) {
                authzHosts.add(qs.hostname);
            }
            authServer = new SaslQuorumAuthServer(isQuorumServerSaslAuthRequired(),
                    quorumServerLoginContext, authzHosts);
            authLearner = new SaslQuorumAuthLearner(isQuorumLearnerSaslAuthRequired(),
                    quorumServicePrincipal, quorumLearnerLoginContext);
        } else {
            authServer = new NullQuorumAuthServer();
            authLearner = new NullQuorumAuthLearner();
        }
    }

    QuorumStats quorumStats() {
        return quorumStats;
    }

    @Override
    public synchronized void start() {
        // 1 标识Zookeeper集群服务器节点的唯一性，所以这里是在做唯一性校验
        if (!getView().containsKey(myid)) {
            throw new RuntimeException("My id " + myid + " not in the peer list");
        }
        // 加载数据库，存放着树结构 TODO FileTxnSnapLog 工具类将snap 和 transaction log反序列化到内存中的
        loadDataBase();
        // 设置cnxn工厂
        startServerCnxnFactory();
        try {
            // 启动admin服务
            adminServer.start();
        } catch (AdminServerException e) {
            LOG.warn("Problem starting AdminServer", e);
            System.out.println(e);
        }
        // 开始Leader选举
        startLeaderElection();
        // TODO 启动父类 QuorumPeer 线程，这里start就是在调用run方法
        super.start();
    }

    private void loadDataBase() {
        try {
            zkDb.loadDataBase();

            // 获得最后一个 epochs，epochs为时代，每一任Leader有一个不同的时代标识
            long lastProcessedZxid = zkDb.getDataTree().lastProcessedZxid;
            // 从Zxid获取时代，zxid由两部分组成，前32位为epochs，后32位为事务计数器
            long epochOfZxid = ZxidUtils.getEpochFromZxid(lastProcessedZxid);
            try {
                // TODO 快照
                currentEpoch = readLongFromFile(CURRENT_EPOCH_FILENAME);
            } catch (FileNotFoundException e) {
                // 择一个合理的纪元数字 TODO 集群节点升级时才会发生无法读取到 epoch 的情况
                // 仅在移至新的代码版本时才发生一次
                currentEpoch = epochOfZxid;
                LOG.info(CURRENT_EPOCH_FILENAME
                                + " not found! Creating with a reasonable default of {}. This should only happen when you are upgrading your installation",
                        currentEpoch);
                writeLongToFile(CURRENT_EPOCH_FILENAME, currentEpoch);
            }
            // 当前时代早于最后一个zxid所属时代，需要重启重新同步数据
            if (epochOfZxid > currentEpoch) {
                throw new IOException("The current epoch, " + ZxidUtils.zxidToString(currentEpoch) + ", is older than the last zxid, " + lastProcessedZxid);
            }
            try {
                acceptedEpoch = readLongFromFile(ACCEPTED_EPOCH_FILENAME);
            } catch (FileNotFoundException e) {
                //选择一个合理的纪元数字
                // 仅在移至新的代码版本时才发生一次
                acceptedEpoch = epochOfZxid;
                LOG.info(ACCEPTED_EPOCH_FILENAME
                                + " not found! Creating with a reasonable default of {}. This should only happen when you are upgrading your installation",
                        acceptedEpoch);
                writeLongToFile(ACCEPTED_EPOCH_FILENAME, acceptedEpoch);
            }
            if (acceptedEpoch < currentEpoch) {
                throw new IOException("The accepted epoch, " + ZxidUtils.zxidToString(acceptedEpoch) + " is less than the current epoch, " + ZxidUtils.zxidToString(currentEpoch));
            }
        } catch (IOException ie) {
            LOG.error("Unable to load database on disk", ie);
            throw new RuntimeException("Unable to run quorum server ", ie);
        }
    }

    ResponderThread responder;

    synchronized public void stopLeaderElection() {
        responder.running = false;
        responder.interrupt();
    }

    synchronized public void startLeaderElection() {
        try {
            // TODO 对等节点状态
            if (getPeerState() == ServerState.LOOKING) {
                currentVote = new Vote(1, getLastLoggedZxid(), getCurrentEpoch());
            }
        } catch (IOException e) {
            RuntimeException re = new RuntimeException(e.getMessage());
            re.setStackTrace(e.getStackTrace());
            throw re;
        }

        // if (!getView().containsKey(1)) {
        //      throw new RuntimeException("My id " + 1 + " not in the peer list");
        //}
        if (electionType == 0) {
            try {
                udpSocket = new DatagramSocket(getQuorumAddress().getPort());
                responder = new ResponderThread();
                responder.start();
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
        }
        // 创建选举算法
        this.electionAlg = createElectionAlgorithm(electionType);
    }

    /**
     * Count the number of nodes in the map that could be followers.
     *
     * @param peers
     * @return The number of followers in the map
     */
    protected static int countParticipants(Map<Long, QuorumServer> peers) {
        int count = 0;
        for (QuorumServer q : peers.values()) {
            if (q.type == LearnerType.PARTICIPANT) {
                count++;
            }
        }
        return count;
    }

    /**
     * This constructor is only used by the existing unit test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File snapDir,
                      File logDir, int clientPort, int electionAlg,
                      long myid, int tickTime, int initLimit, int syncLimit)
            throws IOException {
        this(quorumPeers, snapDir, logDir, electionAlg, 1, tickTime, initLimit, syncLimit, false,
                ServerCnxnFactory.createFactory(getClientAddress(quorumPeers, myid, clientPort), -1),
                new QuorumMaj(quorumPeers));
    }

    /**
     * This constructor is only used by the existing unit test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File snapDir,
                      File logDir, int clientPort, int electionAlg,
                      long myid, int tickTime, int initLimit, int syncLimit,
                      QuorumVerifier quorumConfig)
            throws IOException {
        this(quorumPeers, snapDir, logDir, electionAlg,
                1, tickTime, initLimit, syncLimit, false,
                ServerCnxnFactory.createFactory(getClientAddress(quorumPeers, myid, clientPort), -1),
                quorumConfig);
    }

    private static InetSocketAddress getClientAddress(Map<Long, QuorumServer> quorumPeers, long myid, int clientPort)
            throws IOException {
        QuorumServer quorumServer = quorumPeers.get(myid);
        if (null == quorumServer) {
            throw new IOException("No QuorumServer correspoding to 1 " + 1);
        }
        if (null == quorumServer.clientAddr) {
            return new InetSocketAddress(clientPort);
        }
        if (quorumServer.clientAddr.getPort() != clientPort) {
            throw new IOException("QuorumServer port " + quorumServer.clientAddr.getPort()
                    + " does not match with given port " + clientPort);
        }
        return quorumServer.clientAddr;
    }

    /**
     * returns the highest zxid that this host has seen
     *
     * @return the highest zxid for this host
     */
    public long getLastLoggedZxid() {
        if (!zkDb.isInitialized()) {
            loadDataBase();
        }
        return zkDb.getDataTreeLastProcessedZxid();
    }

    public Follower follower;
    public Leader leader;
    public Observer observer;

    protected Follower makeFollower(FileTxnSnapLog logFactory) throws IOException {
        return new Follower(this, new FollowerZooKeeperServer(logFactory, this, this.zkDb));
    }

    protected Leader makeLeader(FileTxnSnapLog logFactory) throws IOException, X509Exception {
        return new Leader(this, new LeaderZooKeeperServer(logFactory, this, this.zkDb));
    }

    protected Observer makeObserver(FileTxnSnapLog logFactory) throws IOException {
        return new Observer(this, new ObserverZooKeeperServer(logFactory, this, this.zkDb));
    }

    @SuppressWarnings("deprecation")
    protected Election createElectionAlgorithm(int electionAlgorithm) {
        Election le = null;

        // TODO: 使用工厂而不是 switch
        switch (electionAlgorithm) {
            case 0:
                le = new LeaderElection(this);
                break;
            case 1:
                le = new AuthFastLeaderElection(this);
                break;
            case 2:
                le = new AuthFastLeaderElection(this, true);
                break;
            case 3:
                // QuorumCnxManager：维护选举期间网络IO的工具类
                QuorumCnxManager qcm = createCnxnManager();
                // 替换成新的CNXN连接，并将旧CNXN连接停止
                QuorumCnxManager oldQcm = qcmRef.getAndSet(qcm);
                if (oldQcm != null) {
                    // 破坏已经设置的QuorumCnxManager（重新启动领导者选举？）
                    LOG.warn("Clobbering already-set QuorumCnxManager (restarting leader election?)");
                    // 停止
                    oldQcm.halt();
                }
                // Listener是QuorumCnxManager内的一个线程类：负责启动选举监听端口并处理连接进来的Socket
                QuorumCnxManager.Listener listener = qcm.listener;
                if (listener != null) {
                    // Listener 将该客户端 Socket 封装成 RecvWorker 和 SendWorker，发送和接收线程
                    listener.start();
                    // FastLeaderElection 封装了具体选举算法的实现。
                    FastLeaderElection fle = new FastLeaderElection(this, qcm);
                    fle.start();
                    le = fle;
                } else {
                    LOG.error("Null listener when initializing cnx manager");
                }
                break;
            default:
                assert false;
        }
        return le;
    }

    @SuppressWarnings("deprecation")
    protected Election makeLEStrategy() {
        LOG.debug("Initializing leader election protocol...");
        if (getElectionType() == 0) {
            electionAlg = new LeaderElection(this);
        }
        return electionAlg;
    }

    synchronized protected void setLeader(Leader newLeader) {
        leader = newLeader;
    }

    synchronized protected void setFollower(Follower newFollower) {
        follower = newFollower;
    }

    synchronized protected void setObserver(Observer newObserver) {
        observer = newObserver;
    }

    synchronized public ZooKeeperServer getActiveServer() {
        if (leader != null)
            return leader.zk;
        else if (follower != null)
            return follower.zk;
        else if (observer != null)
            return observer.zk;
        return null;
    }

    boolean shuttingDownLE = false;

    @Override
    public void run() {
        // 调皮，修改了线程名称
        updateThreadName();

        LOG.debug("Starting quorum peer");
        try {
            // 向JMX 注册，主要用以监听JVM
            jmxQuorumBean = new QuorumBean(this);
            MBeanRegistry.getInstance().register(jmxQuorumBean, null);
            for (QuorumServer s : getView().values()) {
                ZKMBeanInfo p;
                if (getId() == s.id) {
                    p = jmxLocalPeerBean = new LocalPeerBean(this);
                    try {
                        MBeanRegistry.getInstance().register(p, jmxQuorumBean);
                    } catch (Exception e) {
                        LOG.warn("Failed to register with JMX", e);
                        jmxLocalPeerBean = null;
                    }
                } else {
                    RemotePeerBean rBean = new RemotePeerBean(this, s);
                    try {
                        MBeanRegistry.getInstance().register(rBean, jmxQuorumBean);
                        jmxRemotePeerBean.put(s.id, rBean);
                    } catch (Exception e) {
                        LOG.warn("Failed to register with JMX", e);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxQuorumBean = null;
        }

        try {
            /*
             * 主循环
             */
            while (running) {
                // 节点状态 -> 状态锁
                switch (getPeerState()) {
                    // Looking 选举状态
                    case LOOKING:
                        // 首先系统刚启动时 serverState 默认是 LOOKING，表示需要进行 Leader 选举，这时进入 Leader 选举状态中
                        LOG.info("LOOKING");

                        // 只读模式：指一个服务器与集群中过半机器失去连接以后，这个服务器就不在不处理客户端的请求，但我们仍然希望该服务器可以提供读服务。
                        if (Boolean.getBoolean("readonlymode.enabled")) {
                            LOG.info("Attempting to start ReadOnlyZooKeeperServer");

                            // 开启只读模式后，将启动一个只读的ZK服务节点
                            final ReadOnlyZooKeeperServer roZk =
                                    new ReadOnlyZooKeeperServer(logFactory, this, this.zkDb);

                            // 与其立即启动roZk，不如等待宽限期之后再决定分区。
                            // 这里使用线程是因为否则它将需要更改每个选举策略类，这是不必要的代码耦合。
                            Thread roZkMgr = new Thread() {
                                public void run() {
                                    try {
                                        // 下限宽限期至2秒
                                        sleep(Math.max(2000, tickTime));
                                        if (ServerState.LOOKING.equals(getPeerState())) {
                                            roZk.startup();
                                        }
                                    } catch (InterruptedException e) {
                                        LOG.info("Interrupted while attempting to start ReadOnlyZooKeeperServer, not started");
                                    } catch (Exception e) {
                                        LOG.error("FAILED to start ReadOnlyZooKeeperServer", e);
                                    }
                                }
                            };
                            try {
                                // 运行run方法
                                roZkMgr.start();
                                // 重新配置标志清除
                                reconfigFlagClear();
                                // 如果目前仍然没有选出Leader，当前服务节点，参与选举
                                if (shuttingDownLE) {
                                    shuttingDownLE = false;
                                    // 开始领导人选举
                                    startLeaderElection();
                                }
                                // TODO  lookForLeader() 当前节点参与选举
                                setCurrentVote(makeLEStrategy().lookForLeader());
                            } catch (Exception e) {
                                LOG.warn("Unexpected exception", e);
                                setPeerState(ServerState.LOOKING);
                            } finally {
                                // 如果线程处于宽限期，则中断以退出等待状态。
                                roZkMgr.interrupt();
                                roZk.shutdown();
                            }
                        }
                        // 没有开启只读模式
                        else {
                            try {
                                reconfigFlagClear();
                                if (shuttingDownLE) {
                                    shuttingDownLE = false;
                                    // 开始领导人选举
                                    startLeaderElection();
                                }
                                // 设置当前投票
                                setCurrentVote(makeLEStrategy().lookForLeader());
                            } catch (Exception e) {
                                LOG.warn("Unexpected exception", e);
                                setPeerState(ServerState.LOOKING);
                            }
                        }
                        break;
                    case OBSERVING:
                        try {
                            // TODO ObServer 的观察这模式如何实现？
                            LOG.info("OBSERVING");
                            // 设置观察者
                            setObserver(makeObserver(logFactory));
                            // 观察Leader
                            observer.observeLeader();
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                        } finally {
                            observer.shutdown();
                            setObserver(null);
                            updateServerState();
                        }
                        break;
                    case FOLLOWING:
                        try {
                            // TODO Following 如何同步Leader数据，以及如何转发写入请求？ ObServer和Following都是通过与Leader建立连接，轮询监听读取Leader更新数据的。
                            LOG.info("FOLLOWING");
                            setFollower(makeFollower(logFactory));
                            follower.followLeader();
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                        } finally {
                            follower.shutdown();
                            setFollower(null);
                            updateServerState();
                        }
                        break;
                    case LEADING:
                        // TODO Leader 写入的过程是如何进行的？数据同步、可靠性、XA事务？
                        LOG.info("LEADING");
                        try {
                            setLeader(makeLeader(logFactory));
                            // Leader的主要功能
                            leader.lead();
                            setLeader(null);
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                        } finally {
                            if (leader != null) {
                                leader.shutdown("Forcing shutdown");
                                setLeader(null);
                            }
                            updateServerState();
                        }
                        break;
                }
                start_fle = Time.currentElapsedTime();
            }
        } finally {
            LOG.warn("QuorumPeer main thread exited");
            MBeanRegistry instance = MBeanRegistry.getInstance();
            instance.unregister(jmxQuorumBean);
            instance.unregister(jmxLocalPeerBean);

            for (RemotePeerBean remotePeerBean : jmxRemotePeerBean.values()) {
                instance.unregister(remotePeerBean);
            }

            jmxQuorumBean = null;
            jmxLocalPeerBean = null;
            jmxRemotePeerBean = null;
        }
    }


    private synchronized void updateServerState() {
        if (!reconfigFlag) {
            setPeerState(ServerState.LOOKING);
            LOG.warn("PeerState set to LOOKING");
            return;
        }

        if (getId() == getCurrentVote().getId()) {
            setPeerState(ServerState.LEADING);
            LOG.debug("PeerState set to LEADING");
        } else if (getLearnerType() == LearnerType.PARTICIPANT) {
            setPeerState(ServerState.FOLLOWING);
            LOG.debug("PeerState set to FOLLOWING");
        } else if (getLearnerType() == LearnerType.OBSERVER) {
            setPeerState(ServerState.OBSERVING);
            LOG.debug("PeerState set to OBSERVER");
        } else { // currently shouldn't happen since there are only 2 learner types
            setPeerState(ServerState.LOOKING);
            LOG.debug("Shouldn't be here");
        }
        reconfigFlag = false;
    }

    public void shutdown() {
        running = false;
        x509Util.close();
        if (leader != null) {
            leader.shutdown("quorum Peer shutdown");
        }
        if (follower != null) {
            follower.shutdown();
        }
        shutdownServerCnxnFactory();
        if (udpSocket != null) {
            udpSocket.close();
        }

        try {
            adminServer.shutdown();
        } catch (AdminServerException e) {
            LOG.warn("Problem stopping AdminServer", e);
        }

        if (getElectionAlg() != null) {
            this.interrupt();
            getElectionAlg().shutdown();
        }
        try {
            zkDb.close();
        } catch (IOException ie) {
            LOG.warn("Error closing logs ", ie);
        }
    }

    /**
     * A 'view' is a node's current opinion of the membership of the entire
     * ensemble.
     */
    public Map<Long, QuorumPeer.QuorumServer> getView() {
        return Collections.unmodifiableMap(getQuorumVerifier().getAllMembers());
    }

    /**
     * Observers are not contained in this view, only nodes with
     * PeerType=PARTICIPANT.
     */
    public Map<Long, QuorumPeer.QuorumServer> getVotingView() {
        return getQuorumVerifier().getVotingMembers();
    }

    /**
     * Returns only observers, no followers.
     */
    public Map<Long, QuorumPeer.QuorumServer> getObservingView() {
        return getQuorumVerifier().getObservingMembers();
    }

    public synchronized Set<Long> getCurrentAndNextConfigVoters() {
        Set<Long> voterIds = new HashSet<Long>(getQuorumVerifier() // 从验证者中获取所有参与选举的成员
                .getVotingMembers().keySet());
        if (getLastSeenQuorumVerifier() != null) { // 获取最后一次仲裁验证者，默认当前节点为最后一次选举的验证者
            voterIds.addAll(getLastSeenQuorumVerifier().getVotingMembers()
                    .keySet());
        }
        return voterIds;
    }

    /**
     * Check if a node is in the current view. With static membership, the
     * result of this check will never change; only when dynamic membership
     * is introduced will this be more useful.
     */
    public boolean viewContains(Long sid) {
        return this.getView().containsKey(sid);
    }

    /**
     * Only used by QuorumStats at the moment
     */
    public String[] getQuorumPeers() {
        List<String> l = new ArrayList<String>();
        synchronized (this) {
            if (leader != null) {
                for (LearnerHandler fh : leader.getLearners()) {
                    if (fh.getSocket() != null) {
                        String s = fh.getSocket().getRemoteSocketAddress().toString();
                        if (leader.isLearnerSynced(fh))
                            s += "*";
                        l.add(s);
                    }
                }
            } else if (follower != null) {
                l.add(follower.sock.getRemoteSocketAddress().toString());
            }
        }
        return l.toArray(new String[0]);
    }

    public String getServerState() {
        switch (getPeerState()) {
            case LOOKING:
                return QuorumStats.Provider.LOOKING_STATE;
            case LEADING:
                return QuorumStats.Provider.LEADING_STATE;
            case FOLLOWING:
                return QuorumStats.Provider.FOLLOWING_STATE;
            case OBSERVING:
                return QuorumStats.Provider.OBSERVING_STATE;
        }
        return QuorumStats.Provider.UNKNOWN_STATE;
    }


    /**
     * set the id of this quorum peer.
     */
    public void setMyId(long myid) {
        this.myid = myid;
    }

    /**
     * Get the number of milliseconds of each tick
     */
    public int getTickTime() {
        return tickTime;
    }

    /**
     * Set the number of milliseconds of each tick
     */
    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }

    /**
     * Maximum number of connections allowed from particular host (ip)
     */
    public int getMaxClientCnxnsPerHost() {
        if (cnxnFactory != null) {
            return cnxnFactory.getMaxClientCnxnsPerHost();
        }
        if (secureCnxnFactory != null) {
            return secureCnxnFactory.getMaxClientCnxnsPerHost();
        }
        return -1;
    }

    /**
     * Whether local sessions are enabled
     */
    public boolean areLocalSessionsEnabled() {
        return localSessionsEnabled;
    }

    /**
     * Whether to enable local sessions
     */
    public void enableLocalSessions(boolean flag) {
        LOG.info("Local sessions " + (flag ? "enabled" : "disabled"));
        localSessionsEnabled = flag;
    }

    /**
     * Whether local sessions are allowed to upgrade to global sessions
     */
    public boolean isLocalSessionsUpgradingEnabled() {
        return localSessionsUpgradingEnabled;
    }

    /**
     * Whether to allow local sessions to upgrade to global sessions
     */
    public void enableLocalSessionsUpgrading(boolean flag) {
        LOG.info("Local session upgrading " + (flag ? "enabled" : "disabled"));
        localSessionsUpgradingEnabled = flag;
    }

    /**
     * minimum session timeout in milliseconds
     */
    public int getMinSessionTimeout() {
        return minSessionTimeout;
    }

    /**
     * minimum session timeout in milliseconds
     */
    public void setMinSessionTimeout(int min) {
        LOG.info("minSessionTimeout set to " + min);
        this.minSessionTimeout = min;
    }

    /**
     * maximum session timeout in milliseconds
     */
    public int getMaxSessionTimeout() {
        return maxSessionTimeout;
    }

    /**
     * maximum session timeout in milliseconds
     */
    public void setMaxSessionTimeout(int max) {
        LOG.info("maxSessionTimeout set to " + max);
        this.maxSessionTimeout = max;
    }

    /**
     * Get the number of ticks that the initial synchronization phase can take
     */
    public int getInitLimit() {
        return initLimit;
    }

    /**
     * Set the number of ticks that the initial synchronization phase can take
     */
    public void setInitLimit(int initLimit) {
        LOG.info("initLimit set to " + initLimit);
        this.initLimit = initLimit;
    }

    /**
     * Get the current tick
     */
    public int getTick() {
        return tick.get();
    }

    public QuorumVerifier configFromString(String s) throws IOException, ConfigException {
        Properties props = new Properties();
        props.load(new StringReader(s));
        return QuorumPeerConfig.parseDynamicConfig(props, electionType, false, false);
    }

    /**
     * 返回验证者，默认以自己作为验证者
     */
    public QuorumVerifier getQuorumVerifier() {
        synchronized (QV_LOCK) {
            return quorumVerifier;
        }
    }

    /**
     * 返回最后一个建议的配置的QuorumVerifier对象。
     */
    public QuorumVerifier getLastSeenQuorumVerifier() {
        synchronized (QV_LOCK) {
            return lastSeenQuorumVerifier;
        }
    }

    public synchronized void restartLeaderElection(QuorumVerifier qvOLD, QuorumVerifier qvNEW) {
        if (qvOLD == null || !qvOLD.equals(qvNEW)) {
            LOG.warn("Restarting Leader Election");
            getElectionAlg().shutdown();
            shuttingDownLE = false;
            startLeaderElection();
        }
    }

    public String getNextDynamicConfigFilename() {
        if (configFilename == null) {
            LOG.warn("configFilename is null! This should only happen in tests.");
            return null;
        }
        return configFilename + QuorumPeerConfig.nextDynamicConfigFileSuffix;
    }

    // On entry to this method, qcm must be non-null and the locks on both qcm and QV_LOCK
    // must be held.  We don't want quorumVerifier/lastSeenQuorumVerifier to change out from
    // under us, so we have to hold QV_LOCK; and since the call to qcm.connectOne() will take
    // the lock on qcm (and take QV_LOCK again inside that), the caller needs to have taken
    // qcm outside QV_LOCK to avoid a deadlock against other callers of qcm.connectOne().
    private void connectNewPeers(QuorumCnxManager qcm) {
        if (quorumVerifier != null && lastSeenQuorumVerifier != null) {
            Map<Long, QuorumServer> committedView = quorumVerifier.getAllMembers();
            for (Entry<Long, QuorumServer> e : lastSeenQuorumVerifier.getAllMembers().entrySet()) {
                if (e.getKey() != getId() && !committedView.containsKey(e.getKey()))
                    qcm.connectOne(e.getKey());
            }
        }
    }

    public void setLastSeenQuorumVerifier(QuorumVerifier qv, boolean writeToDisk) {
        /*
        如果qcm不为空，我们可以调用qcm.connectOne（），它将锁定qcm
        然后使用QV_LOCK。以相同的顺序获取锁，以确保我们不会对connectOne（）的其他调用者造成死锁。如果当我们在同步块中时在另一个线程中设置了qcmRef，
        则没有任何危害；如果我们不对qcm进行锁定（因为采样时为null），则不会对其调用connectOne（）。
         （使用AtomicReference足以保证在进入此方法之前可能在另一个线程中发生的更新的可见性。）
         */
        QuorumCnxManager qcm = qcmRef.get();
        Object outerLockObject = (qcm != null) ? qcm : QV_LOCK;
        synchronized (outerLockObject) {
            synchronized (QV_LOCK) {
                if (lastSeenQuorumVerifier != null && lastSeenQuorumVerifier.getVersion() > qv.getVersion()) {
                    LOG.error("setLastSeenQuorumVerifier called with stale config " + qv.getVersion() +
                            ". Current version: " + quorumVerifier.getVersion());
                }
                // 假定版本唯一标识一个配置，因此如果版本相同，则此处无需执行任何操作。
                if (lastSeenQuorumVerifier != null &&
                        lastSeenQuorumVerifier.getVersion() == qv.getVersion()) {
                    return;
                }
                lastSeenQuorumVerifier = qv;
                if (qcm != null) {
                    connectNewPeers(qcm);
                }

                if (writeToDisk) {
                    try {
                        String fileName = getNextDynamicConfigFilename();
                        if (fileName != null) {
                            QuorumPeerConfig.writeDynamicConfig(fileName, qv, true);
                        }
                    } catch (IOException e) {
                        LOG.error("Error writing next dynamic config file to disk: ", e.getMessage());
                    }
                }
            }
        }
    }

    public QuorumVerifier setQuorumVerifier(QuorumVerifier qv, boolean writeToDisk) {
        synchronized (QV_LOCK) {
            if ((quorumVerifier != null) && (quorumVerifier.getVersion() >= qv.getVersion())) {
                // 这是正常的。例如-服务器通过FastLeaderElection闲聊发现了有关新配置的信息，然后在UPTODATE消息中获得了相同的配置，因此它已经为人所知
                LOG.debug(getId() + " setQuorumVerifier called with known or old config " + qv.getVersion() +
                        ". Current version: " + quorumVerifier.getVersion());
                return quorumVerifier;
            }
            QuorumVerifier prevQV = quorumVerifier;
            quorumVerifier = qv;
            // TODO 最后的验证节点，默认为当前节点
            if (lastSeenQuorumVerifier == null || (qv.getVersion() > lastSeenQuorumVerifier.getVersion()))
                lastSeenQuorumVerifier = qv;

            if (writeToDisk) {
                // 一些测试在没有静态配置文件的情况下初始化QuorumPeer
                if (configFilename != null) {
                    try {
                        // 制作动态配置文件名
                        String dynamicConfigFilename = makeDynamicConfigFilename(
                                qv.getVersion());
                        // 编写动态配置
                        QuorumPeerConfig.writeDynamicConfig(
                                dynamicConfigFilename, qv, false);
                        // 编写静态配置
                        QuorumPeerConfig.editStaticConfig(configFilename,
                                dynamicConfigFilename,
                                // 需要从静态配置中擦除客户端信息
                                needEraseClientInfoFromStaticConfig());
                    } catch (IOException e) {
                        LOG.error("Error closing file: ", e.getMessage());
                    }
                } else {
                    LOG.info("writeToDisk == true but configFilename == null");
                }
            }

            // 最后看到的法定人数验证者
            if (qv.getVersion() == lastSeenQuorumVerifier.getVersion()) {
                QuorumPeerConfig.deleteFile(getNextDynamicConfigFilename());
            }
            // 获取所有成员
            QuorumServer qs = qv.getAllMembers().get(getId());
            if (qs != null) {
                setAddrs(qs.addr, qs.electionAddr, qs.clientAddr);
            }
            return prevQV;
        }
    }

    private String makeDynamicConfigFilename(long version) {
        return configFilename + ".dynamic." + Long.toHexString(version);
    }

    private boolean needEraseClientInfoFromStaticConfig() {
        QuorumServer server = quorumVerifier.getAllMembers().get(getId());
        return (server != null && server.clientAddr != null);
    }

    /**
     * Get an instance of LeaderElection
     */
    public Election getElectionAlg() {
        return electionAlg;
    }

    /**
     * Get the synclimit
     */
    public int getSyncLimit() {
        return syncLimit;
    }

    /**
     * Set the synclimit
     */
    public void setSyncLimit(int syncLimit) {
        this.syncLimit = syncLimit;
    }


    /**
     * The syncEnabled can also be set via a system property.
     */
    public static final String SYNC_ENABLED = "zookeeper.observer.syncEnabled";

    /**
     * Return syncEnabled.
     *
     * @return
     */
    public boolean getSyncEnabled() {
        if (System.getProperty(SYNC_ENABLED) != null) {
            LOG.info(SYNC_ENABLED + "=" + Boolean.getBoolean(SYNC_ENABLED));
            return Boolean.getBoolean(SYNC_ENABLED);
        } else {
            return syncEnabled;
        }
    }

    /**
     * Set syncEnabled.
     *
     * @param syncEnabled
     */
    public void setSyncEnabled(boolean syncEnabled) {
        this.syncEnabled = syncEnabled;
    }

    /**
     * Gets the election type
     */
    public int getElectionType() {
        return electionType;
    }

    /**
     * Sets the election type
     */
    public void setElectionType(int electionType) {
        this.electionType = electionType;
    }

    public boolean getQuorumListenOnAllIPs() {
        return quorumListenOnAllIPs;
    }

    public void setQuorumListenOnAllIPs(boolean quorumListenOnAllIPs) {
        this.quorumListenOnAllIPs = quorumListenOnAllIPs;
    }

    public void setCnxnFactory(ServerCnxnFactory cnxnFactory) {
        this.cnxnFactory = cnxnFactory;
    }

    public void setSecureCnxnFactory(ServerCnxnFactory secureCnxnFactory) {
        this.secureCnxnFactory = secureCnxnFactory;
    }

    public void setSslQuorum(boolean sslQuorum) {
        if (sslQuorum) {
            LOG.info("Using TLS encrypted quorum communication");
        } else {
            LOG.info("Using insecure (non-TLS) quorum communication");
        }
        this.sslQuorum = sslQuorum;
    }

    public void setUsePortUnification(boolean shouldUsePortUnification) {
        LOG.info("Port unification {}", shouldUsePortUnification ? "enabled" : "disabled");
        this.shouldUsePortUnification = shouldUsePortUnification;
    }

    private void startServerCnxnFactory() {
        if (cnxnFactory != null) {
            cnxnFactory.start();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.start();
        }
    }

    private void shutdownServerCnxnFactory() {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.shutdown();
        }
    }

    // 负责人和学习者将控制zookeeper服务器，并将其传递给QuorumPeer。
    public void setZooKeeperServer(ZooKeeperServer zks) {
        if (cnxnFactory != null) {
            cnxnFactory.setZooKeeperServer(zks);
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.setZooKeeperServer(zks);
        }
    }

    public void closeAllConnections() {
        if (cnxnFactory != null) {
            cnxnFactory.closeAll();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.closeAll();
        }
    }

    public int getClientPort() {
        if (cnxnFactory != null) {
            return cnxnFactory.getLocalPort();
        }
        return -1;
    }

    public void setTxnFactory(FileTxnSnapLog factory) {
        this.logFactory = factory;
    }

    public FileTxnSnapLog getTxnFactory() {
        return this.logFactory;
    }

    /**
     * set zk database for this node
     *
     * @param database
     */
    public void setZKDatabase(ZKDatabase database) {
        this.zkDb = database;
    }

    protected ZKDatabase getZkDb() {
        return zkDb;
    }

    public synchronized void initConfigInZKDatabase() {
        if (zkDb != null) zkDb.initConfigInZKDatabase(getQuorumVerifier());
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * 获取对QuorumCnxManager的引用
     */
    public QuorumCnxManager getQuorumCnxManager() {
        return qcmRef.get();
    }

    private long readLongFromFile(String name) throws IOException {
        File file = new File(logFactory.getSnapDir(), name);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = "";
        try {
            line = br.readLine();
            return Long.parseLong(line);
        } catch (NumberFormatException e) {
            throw new IOException("Found " + line + " in " + file);
        } finally {
            br.close();
        }
    }

    private long acceptedEpoch = -1;
    private long currentEpoch = -1;

    public static final String CURRENT_EPOCH_FILENAME = "currentEpoch";

    public static final String ACCEPTED_EPOCH_FILENAME = "acceptedEpoch";

    /**
     * 自动将长值写入磁盘。
     * 成功或抛出异常。
     *
     * @param name  file name to write the long to
     * @param value the long value to write to the named file
     * @throws IOException if the file cannot be written atomically
     */
    private void writeLongToFile(String name, final long value) throws IOException {
        File file = new File(logFactory.getSnapDir(), name);
        new AtomicFileWritingIdiom(file, new WriterStatement() {
            @Override
            public void write(Writer bw) throws IOException {
                bw.write(Long.toString(value));
            }
        });
    }

    public long getCurrentEpoch() throws IOException {
        if (currentEpoch == -1) {
            currentEpoch = readLongFromFile(CURRENT_EPOCH_FILENAME);
        }
        return currentEpoch;
    }

    public long getAcceptedEpoch() throws IOException {
        if (acceptedEpoch == -1) {
            acceptedEpoch = readLongFromFile(ACCEPTED_EPOCH_FILENAME);
        }
        return acceptedEpoch;
    }

    public void setCurrentEpoch(long e) throws IOException {
        currentEpoch = e;
        writeLongToFile(CURRENT_EPOCH_FILENAME, e);

    }

    public void setAcceptedEpoch(long e) throws IOException {
        acceptedEpoch = e;
        writeLongToFile(ACCEPTED_EPOCH_FILENAME, e);
    }

    /**
     * @param qv
     * @param suggestedLeaderId 建议的领导人 null
     * @param zxid              null
     * @param restartLE
     * @return
     */
    public boolean processReconfig(QuorumVerifier qv, Long suggestedLeaderId, Long zxid, boolean restartLE) {
        if (!QuorumPeerConfig.isReconfigEnabled()) {
            LOG.debug("Reconfig feature is disabled, skip reconfig processing.");
            return false;
        }

        InetSocketAddress oldClientAddr = getClientAddress();

        // 更新最后提交的仲裁验证程序，将新配置写入磁盘，如果配置更改，则重新启动领导者选择。
        QuorumVerifier prevQV = setQuorumVerifier(qv, true);

        // 初始配置没有日志记录，因此与领导者同步 zookeeper config为空！
        // 也有可能在领导者选举期间传播最后提交的配置，而不传播相应的日志记录。
        // 因此，我们应该明确地执行此操作（当我们已经是Follower / Observer时，
        // 对于Learner则不需要这样做）：
        initConfigInZKDatabase();

        // Version的验证是在保证，当前节点属于本轮选举，超出或低于 则不应该参与本轮选举

        // 收到的版本 > 本地记录的前一轮选举版本 && 并且不等于前一轮选举版本
        if (prevQV.getVersion() < qv.getVersion() && !prevQV.equals(qv)) {
            Map<Long, QuorumServer> newMembers = qv.getAllMembers();
            // 更新远程对等MX Beans
            updateRemotePeerMXBeans(newMembers);

            // TODO 重新开始领导人选举，此处传入为false，因此不重新开始选举
            if (restartLE) restartLeaderElection(prevQV, qv);

            QuorumServer myNewQS = newMembers.get(getId());
            if (myNewQS != null && myNewQS.clientAddr != null
                    && !myNewQS.clientAddr.equals(oldClientAddr)) {
                // 重新配置
                cnxnFactory.reconfigure(myNewQS.clientAddr);
                updateThreadName();
            }

            // 更新学习者类型
            boolean roleChange = updateLearnerType(qv);
            boolean leaderChange = false;
            // TODO 第一次启动时，推荐 suggestedLeaderId(建议的领导者编号)为null
            if (suggestedLeaderId != null) {
                // 如果指定了 推荐的领导人，则更新覆盖
                leaderChange = updateVote(suggestedLeaderId, zxid);
            } else {
                // 获得当前投票
                long currentLeaderId = getCurrentVote().getId();
                // getVotingMembers() 获得投票成员
                QuorumServer myleaderInCurQV = prevQV.getVotingMembers().get(currentLeaderId);
                QuorumServer myleaderInNewQV = qv.getVotingMembers().get(currentLeaderId);
                leaderChange = (myleaderInCurQV == null || myleaderInCurQV.addr == null ||
                        myleaderInNewQV == null || !myleaderInCurQV.addr.equals(myleaderInNewQV.addr));
                // 我们没有指定的领导者-需要参加领导者选举
                reconfigFlagClear();
            }

            // 角色/Leader 变更
            if (roleChange || leaderChange) {
                return true;
            }
        }
        return false;

    }

    private void updateRemotePeerMXBeans(Map<Long, QuorumServer> newMembers) {
        // 现有成员
        Set<Long> existingMembers = new HashSet<Long>(newMembers.keySet());
        existingMembers.retainAll(jmxRemotePeerBean.keySet());
        for (Long id : existingMembers) {
            RemotePeerBean rBean = jmxRemotePeerBean.get(id);
            rBean.setQuorumServer(newMembers.get(id));
        }

        // 加入成员
        Set<Long> joiningMembers = new HashSet<Long>(newMembers.keySet());
        joiningMembers.removeAll(jmxRemotePeerBean.keySet());
        // 删除自身，因为它是本地bean
        joiningMembers.remove(getId());
        for (Long id : joiningMembers) {
            QuorumServer qs = newMembers.get(id);
            RemotePeerBean rBean = new RemotePeerBean(this, qs);
            try {
                // JMX 注册
                MBeanRegistry.getInstance().register(rBean, jmxQuorumBean);
                jmxRemotePeerBean.put(qs.id, rBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX", e);
            }
        }

        // 离开成员
        Set<Long> leavingMembers = new HashSet<Long>(jmxRemotePeerBean.keySet());
        leavingMembers.removeAll(newMembers.keySet());
        for (Long id : leavingMembers) {
            RemotePeerBean rBean = jmxRemotePeerBean.remove(id);
            try {
                // 取消注册
                MBeanRegistry.getInstance().unregister(rBean);
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
        }
    }

    private boolean updateLearnerType(QuorumVerifier newQV) {
        // 检查我是否是新配置中的观察者
        if (newQV.getObservingMembers().containsKey(getId())) {
            if (getLearnerType() != LearnerType.OBSERVER) {
                setLearnerType(LearnerType.OBSERVER);
                LOG.info("Becoming an observer");
                reconfigFlagSet();
                return true;
            } else {
                return false;
            }
        } else if (newQV.getVotingMembers().containsKey(getId())) {
            if (getLearnerType() != LearnerType.PARTICIPANT) {
                setLearnerType(LearnerType.PARTICIPANT);
                LOG.info("Becoming a voting participant");
                reconfigFlagSet();
                return true;
            } else {
                return false;
            }
        }
        // 参与者，我不在视野中
        if (getLearnerType() != LearnerType.PARTICIPANT) {
            setLearnerType(LearnerType.PARTICIPANT);
            LOG.info("Becoming a non-voting participant");
            reconfigFlagSet();
            return true;
        }
        return false;
    }

    private boolean updateVote(long designatedLeader, long zxid) {
        // 获取我认为的领导者
        Vote currentVote = getCurrentVote();
        if (currentVote != null && designatedLeader != currentVote.getId()) {
            setCurrentVote(new Vote(designatedLeader, zxid));
            reconfigFlagSet();
            LOG.warn("Suggested leader: " + designatedLeader);
            return true;
        }
        return false;
    }

    /**
     * 更新领导者选举信息，以避免在新服务器尝试加入集合时出现不一致的情况。
     * <p>
     * 我们尝试通过在以下方案解决在领导者之后更新对等纪元的不一致情况：
     * <p>
     * 假设我们有一个包含3个服务器z1，z2和z3的集合。
     * <p>
     * 1. z1，z2在z3之后纪元，peerEpoch为0xb8，新纪元为 0xb9，即磁盘上当前接受的纪元。
     * <p>
     * 2. z2重新启动，从磁盘加载当前接受时期时，它将使用0xb9作为对等时期。
     * <p>
     * 3. z2接收到来自z1和z3的通知，该通知在z3之后带有时期0xb8，因此它再次从对等时代0xb8开始跟随z3。
     * <p>
     * 4. 在z2成功连接到z3之前，z3将以新的时期0xb9重新启动。
     * <p>
     * 5. z2将在放弃之前重试几轮（默认为5s），同时将z3报告为领导者。
     * <p>
     * 6. z1重新启动，并查看对等纪元0xb9。
     * 7. Z1投票Z3，与Z3都属于0xb9时代，再次当选为领头羊。
     * 8. 在放弃之前，z2成功连接到z3，但是对等纪元0xb8。
     * <p>
     * 9. 重新启动z1，寻找具有对等纪元0xba的领导者，但不能加入，因为z2正在报告对等纪元0xb8，而z3正在报告 0xb9。
     * <p>
     * 通过在真正跟随领导人之后更新选举投票，我们可以避免这种卡住的情况发生。
     * <p>
     * 顺便说一句，由于相同的原因，zxid和选举Epoch可能不一致，与领导者同步后最好也更新它们，但是要求协议更改并非易事。
     * 通过在寻找领导者期间为选举服务器中的选票计数时跳过 zxid和elementEpoch来解决此问题。
     * <p>
     * {@see https://issues.apache.org/jira/browse/ZOOKEEPER-1732}
     */
    protected void updateElectionVote(long newEpoch) {
        Vote currentVote = getCurrentVote();
        if (currentVote != null) {
            setCurrentVote(new Vote(currentVote.getId(),
                    currentVote.getZxid(),
                    currentVote.getElectionEpoch(),
                    newEpoch,
                    currentVote.getState()));
        }
    }

    private void updateThreadName() {
        String plain = cnxnFactory != null ?
                cnxnFactory.getLocalAddress() != null ?
                        cnxnFactory.getLocalAddress().toString() : "disabled" : "disabled";
        String secure = secureCnxnFactory != null ? secureCnxnFactory.getLocalAddress().toString() : "disabled";
        setName(String.format("QuorumPeer[1=%d](plain=%s)(secure=%s)", getId(), plain, secure));
    }

    /**
     * Sets the time taken for leader election in milliseconds.
     *
     * @param electionTimeTaken time taken for leader election
     */
    void setElectionTimeTaken(long electionTimeTaken) {
        this.electionTimeTaken = electionTimeTaken;
    }

    /**
     * @return the time taken for leader election in milliseconds.
     */
    long getElectionTimeTaken() {
        return electionTimeTaken;
    }

    void setQuorumServerSaslRequired(boolean serverSaslRequired) {
        quorumServerSaslAuthRequired = serverSaslRequired;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_SERVER_SASL_AUTH_REQUIRED,
                serverSaslRequired);
    }

    void setQuorumLearnerSaslRequired(boolean learnerSaslRequired) {
        quorumLearnerSaslAuthRequired = learnerSaslRequired;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_LEARNER_SASL_AUTH_REQUIRED,
                learnerSaslRequired);
    }

    void setQuorumSaslEnabled(boolean enableAuth) {
        quorumSaslEnableAuth = enableAuth;
        if (!quorumSaslEnableAuth) {
            LOG.info("QuorumPeer communication is not secured! (SASL auth disabled)");
        } else {
            LOG.info("{} set to {}",
                    QuorumAuth.QUORUM_SASL_AUTH_ENABLED, enableAuth);
        }
    }

    void setQuorumServicePrincipal(String servicePrincipal) {
        quorumServicePrincipal = servicePrincipal;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_KERBEROS_SERVICE_PRINCIPAL,
                quorumServicePrincipal);
    }

    void setQuorumLearnerLoginContext(String learnerContext) {
        quorumLearnerLoginContext = learnerContext;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_LEARNER_SASL_LOGIN_CONTEXT,
                quorumLearnerLoginContext);
    }

    void setQuorumServerLoginContext(String serverContext) {
        quorumServerLoginContext = serverContext;
        LOG.info("{} set to {}", QuorumAuth.QUORUM_SERVER_SASL_LOGIN_CONTEXT,
                quorumServerLoginContext);
    }

    void setQuorumCnxnThreadsSize(int qCnxnThreadsSize) {
        if (qCnxnThreadsSize > QUORUM_CNXN_THREADS_SIZE_DEFAULT_VALUE) {
            quorumCnxnThreadsSize = qCnxnThreadsSize;
        }
        LOG.info("quorum.cnxn.threads.size set to {}", quorumCnxnThreadsSize);
    }

    boolean isQuorumSaslAuthEnabled() {
        return quorumSaslEnableAuth;
    }

    private boolean isQuorumServerSaslAuthRequired() {
        return quorumServerSaslAuthRequired;
    }

    private boolean isQuorumLearnerSaslAuthRequired() {
        return quorumLearnerSaslAuthRequired;
    }

    public QuorumCnxManager createCnxnManager() {
        return new QuorumCnxManager(this,
                this.getId(),
                this.getView(),
                this.authServer,
                this.authLearner,
                this.tickTime * this.syncLimit,
                this.getQuorumListenOnAllIPs(),
                this.quorumCnxnThreadsSize,
                this.isQuorumSaslAuthEnabled());
    }

    boolean isLeader(long id) {
        Vote vote = getCurrentVote();
        return vote != null && id == vote.getId();
    }
}
