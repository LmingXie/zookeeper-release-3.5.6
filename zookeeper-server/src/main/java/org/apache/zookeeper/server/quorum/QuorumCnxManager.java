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

import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocket;
import java.io.*;
import java.net.*;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.zookeeper.common.NetUtils.formatInetAddr;

/**
 * This class implements a connection manager for leader election using TCP. It
 * maintains one connection for every pair of servers. The tricky part is to
 * guarantee that there is exactly one connection for every pair of servers that
 * are operating correctly and that can communicate over the network.
 * <p>
 * If two servers try to start a connection concurrently, then the connection
 * manager uses a very simple tie-breaking mechanism to decide which connection
 * to drop based on the IP addressed of the two parties.
 * <p>
 * For every peer, the manager maintains a queue of messages to send. If the
 * connection to any particular peer drops, then the sender thread puts the
 * message back on the list. As this implementation currently uses a queue
 * implementation to maintain messages to send to another peer, we add the
 * message to the tail of the queue, thus changing the order of messages.
 * Although this is not a problem for the leader election, it could be a problem
 * when consolidating peer communication. This is to be verified, though.
 */

public class QuorumCnxManager {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumCnxManager.class);

    /*
     * Maximum capacity of thread queues
     */
    static final int RECV_CAPACITY = 100;
    // Initialized to 1 to prevent sending
    // stale notifications to peers
    static final int SEND_CAPACITY = 1;

    static final int PACKETMAXSIZE = 1024 * 512;

    /*
     * Negative counter for observer server ids.
     */

    private AtomicLong observerCounter = new AtomicLong(-1);

    /*
     * Protocol identifier used among peers
     */
    public static final long PROTOCOL_VERSION = -65536L;

    /*
     * Max buffer size to be read from the network.
     */
    static public final int maxBuffer = 2048;

    /*
     * Connection time out value in milliseconds
     */

    private int cnxTO = 5000;

    final QuorumPeer self;

    /*
     * Local IP address
     */
    final long mySid;
    final int socketTimeout;
    final Map<Long, QuorumPeer.QuorumServer> view;
    final boolean listenOnAllIPs;
    private ThreadPoolExecutor connectionExecutor;
    private final Set<Long> inprogressConnections = Collections
            .synchronizedSet(new HashSet<Long>());
    private QuorumAuthServer authServer;
    private QuorumAuthLearner authLearner;
    private boolean quorumSaslAuthEnabled;
    /*
     * Counter to count connection processing threads.
     */
    private AtomicInteger connectionThreadCnt = new AtomicInteger(0);

    /*
     * Mapping from Peer to Thread number
     */
    final ConcurrentHashMap<Long, SendWorker> senderWorkerMap;
    final ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>> queueSendMap;
    final ConcurrentHashMap<Long, ByteBuffer> lastMessageSent;

    /*
     * Reception queue
     */
    public final ArrayBlockingQueue<Message> recvQueue;
    /*
     * Object to synchronize access to recvQueue
     */
    private final Object recvQLock = new Object();

    /*
     * Shutdown flag
     */

    volatile boolean shutdown = false;

    /*
     * Listener thread
     */
    public final Listener listener;

    /*
     * Counter to count worker threads
     */
    private AtomicInteger threadCnt = new AtomicInteger(0);

    /*
     * CP保持连接的套接字选项
     */
    private final boolean tcpKeepAlive = Boolean.getBoolean("zookeeper.tcpKeepAlive");

    static public class Message {
        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;
        long sid;
    }

    /*
     * This class parses the initial identification sent out by peers with their
     * sid & hostname.
     */
    static public class InitialMessage {
        public Long sid;
        public InetSocketAddress electionAddr;

        InitialMessage(Long sid, InetSocketAddress address) {
            this.sid = sid;
            this.electionAddr = address;
        }

        @SuppressWarnings("serial")
        public static class InitialMessageException extends Exception {
            InitialMessageException(String message, Object... args) {
                super(String.format(message, args));
            }
        }

        static public InitialMessage parse(Long protocolVersion, DataInputStream din)
                throws InitialMessageException, IOException {
            Long sid;

            if (protocolVersion != PROTOCOL_VERSION) {
                throw new InitialMessageException(
                        "Got unrecognized protocol version %s", protocolVersion);
            }

            sid = din.readLong();

            int remaining = din.readInt();
            if (remaining <= 0 || remaining > maxBuffer) {
                throw new InitialMessageException(
                        "Unreasonable buffer length: %s", remaining);
            }

            byte[] b = new byte[remaining];
            int num_read = din.read(b);

            if (num_read != remaining) {
                throw new InitialMessageException(
                        "Read only %s bytes out of %s sent by server %s",
                        num_read, remaining, sid);
            }

            // FIXME: IPv6 is not supported. Using something like Guava's HostAndPort
            //        parser would be good.
            String addr = new String(b);
            String[] host_port = addr.split(":");

            if (host_port.length != 2) {
                throw new InitialMessageException("Badly formed address: %s", addr);
            }

            int port;
            try {
                port = Integer.parseInt(host_port[1]);
            } catch (NumberFormatException e) {
                throw new InitialMessageException("Bad port number: %s", host_port[1]);
            }

            return new InitialMessage(sid, new InetSocketAddress(host_port[0], port));
        }
    }

    public QuorumCnxManager(QuorumPeer self,
                            final long mySid,
                            Map<Long, QuorumPeer.QuorumServer> view,
                            QuorumAuthServer authServer,
                            QuorumAuthLearner authLearner,
                            int socketTimeout,
                            boolean listenOnAllIPs,
                            int quorumCnxnThreadsSize,
                            boolean quorumSaslAuthEnabled) {
        this.recvQueue = new ArrayBlockingQueue<Message>(RECV_CAPACITY);
        this.queueSendMap = new ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>>();
        this.senderWorkerMap = new ConcurrentHashMap<Long, SendWorker>();
        this.lastMessageSent = new ConcurrentHashMap<Long, ByteBuffer>();

        String cnxToValue = System.getProperty("zookeeper.cnxTimeout");
        if (cnxToValue != null) {
            this.cnxTO = Integer.parseInt(cnxToValue);
        }

        this.self = self;

        this.mySid = mySid;
        this.socketTimeout = socketTimeout;
        this.view = view;
        this.listenOnAllIPs = listenOnAllIPs;

        initializeAuth(mySid, authServer, authLearner, quorumCnxnThreadsSize,
                quorumSaslAuthEnabled);

        // Starts listener thread that waits for connection requests
        listener = new Listener();
        listener.setName("QuorumPeerListener");
    }

    private void initializeAuth(final long mySid,
                                final QuorumAuthServer authServer,
                                final QuorumAuthLearner authLearner,
                                final int quorumCnxnThreadsSize,
                                final boolean quorumSaslAuthEnabled) {
        this.authServer = authServer;
        this.authLearner = authLearner;
        this.quorumSaslAuthEnabled = quorumSaslAuthEnabled;
        if (!this.quorumSaslAuthEnabled) {
            LOG.debug("Not initializing connection executor as quorum sasl auth is disabled");
            return;
        }

        // init connection executors
        final AtomicInteger threadIndex = new AtomicInteger(1);
        SecurityManager s = System.getSecurityManager();
        final ThreadGroup group = (s != null) ? s.getThreadGroup()
                : Thread.currentThread().getThreadGroup();
        ThreadFactory daemonThFactory = new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(group, r, "QuorumConnectionThread-"
                        + "[1=" + mySid + "]-"
                        + threadIndex.getAndIncrement());
                return t;
            }
        };
        this.connectionExecutor = new ThreadPoolExecutor(3,
                quorumCnxnThreadsSize, 60, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), daemonThFactory);
        this.connectionExecutor.allowCoreThreadTimeOut(true);
    }


    /**
     * Invokes initiateConnection for testing purposes
     *
     * @param sid
     */
    public void testInitiateConnection(long sid) throws Exception {
        LOG.debug("Opening channel to server " + sid);
        Socket sock = new Socket();
        setSockOpts(sock);
        sock.connect(self.getVotingView().get(sid).electionAddr, cnxTO);
        initiateConnection(sock, sid);
    }

    /**
     * If this server has initiated the connection, then it gives up on the
     * connection if it loses challenge. Otherwise, it keeps the connection.
     */
    public void initiateConnection(final Socket sock, final Long sid) {
        try {
            startConnection(sock, sid);
        } catch (IOException e) {
            LOG.error("Exception while connecting, id: {}, addr: {}, closing learner connection",
                    new Object[]{sid, sock.getRemoteSocketAddress()}, e);
            closeSocket(sock);
            return;
        }
    }

    /**
     * Server will initiate the connection request to its peer server
     * asynchronously via separate connection thread.
     */
    public void initiateConnectionAsync(final Socket sock, final Long sid) {
        if (!inprogressConnections.add(sid)) {
            // simply return as there is a connection request to
            // server 'sid' already in progress.
            LOG.debug("Connection request to server id: {} is already in progress, so skipping this request",
                    sid);
            closeSocket(sock);
            return;
        }
        try {
            connectionExecutor.execute(
                    new QuorumConnectionReqThread(sock, sid));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            // Imp: Safer side catching all type of exceptions and remove 'sid'
            // from inprogress connections. This is to avoid blocking further
            // connection requests from this 'sid' in case of errors.
            inprogressConnections.remove(sid);
            LOG.error("Exception while submitting quorum connection request", e);
            closeSocket(sock);
        }
    }

    /**
     * Thread to send connection request to peer server.
     */
    private class QuorumConnectionReqThread extends ZooKeeperThread {
        final Socket sock;
        final Long sid;

        QuorumConnectionReqThread(final Socket sock, final Long sid) {
            super("QuorumConnectionReqThread-" + sid);
            this.sock = sock;
            this.sid = sid;
        }

        @Override
        public void run() {
            try {
                initiateConnection(sock, sid);
            } finally {
                inprogressConnections.remove(sid);
            }
        }
    }

    private boolean startConnection(Socket sock, Long sid)
            throws IOException {
        DataOutputStream dout = null;
        DataInputStream din = null;
        try {
            // Use BufferedOutputStream to reduce the number of IP packets. This is
            // important for x-DC scenarios.
            BufferedOutputStream buf = new BufferedOutputStream(sock.getOutputStream());
            dout = new DataOutputStream(buf);

            // Sending id and challenge
            // represents protocol version (in other words - message type)
            dout.writeLong(PROTOCOL_VERSION);
            dout.writeLong(self.getId());
            final InetSocketAddress electionAddr = self.getElectionAddress();
            String addr = electionAddr.getHostString() + ":" + electionAddr.getPort();
            byte[] addr_bytes = addr.getBytes();
            dout.writeInt(addr_bytes.length);
            dout.write(addr_bytes);
            dout.flush();

            din = new DataInputStream(
                    new BufferedInputStream(sock.getInputStream()));
        } catch (IOException e) {
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
            return false;
        }

        // authenticate learner
        QuorumPeer.QuorumServer qps = self.getVotingView().get(sid);
        if (qps != null) {
            // TODO - investigate why reconfig makes qps null.
            authLearner.authenticate(sock, qps.hostname);
        }

        // If lost the challenge, then drop the new connection
        if (sid > self.getId()) {
            LOG.info("Have smaller server identifier, so dropping the " +
                    "connection: (" + sid + ", " + self.getId() + ")");
            closeSocket(sock);
            // Otherwise proceed with the connection
        } else {
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            if (vsw != null)
                vsw.finish();

            senderWorkerMap.put(sid, sw);
            queueSendMap.putIfAbsent(sid, new ArrayBlockingQueue<ByteBuffer>(
                    SEND_CAPACITY));

            sw.start();
            rw.start();

            return true;

        }
        return false;
    }

    /**
     * If this server receives a connection request, then it gives up on the new
     * connection if it wins. Notice that it checks whether it has a connection
     * to this server already or not. If it does, then it sends the smallest
     * possible long value to lose the challenge.
     * <p>
     * 如果该服务器接收到的连接请求，然后放弃新的连接，如果它赢了。
     * 注意，它检查是否有到该服务器的连接已经与否。 如果确实如此，那么它发送尽可能小的长期价值就失去了挑战。
     */
    public void receiveConnection(final Socket sock) {
        DataInputStream din = null;
        try {
            // 读取来自客户端的数据流
            din = new DataInputStream(
                    new BufferedInputStream(sock.getInputStream()));

            // 处理连接
            handleConnection(sock, din);
        } catch (IOException e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                    sock.getRemoteSocketAddress());
            closeSocket(sock);
        }
    }

    /**
     * 服务器接收连接请求，并经由单独的线程异步处理它。
     */
    public void receiveConnectionAsync(final Socket sock) {
        try {
            connectionExecutor.execute(
                    new QuorumConnectionReceiverThread(sock));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection",
                    sock.getRemoteSocketAddress());
            closeSocket(sock);
        }
    }

    /**
     * 接收来自对等服务器的连接请求的线程。
     */
    private class QuorumConnectionReceiverThread extends ZooKeeperThread {
        private final Socket sock;

        QuorumConnectionReceiverThread(final Socket sock) {
            super("QuorumConnectionReceiverThread-" + sock.getRemoteSocketAddress());
            this.sock = sock;
        }

        @Override
        public void run() {
            receiveConnection(sock);
        }
    }

    private void handleConnection(Socket sock, DataInputStream din)
            throws IOException {
        Long sid = null, protocolVersion = null;
        InetSocketAddress electionAddr = null;

        try {
            // 协议版本
            protocolVersion = din.readLong();
            // 这是服务器ID，而不是协议版本
            if (protocolVersion >= 0) {
                sid = protocolVersion;
            } else {
                try {
                    // 解析选举地址和端口号
                    InitialMessage init = InitialMessage.parse(protocolVersion, din);
                    sid = init.sid;
                    electionAddr = init.electionAddr;
                } catch (InitialMessage.InitialMessageException ex) {
                    LOG.error(ex.toString());
                    // 异常时关闭Socket
                    closeSocket(sock);
                    return;
                }
            }

            // 观察者节点的sid是固定的
            if (sid == QuorumPeer.OBSERVER_ID) {
                /*
                 * 随机选择标识符。我们需要一个值来标识连接。
                 */
                sid = observerCounter.getAndDecrement();
                LOG.info("Setting arbitrary identifier to observer: " + sid);
            }
        } catch (IOException e) {
            LOG.warn("Exception reading or writing challenge: {}", e);
            closeSocket(sock);
            return;
        }

        // SASL认证，失败时抛出 SaslException
        authServer.authenticate(sock, din);

        // 如果赢得挑战，则关闭新连接。
        if (sid < self.getId()) {
            /*
             * 该副本可能仍然认为与sid的连接已建立，因此我们必须先关闭工作进程，然后再尝试打开新的连接。
             */
            SendWorker sw = senderWorkerMap.get(sid);
            if (sw != null) {
                sw.finish();
            }

            /*
             * 现在我们开始一个新的连接
             */
            LOG.debug("Create new connection to server: {}", sid);
            closeSocket(sock);

            // 选举地址不存在
            if (electionAddr != null) {
                // 尝试建立与使用其electionAddr ID SID 到服务器的连接。
                connectOne(sid, electionAddr);
            } else {
                connectOne(sid);
            }

        }
        // 否则，启动工作线程以接收数据。
        else {
            /*          SendWorker、RecvWorker 成对出现，SendWorker负责向Socket代表的对等节点发送消息，RecvWorker负责接收来自Socket的消息            */

            // 该线程的一个实例通过一个队列接收要发送的消息，并将它们发送到服务器的sid。
            SendWorker sw = new SendWorker(sock, sid);
            // 线程接收消息。 套接字实例等待读取。 如果信道符，然后从接收器池中删除自身。
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            // 相互引用
            sw.setRecv(rw);

            // 第一次获取不到，第二次循环时
            SendWorker vsw = senderWorkerMap.get(sid);
            if (vsw != null) {
                // 使SendWorker 进入“中断状态”，无法再被停止
                vsw.finish();
            }

            // 放入发件人Map
            senderWorkerMap.put(sid, sw);

            // TODO ?
            queueSendMap.putIfAbsent(sid, new ArrayBlockingQueue<ByteBuffer>(SEND_CAPACITY));

            sw.start();
            rw.start();
        }
    }

    /**
     * 进程调用此消息以使要发送的消息排队。当前，只有领导人选举使用它。
     */
    public void toSend(Long sid, ByteBuffer b) {
        /*
         * 如果要向自己发送消息，则只需使其入队（环回）。
         */
        if (this.mySid == sid) {
            b.position(0);
            addToRecvQueue(new Message(b.duplicate(), sid));
            /*
             * 否则发送到相应的线程发送。
             */
        } else {
            /*
             * 如果尚未建立新连接，请开始。
             */
            ArrayBlockingQueue<ByteBuffer> bq = new ArrayBlockingQueue<ByteBuffer>(SEND_CAPACITY);
            ArrayBlockingQueue<ByteBuffer> oldq = queueSendMap.putIfAbsent(sid, bq);
            if (oldq != null) {
                addToSendQueue(oldq, b);
            } else {
                addToSendQueue(bq, b);
            }
            connectOne(sid);

        }
    }

    /**
     * Try to establish a connection to server with id sid using its electionAddr.
     *
     * @param sid server id
     * @return boolean success indication
     */
    synchronized private boolean connectOne(long sid, InetSocketAddress electionAddr) {
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server " + sid);
            return true;
        }

        Socket sock = null;
        try {
            LOG.debug("Opening channel to server " + sid);
            if (self.isSslQuorum()) {
                SSLSocket sslSock = self.getX509Util().createSSLSocket();
                setSockOpts(sslSock);
                sslSock.connect(electionAddr, cnxTO);
                sslSock.startHandshake();
                sock = sslSock;
                LOG.info("SSL handshake complete with {} - {} - {}", sslSock.getRemoteSocketAddress(), sslSock.getSession().getProtocol(), sslSock.getSession().getCipherSuite());
            } else {
                sock = new Socket();
                setSockOpts(sock);
                sock.connect(electionAddr, cnxTO);

            }
            LOG.debug("Connected to server " + sid);
            // Sends connection request asynchronously if the quorum
            // sasl authentication is enabled. This is required because
            // sasl server authentication process may take few seconds to
            // finish, this may delay next peer connection requests.
            if (quorumSaslAuthEnabled) {
                initiateConnectionAsync(sock, sid);
            } else {
                initiateConnection(sock, sid);
            }
            return true;
        } catch (UnresolvedAddressException e) {
            // Sun doesn't include the address that causes this
            // exception to be thrown, also UAE cannot be wrapped cleanly
            // so we log the exception in order to capture this critical
            // detail.
            LOG.warn("Cannot open channel to " + sid
                    + " at election address " + electionAddr, e);
            closeSocket(sock);
            throw e;
        } catch (X509Exception e) {
            LOG.warn("Cannot open secure channel to " + sid
                    + " at election address " + electionAddr, e);
            closeSocket(sock);
            return false;
        } catch (IOException e) {
            LOG.warn("Cannot open channel to " + sid
                            + " at election address " + electionAddr,
                    e);
            closeSocket(sock);
            return false;
        }
    }

    /**
     * 尝试建立与ID SID到服务器的连接。
     *
     * @param sid server id
     */
    synchronized void connectOne(long sid) {
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server " + sid);
            return;
        }
        synchronized (self.QV_LOCK) {
            boolean knownId = false;
            // 如果基础IP地址已更改，请在尝试连接之前为远程服务器解析主机名。
            self.recreateSocketAddresses(sid);
            Map<Long, QuorumPeer.QuorumServer> lastCommittedView = self.getView();
            QuorumVerifier lastSeenQV = self.getLastSeenQuorumVerifier();
            Map<Long, QuorumPeer.QuorumServer> lastProposedView = lastSeenQV.getAllMembers();
            if (lastCommittedView.containsKey(sid)) {
                knownId = true;
                if (connectOne(sid, lastCommittedView.get(sid).electionAddr))
                    return;
            }
            if (lastSeenQV != null && lastProposedView.containsKey(sid)
                    && (!knownId || (lastProposedView.get(sid).electionAddr !=
                    lastCommittedView.get(sid).electionAddr))) {
                knownId = true;
                if (connectOne(sid, lastProposedView.get(sid).electionAddr))
                    return;
            }
            if (!knownId) {
                LOG.warn("Invalid server id: " + sid);
                return;
            }
        }
    }


    /**
     * 如果不存在，请尝试与每台服务器建立连接。
     */
    public void connectAll() {
        long sid;
        for (Enumeration<Long> en = queueSendMap.keys();
             en.hasMoreElements(); ) {
            sid = en.nextElement();
            connectOne(sid);
        }
    }


    /**
     * 检查所有队列是否为空，表示所有邮件均已传递。
     */
    boolean haveDelivered() {
        for (ArrayBlockingQueue<ByteBuffer> queue : queueSendMap.values()) {
            LOG.debug("Queue size: " + queue.size());
            if (queue.size() == 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * 标记是该结束所有活动并中断侦听器的时候了。
     */
    public void halt() {
        shutdown = true;
        LOG.debug("Halting listener");
        listener.halt();

        // 等待侦听器终止。
        try {
            listener.join();
        } catch (InterruptedException ex) {
            LOG.warn("Got interrupted before joining the listener", ex);
        }
        softHalt();

        // 清除用于身份验证的数据结构
        if (connectionExecutor != null) {
            connectionExecutor.shutdown();
        }
        inprogressConnections.clear();
        resetConnectionThreadCount();
    }

    /**
     * 轻柔的停顿只会使工人难堪。
     */
    public void softHalt() {
        for (SendWorker sw : senderWorkerMap.values()) {
            LOG.debug("Halting sender: " + sw);
            sw.finish();
        }
    }

    /**
     * 辅助方法来设置套接字选项。
     *
     * @param sock Reference to socket
     */
    private void setSockOpts(Socket sock) throws SocketException {
        /*
            默认情况下，发送数数据采用Negale算法。Negale算法是指发送方发送 数据不会立刻发出，而是先放在缓冲区内，等缓冲区满了再发出。
        发送完一批数据后，会等待接收方对这批数据的回应，然后在发送下一批数据。

        Negale算法适用于发送方需要发送大批量数据，并且接受方需要及时作出反应的场合，这种算法通过减少数据的通信的次数来提高通信效率。

        如果发送方持续的发送小批量数据，并且接受方不一定会立即发送响应数据，那么Negale算法会使发送方运行很慢。

        对于GUI程序，如网络游戏程序(服务器需要实时跟踪客户端鼠标的移动)，这个问题尤为突出。
        客户端鼠标位置的改动的信息需要实时发送到服务器上，由于Negale算法采用缓冲，大大减低了实时反应速度，导致客户端运行很慢。

        TCP_NODELY的默认值为false，表示采用Negale算法。如果调用setTcpNoDelay(true)方法，就会关闭Socket的缓冲，确保数据及时发送。

        如果Socket的底层实现不支持TCP_NODELY选项，那么socket.getTcpNoDelay()和setTcpNoDelay()方法就会抛出SocketException异常。
         */
        sock.setTcpNoDelay(true);
        // 设置心跳包内容
        sock.setKeepAlive(tcpKeepAlive);
        // 设置超时时间
        sock.setSoTimeout(self.tickTime * self.syncLimit);
    }

    /**
     * Helper method to close a socket.
     *
     * @param sock Reference to socket
     */
    private void closeSocket(Socket sock) {
        if (sock == null) {
            return;
        }

        try {
            sock.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

    /**
     * Return number of worker threads
     */
    public long getThreadCount() {
        return threadCnt.get();
    }

    /**
     * Return number of connection processing threads.
     */
    public long getConnectionThreadCount() {
        return connectionThreadCnt.get();
    }

    /**
     * 将连接处理线程计数的值重置为零。
     */
    private void resetConnectionThreadCount() {
        connectionThreadCnt.set(0);
    }

    /**
     * 在某些端口上监听的线程
     */
    public class Listener extends ZooKeeperThread {

        private static final String ELECTION_PORT_BIND_RETRY = "zookeeper.electionPortBindRetry";
        private static final int DEFAULT_PORT_BIND_MAX_RETRY = 3;

        private final int portBindMaxRetry;
        private Runnable socketBindErrorHandler = () -> System.exit(ExitCode.UNABLE_TO_BIND_QUORUM_PORT.getValue());
        volatile ServerSocket ss = null;

        public Listener() {
            // 在线程启动期间，线程名称将被覆盖到特定的选举地址
            super("ListenerThread");

            // 尝试绑定到选举端口时的最大重试次数，请参阅ZOOKEEPER-3320了解更多详细信息
            final Integer maxRetry = Integer.getInteger(ELECTION_PORT_BIND_RETRY,
                    DEFAULT_PORT_BIND_MAX_RETRY);
            if (maxRetry >= 0) {
                LOG.info("Election port bind maximum retries is {}",
                        maxRetry == 0 ? "infinite" : maxRetry);
                portBindMaxRetry = maxRetry;
            } else {
                LOG.info("'{}' contains invalid value: {}(must be >= 0). "
                                + "Use default value of {} instead.",
                        ELECTION_PORT_BIND_RETRY, maxRetry, DEFAULT_PORT_BIND_MAX_RETRY);
                portBindMaxRetry = DEFAULT_PORT_BIND_MAX_RETRY;
            }
        }

        /**
         * 更改套接字绑定错误处理程序。用于测试。
         */
        public void setSocketBindErrorHandler(Runnable errorHandler) {
            this.socketBindErrorHandler = errorHandler;
        }

        /**
         * 在accept()上休眠
         */
        @Override
        public void run() {
            // 重试计数
            int numRetries = 0;
            // 外网Socket连接地址
            InetSocketAddress addr;
            Socket client = null;
            // 退出异常
            Exception exitException = null;
            // 判断是否关机，以及是否超过端口绑定的最大重试次数
            while ((!shutdown) && (portBindMaxRetry == 0 || numRetries < portBindMaxRetry)) {
                try {
                    /*        根据是否启用TLS，来创建Socket连接，Socket也被称为“套接字”       */
                    if (self.shouldUsePortUnification()) {
                        LOG.info("Creating TLS-enabled quorum server socket");
                        ss = new UnifiedServerSocket(self.getX509Util(), true);
                    } else if (self.isSslQuorum()) {
                        LOG.info("Creating TLS-only quorum server socket");
                        ss = new UnifiedServerSocket(self.getX509Util(), false);
                    } else {
                        ss = new ServerSocket();
                    }

                    /*
                     * TCP的主要设计目标是在出现数据包丢失，数据包重新排序以及数据包复制（此处是关键）的情况下，实现可靠的数据通信。
                     * 在使用bind（SocketAddress）绑定套接字之前启用SO_REUSEADDR，即使先前的连接处于超时状态，也可以绑定套接字。
                     * 有关详细描述：https://stackoverflow.com/questions/3229860/what-is-the-meaning-of-so-reuseaddr-setsockopt-option-linux
                     */
                    ss.setReuseAddress(true);


                    /*        绑定IP和端口       */
                    // 是否不听就两个法定人数端口（广播和快速的地区领导人选举）所有IP
                    if (self.getQuorumListenOnAllIPs()) {
                        int port = self.getElectionAddress().getPort();
                        addr = new InetSocketAddress(port);
                    } else {
                        // 如果基础IP地址已更改，请解析该服务器的主机名。
                        self.recreateSocketAddresses(self.getId());
                        addr = self.getElectionAddress();
                    }
                    // 绑定选举的端口
                    LOG.info("My election bind port: " + addr.toString());
                    setName(addr.toString());
                    // 绑定
                    ss.bind(addr);

                    // 不断判断是否停止，停止意味着选举完成，已产生Leader
                    while (!shutdown) {
                        try {
                            // Socket accept()：阻塞，等待接收来自对等节点客户端的请求
                            client = ss.accept();

                            // 设置Socket优化选项，如：关闭缓冲区、设置心跳连接
                            setSockOpts(client);
                            LOG.info("Received connection request "
                                    + formatInetAddr((InetSocketAddress) client.getRemoteSocketAddress()));


                            // 如果启用了法定sasl身份验证异步接收和处理连接请求。
                            // 这是必需的，因为sasl服务器身份验证过程可能需要几秒钟才能完成，这可能会延迟下一个对等连接请求。
                            if (quorumSaslAuthEnabled) {
                                // TODO SASL：连接认证，启用后将使用异步接受连接，因为sasl服务器身份验证过程可能需要几秒钟才能完成，这可能会延迟下一个对等连接请求。
                                receiveConnectionAsync(client);
                            } else {
                                // TODO 真正的连接处理逻辑，处理来自客户端的连接
                                receiveConnection(client);
                            }
                            numRetries = 0;
                        } catch (SocketTimeoutException e) {
                            LOG.warn("The socket is listening for the election accepted "
                                    + "and it timed out unexpectedly, but will retry."
                                    + "see ZOOKEEPER-2836");
                        }
                    }
                } catch (IOException e) {
                    if (shutdown) {
                        break;
                    }
                    LOG.error("Exception while listening", e);
                    exitException = e;
                    numRetries++;
                    try {
                        // 释放Socket连接
                        ss.close();
                        Thread.sleep(1000);
                    } catch (IOException ie) {
                        LOG.error("Error closing server socket", ie);
                    } catch (InterruptedException ie) {
                        LOG.error("Interrupted while sleeping. " +
                                "Ignoring exception", ie);
                    }
                    closeSocket(client);
                }
            }
            LOG.info("Leaving listener");
            if (!shutdown) {
                LOG.error("As I'm leaving the listener thread after "
                        + numRetries + " errors. "
                        + "I won't be able to participate in leader "
                        + "election any longer: "
                        + formatInetAddr(self.getElectionAddress())
                        + ". Use " + ELECTION_PORT_BIND_RETRY + " property to "
                        + "increase retry count.");
                if (exitException instanceof SocketException) {
                    // 离开侦听器线程后，主机不能再加入仲裁，这是一个严重的错误，我们无法从中恢复，因此我们需要退出
                    socketBindErrorHandler.run();
                }
            } else if (ss != null) {
                // 清理关机。
                try {
                    ss.close();
                } catch (IOException ie) {
                    // Don't log an error for shutdown.
                    LOG.debug("Error closing server socket", ie);
                }
            }
        }

        /**
         * 暂停此侦听器线程。
         */
        void halt() {
            try {
                LOG.debug("Trying to close listener: " + ss);
                if (ss != null) {
                    LOG.debug("Closing listener: "
                            + QuorumCnxManager.this.mySid);
                    ss.close();
                }
            } catch (IOException e) {
                LOG.warn("Exception when shutting down listener: " + e);
            }
        }
    }

    /**
     * 线程发送消息。 实例等待队列上，并尽快发送一个消息有一个可用。
     * 如果连接断开，然后打开一个新的。
     */
    class SendWorker extends ZooKeeperThread {
        Long sid;
        Socket sock;
        RecvWorker recvWorker;
        volatile boolean running = true;
        DataOutputStream dout;

        /**
         * 该线程的一个实例通过一个队列接收要发送的消息，并将它们发送到服务器的sid。
         *
         * @param sock Socket to remote peer
         * @param sid  Server identifier of remote peer
         */
        SendWorker(Socket sock, Long sid) {
            super("SendWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            recvWorker = null;
            try {
                dout = new DataOutputStream(sock.getOutputStream());
            } catch (IOException e) {
                LOG.error("Unable to access socket output stream", e);
                closeSocket(sock);
                running = false;
            }
            LOG.debug("Address of remote peer: " + this.sid);
        }

        synchronized void setRecv(RecvWorker recvWorker) {
            this.recvWorker = recvWorker;
        }

        /**
         * 返回与此SendWorker配对的RecvWorker。
         *
         * @return RecvWorker
         */
        synchronized RecvWorker getRecvWorker() {
            return recvWorker;
        }

        synchronized boolean finish() {
            LOG.debug("Calling finish for " + sid);

            if (!running) {
                /*
                 * 避免两次运行finish（）。
                 */
                return running;
            }

            running = false;
            // 关闭Socket连接
            closeSocket(sock);

            // 线程中断：interrupt()方法只是将线程状态改变成中断状态，不会中断正在运行的线程
            // 当线程被wait()/join()/sleep，再调用interrupt()会抛出interruptedException

            // 这里标识成“中断”状态的目的使当前线程，无法再被wait()/join()/sleep，线程将一直运行
            this.interrupt();

            if (recvWorker != null) {
                // 同上，使 RecvWorker 接收工作线程也无法被中断
                recvWorker.finish();
            }

            LOG.debug("Removing entry from senderWorkerMap sid=" + sid);

            senderWorkerMap.remove(sid, this);
            threadCnt.decrementAndGet();
            return running;
        }


        synchronized void send(ByteBuffer b) throws IOException {
            byte[] msgBytes = new byte[b.capacity()];
            try {
                b.position(0);
                b.get(msgBytes);
            } catch (BufferUnderflowException be) {
                LOG.error("BufferUnderflowException ", be);
                return;
            }
            dout.writeInt(b.capacity());
            dout.write(b.array());
            dout.flush();
        }

        @Override
        public void run() {
            threadCnt.incrementAndGet();
            try {
                /**
                 * 如果队列中没有要发送的内容，那么我们发送lastMessage以确保对等方接收到最后一条消息。
                 * 如果自身或对等方在读取/处理最后一条消息之前关闭了它们的连接（并退出线程），则可能会丢弃该消息。
                 *
                 * 重复消息由对等方正确处理。
                 *
                 * 如果发送队列是非空的，那么我们的最新消息比lastMessage中存储的消息要新。
                 *
                 * 为了避免发送过时的消息，我们应该在发送队列中发送该消息。
                 */
                ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                if (bq == null || isSendQueueEmpty(bq)) {
                    // 如果队列中没有要发送的内容，那么我们发送lastMessage以确保对等方接收到最后一条消息。
                    ByteBuffer b = lastMessageSent.get(sid);
                    if (b != null) {
                        LOG.debug("Attempting to send lastMessage to sid=" + sid);
                        // 发送sid
                        send(b);
                    }
                }
            } catch (IOException e) {
                LOG.error("Failed to send last message. Shutting down thread.", e);
                this.finish();
            }

            try {
                while (running && !shutdown && sock != null) {

                    ByteBuffer b = null;
                    try {
                        ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                        if (bq != null) {
                            // 获取并移除缓存在此队列的头，如有必要，等待将在指定的等待时间的可用元素。
                            b = pollSendQueue(bq, 1000, TimeUnit.MILLISECONDS);
                        } else {
                            LOG.error("No queue of incoming messages for " +
                                    "server " + sid);
                            break;
                        }

                        if (b != null) {
                            lastMessageSent.put(sid, b);
                            send(b);
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for message on queue",
                                e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception when using channel: for id " + sid
                        + " my id = " + QuorumCnxManager.this.mySid
                        + " error = " + e);
            }
            // TODO 没发送一次消息，就finish()一次
            this.finish();
            LOG.warn("Send worker leaving thread " + " id " + sid + " my id = " + self.getId());
        }
    }

    /**
     * Thread to receive messages. Instance waits on a socket read. If the
     * channel breaks, then removes itself from the pool of receivers.
     */
    class RecvWorker extends ZooKeeperThread {
        Long sid;
        Socket sock;
        volatile boolean running = true;
        final DataInputStream din;
        final SendWorker sw;

        RecvWorker(Socket sock, DataInputStream din, Long sid, SendWorker sw) {
            super("RecvWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            this.sw = sw;
            this.din = din;
            try {
                // 单击确定，等到套接字在读取时断开连接。
                sock.setSoTimeout(0);
            } catch (IOException e) {
                LOG.error("Error while accessing socket for " + sid, e);
                closeSocket(sock);
                running = false;
            }
        }

        /**
         * Shuts down this worker
         *
         * @return boolean  Value of variable running
         */
        synchronized boolean finish() {
            if (!running) {
                /*
                 * Avoids running finish() twice.
                 */
                return running;
            }
            running = false;

            this.interrupt();
            threadCnt.decrementAndGet();
            return running;
        }

        @Override
        public void run() {
            threadCnt.incrementAndGet();
            try {
                while (running && !shutdown && sock != null) {
                    /**
                     * 读取第一个int以确定消息的长度
                     */
                    int length = din.readInt();
                    if (length <= 0 || length > PACKETMAXSIZE) {
                        throw new IOException(
                                "Received packet with invalid packet: "
                                        + length);
                    }
                    /**
                     * 分配一个新的ByteBuffer接收消息
                     */
                    byte[] msgArray = new byte[length];
                    din.readFully(msgArray, 0, length);
                    ByteBuffer message = ByteBuffer.wrap(msgArray);
                    // 添加到接收队列
                    addToRecvQueue(new Message(message.duplicate(), sid));
                }
            } catch (Exception e) {
                LOG.warn("Connection broken for id " + sid + ", my id = "
                        + QuorumCnxManager.this.mySid + ", error = ", e);
            } finally {
                LOG.warn("Interrupting SendWorker");
                sw.finish();
                closeSocket(sock);
            }
        }
    }

    /**
     * Inserts an element in the specified queue. If the Queue is full, this
     * method removes an element from the head of the Queue and then inserts
     * the element at the tail. It can happen that the an element is removed
     * by another thread in {@link SendWorker#processMessage() processMessage}
     * method before this method attempts to remove an element from the queue.
     * This will cause {@link ArrayBlockingQueue#remove() remove} to throw an
     * exception, which is safe to ignore.
     * <p>
     * Unlike {@link #addToRecvQueue(Message) addToRecvQueue} this method does
     * not need to be synchronized since there is only one thread that inserts
     * an element in the queue and another thread that reads from the queue.
     *
     * @param queue  Reference to the Queue
     * @param buffer Reference to the buffer to be inserted in the queue
     */
    private void addToSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
                                ByteBuffer buffer) {
        if (queue.remainingCapacity() == 0) {
            try {
                queue.remove();
            } catch (NoSuchElementException ne) {
                // element could be removed by poll()
                LOG.debug("Trying to remove from an empty " +
                        "Queue. Ignoring exception " + ne);
            }
        }
        try {
            queue.add(buffer);
        } catch (IllegalStateException ie) {
            // This should never happen
            LOG.error("Unable to insert an element in the queue " + ie);
        }
    }

    /**
     * Returns true if queue is empty.
     *
     * @param queue Reference to the queue
     * @return true if the specified queue is empty
     */
    private boolean isSendQueueEmpty(ArrayBlockingQueue<ByteBuffer> queue) {
        return queue.isEmpty();
    }

    /**
     * Retrieves and removes buffer at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     * <p>
     * {@link ArrayBlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    private ByteBuffer pollSendQueue(ArrayBlockingQueue<ByteBuffer> queue,
                                     long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    /**
     * Inserts an element in the {@link #recvQueue}. If the Queue is full, this
     * methods removes an element from the head of the Queue and then inserts
     * the element at the tail of the queue.
     * <p>
     * This method is synchronized to achieve fairness between two threads that
     * are trying to insert an element in the queue. Each thread checks if the
     * queue is full, then removes the element at the head of the queue, and
     * then inserts an element at the tail. This three-step process is done to
     * prevent a thread from blocking while inserting an element in the queue.
     * If we do not synchronize the call to this method, then a thread can grab
     * a slot in the queue created by the second thread. This can cause the call
     * to insert by the second thread to fail.
     * Note that synchronizing this method does not block another thread
     * from polling the queue since that synchronization is provided by the
     * queue itself.
     *
     * @param msg Reference to the message to be inserted in the queue
     */
    public void addToRecvQueue(Message msg) {
        synchronized (recvQLock) {
            if (recvQueue.remainingCapacity() == 0) {
                try {
                    recvQueue.remove();
                } catch (NoSuchElementException ne) {
                    // element could be removed by poll()
                    LOG.debug("Trying to remove from an empty " +
                            "recvQueue. Ignoring exception " + ne);
                }
            }
            try {
                recvQueue.add(msg);
            } catch (IllegalStateException ie) {
                // This should never happen
                LOG.error("Unable to insert element in the recvQueue " + ie);
            }
        }
    }

    /**
     * 检索并移除在这个队列的头部的消息，如果需要等待将在指定的等待时间的元件变得可用。
     * <p>
     * {@link ArrayBlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    public Message pollRecvQueue(long timeout, TimeUnit unit)
            throws InterruptedException {
        return recvQueue.poll(timeout, unit);
    }

    public boolean connectedToPeer(long peerSid) {
        return senderWorkerMap.get(peerSid) != null;
    }
}
