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

package org.apache.zookeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * NIOServerCnxnFactory implements a multi-threaded ServerCnxnFactory using
 * NIO non-blocking socket calls. Communication between threads is handled via
 * queues.
 * <p>
 * - 1   accept thread, which accepts new connections and assigns to a
 * selector thread
 * - 1-N selector threads, each of which selects on 1/N of the connections.
 * The reason the factory supports more than one selector thread is that
 * with large numbers of connections, select() itself can become a
 * performance bottleneck.
 * - 0-M socket I/O worker threads, which perform basic socket reads and
 * writes. If configured with 0 worker threads, the selector threads
 * do the socket I/O directly.
 * - 1   connection expiration thread, which closes idle connections; this is
 * necessary to expire connections on which no session is established.
 * <p>
 * Typical (default) thread counts are: on a 32 core machine, 1 accept thread,
 * 1 connection expiration thread, 4 selector threads, and 64 worker threads.
 */
public class NIOServerCnxnFactory extends ServerCnxnFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxnFactory.class);

    /**
     * Default sessionless connection timeout in ms: 10000 (10s)
     */
    public static final String ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT =
            "zookeeper.nio.sessionlessCnxnTimeout";
    /**
     * With 500 connections to an observer with watchers firing on each, is
     * unable to exceed 1GigE rates with only 1 selector.
     * Defaults to using 2 selector threads with 8 cores and 4 with 32 cores.
     * Expressed as sqrt(numCores/2). Must have at least 1 selector thread.
     */
    public static final String ZOOKEEPER_NIO_NUM_SELECTOR_THREADS =
            "zookeeper.nio.numSelectorThreads";
    /**
     * Default: 2 * numCores
     */
    public static final String ZOOKEEPER_NIO_NUM_WORKER_THREADS =
            "zookeeper.nio.numWorkerThreads";
    /**
     * Default: 64kB
     */
    public static final String ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES =
            "zookeeper.nio.directBufferBytes";
    /**
     * Default worker pool shutdown timeout in ms: 5000 (5s)
     */
    public static final String ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT =
            "zookeeper.nio.shutdownTimeout";

    static {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Thread " + t + " died", e);
            }
        });
        /**
         * this is to avoid the jvm bug:
         * NullPointerException in Selector.open()
         * http://bugs.sun.com/view_bug.do?bug_id=6427854
         */
        try {
            Selector.open().close();
        } catch (IOException ie) {
            LOG.error("Selector failed to open", ie);
        }

        /**
         * Value of 0 disables use of direct buffers and instead uses
         * gathered write call.
         *
         * Default to using 64k direct buffers.
         */
        directBufferBytes = Integer.getInteger(
                ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES, 64 * 1024);
    }

    /**
     * AbstractSelectThread is an abstract base class containing a few bits
     * of code shared by the AcceptThread (which selects on the listen socket)
     * and SelectorThread (which selects on client connections) classes.
     */
    private abstract class AbstractSelectThread extends ZooKeeperThread {
        protected final Selector selector;

        public AbstractSelectThread(String name) throws IOException {
            super(name);
            // Allows the JVM to shutdown even if this thread is still running.
            setDaemon(true);
            this.selector = Selector.open();
        }

        public void wakeupSelector() {
            selector.wakeup();
        }

        /**
         * Close the selector. This should be called when the thread is about to
         * exit and no operation is going to be performed on the Selector or
         * SelectionKey
         */
        protected void closeSelector() {
            try {
                selector.close();
            } catch (IOException e) {
                LOG.warn("ignored exception during selector close "
                        + e.getMessage());
            }
        }

        protected void cleanupSelectionKey(SelectionKey key) {
            if (key != null) {
                try {
                    key.cancel();
                } catch (Exception ex) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("ignoring exception during selectionkey cancel", ex);
                    }
                }
            }
        }

        protected void fastCloseSock(SocketChannel sc) {
            if (sc != null) {
                try {
                    // Hard close immediately, discarding buffers
                    sc.socket().setSoLinger(true, 0);
                } catch (SocketException e) {
                    LOG.warn("Unable to set socket linger to 0, socket close"
                            + " may stall in CLOSE_WAIT", e);
                }
                NIOServerCnxn.closeSock(sc);
            }
        }
    }

    /**
     * There is a single AcceptThread which accepts new connections and assigns
     * them to a SelectorThread using a simple round-robin scheme to spread
     * them across the SelectorThreads. It enforces maximum number of
     * connections per IP and attempts to cope with running out of file
     * descriptors by briefly sleeping before retrying.
     */
    private class AcceptThread extends AbstractSelectThread {
        private final ServerSocketChannel acceptSocket;
        private final SelectionKey acceptKey;
        private final RateLogger acceptErrorLogger = new RateLogger(LOG);
        private final Collection<SelectorThread> selectorThreads;
        private Iterator<SelectorThread> selectorIterator;
        private volatile boolean reconfiguring = false;

        public AcceptThread(ServerSocketChannel ss, InetSocketAddress addr,
                            Set<SelectorThread> selectorThreads) throws IOException {
            super("NIOServerCxnFactory.AcceptThread:" + addr);
            this.acceptSocket = ss;
            this.acceptKey =
                    acceptSocket.register(selector, SelectionKey.OP_ACCEPT);
            this.selectorThreads = Collections.unmodifiableList(
                    new ArrayList<SelectorThread>(selectorThreads));
            selectorIterator = this.selectorThreads.iterator();
        }

        public void run() {
            try {
                while (!stopped && !acceptSocket.socket().isClosed()) {
                    try {
                        select();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }
            } finally {
                closeSelector();
                // This will wake up the selector threads, and tell the
                // worker thread pool to begin shutdown.
                if (!reconfiguring) {
                    NIOServerCnxnFactory.this.stop();
                }
                LOG.info("accept thread exitted run method");
            }
        }

        public void setReconfiguring() {
            reconfiguring = true;
        }

        private void select() {
            try {
                selector.select();

                Iterator<SelectionKey> selectedKeys =
                        selector.selectedKeys().iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        if (!doAccept()) {
                            // If unable to pull a new connection off the accept
                            // queue, pause accepting to give us time to free
                            // up file descriptors and so the accept thread
                            // doesn't spin in a tight loop.
                            pauseAccept(10);
                        }
                    } else {
                        LOG.warn("Unexpected ops in accept select "
                                + key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Mask off the listen socket interest ops and use select() to sleep
         * so that other threads can wake us up by calling wakeup() on the
         * selector.
         */
        private void pauseAccept(long millisecs) {
            acceptKey.interestOps(0);
            try {
                selector.select(millisecs);
            } catch (IOException e) {
                // ignore
            } finally {
                acceptKey.interestOps(SelectionKey.OP_ACCEPT);
            }
        }

        /**
         * Accept new socket connections. Enforces maximum number of connections
         * per client IP address. Round-robin assigns to selector thread for
         * handling. Returns whether pulled a connection off the accept queue
         * or not. If encounters an error attempts to fast close the socket.
         *
         * @return whether was able to accept a connection or not
         */
        private boolean doAccept() {
            boolean accepted = false;
            SocketChannel sc = null;
            try {
                // 接受连接
                sc = acceptSocket.accept();
                accepted = true;
                InetAddress ia = sc.socket().getInetAddress();
                int cnxncount = getClientCnxnCount(ia);

                if (maxClientCnxns > 0 && cnxncount >= maxClientCnxns) {
                    throw new IOException("Too many connections from " + ia
                            + " - max is " + maxClientCnxns);
                }

                LOG.debug("Accepted socket connection from "
                        + sc.socket().getRemoteSocketAddress());
                sc.configureBlocking(false);

                // Round-robin assign this connection to a selector thread
                if (!selectorIterator.hasNext()) {
                    selectorIterator = selectorThreads.iterator();
                }
                SelectorThread selectorThread = selectorIterator.next();
                if (!selectorThread.addAcceptedConnection(sc)) {
                    throw new IOException(
                            "Unable to add connection to selector queue"
                                    + (stopped ? " (shutdown in progress)" : ""));
                }
                acceptErrorLogger.flush();
            } catch (IOException e) {
                // accept, maxClientCnxns, configureBlocking
                acceptErrorLogger.rateLimitLog(
                        "Error accepting new connection: " + e.getMessage());
                fastCloseSock(sc);
            }
            return accepted;
        }
    }

    /**
     * SelectorThread从 AcceptThread接收新接受的连接，并负责选择连接中的I / O准备状态。
     * 该线程是唯一在选择器上执行任何非线程安全或可能阻塞的调用的线程（注册新连接和读取/写入兴趣操作）。
     * 对SelectorThread的连接分配是永久的，只有 SelectorThread将与该连接进行交互。
     * 有1-N个SelectorThreads，在* SelectorThreads之间平均分配了连接。
     * 如果存在工作线程池，则当连接具有要执行的I / O时 SelectorThread通过清除其兴趣将其从选择中删除*
     * ops并安排I / O进行工作线程的处理。工作完成后，将连接放置在就绪队列中，以恢复其关注的操作并恢复选择。
     * 如果没有辅助线程池，则SelectorThread将直接执行I / O 。
     */
    class SelectorThread extends AbstractSelectThread {
        private final int id;
        private final Queue<SocketChannel> acceptedQueue;
        private final Queue<SelectionKey> updateQueue;

        public SelectorThread(int id) throws IOException {
            super("NIOServerCxnFactory.SelectorThread-" + id);
            this.id = id;
            acceptedQueue = new LinkedBlockingQueue<SocketChannel>();
            updateQueue = new LinkedBlockingQueue<SelectionKey>();
        }

        /**
         * 将新接受的连接放入队列以进行添加。这样做*因此，只有选择器线程会修改在选择器中注册的键*。
         */
        public boolean addAcceptedConnection(SocketChannel accepted) {
            if (stopped || !acceptedQueue.offer(accepted)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

        /**
         * 将兴趣操作更新请求放入队列中，以便仅
         * 选择器线程修改兴趣操作，因为如果发生其他select
         * 操作，则兴趣操作读取/设置可能会阻塞操作。
         */
        public boolean addInterestOpsUpdateRequest(SelectionKey sk) {
            if (stopped || !updateQueue.offer(sk)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

        /**
         * 线程的主循环在连接上执行selects（）并分派就绪的I / O工作请求，
         * 然后注册所有未决的*新接受的连接并更新*队列上的所有兴趣操作。
         * // TODO 选择器 -> 多路复用
         */
        public void run() {
            try {
                // 未停止将一直循环
                while (!stopped) {
                    try {
                        select();
                        // 处理接受的连接
                        processAcceptedConnections();
                        // 处理兴趣操作更新请求
                        processInterestOpsUpdateRequests();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }


                /*        断开连接       */

                // 关闭仍在选择器上挂起的连接。任何其他正在进行中的工作让工作流失。
                for (SelectionKey key : selector.keys()) {
                    NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                    if (cnxn.isSelectable()) {
                        cnxn.close();// 释放资源
                    }
                    // 关闭选择器
                    cleanupSelectionKey(key);
                }
                SocketChannel accepted;
                while ((accepted = acceptedQueue.poll()) != null) {
                    // 快速关闭Socket连接
                    fastCloseSock(accepted);
                }
                updateQueue.clear();
            } finally {
                closeSelector();
                // 这将唤醒接受线程和其他选择器线程，并告诉工作线程池开始关闭。
                NIOServerCnxnFactory.this.stop();
                LOG.info("selector thread exitted run method");
            }
        }

        private void select() {
            try {
                // 选择所有就绪的连接
                selector.select();

                // TODO SelectionKey
                Set<SelectionKey> selected = selector.selectedKeys();
                ArrayList<SelectionKey> selectedList =
                        new ArrayList<SelectionKey>(selected);
                Collections.shuffle(selectedList);
                Iterator<SelectionKey> selectedKeys = selectedList.iterator();

                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selected.remove(key);

                    // 若连接已失效，直接清理掉
                    if (!key.isValid()) {
                        // 注销选择器
                        cleanupSelectionKey(key);
                        continue;
                    }
                    // 客户端已经准备好数据后，才可读/写
                    if (key.isReadable() || key.isWritable()) {
                        // IO 处理器
                        handleIO(key);
                    } else {
                        LOG.warn("Unexpected ops in select " + key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * 计划I / O，以处理与给定SelectionKey关联的连接。
         * 如果未使用工作线程池，则此线程直接运行I / O。
         */
        private void handleIO(SelectionKey key) {
            IOWorkRequest workRequest = new IOWorkRequest(this, key);
            // NIO建立socket连接管道后被命名为cnxn连接
            NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();

            // 在处理其连接时停止选择此键，禁止可选
            cnxn.disableSelectable();
            // 唤醒线程
            key.interestOps(0);
            touchCnxn(cnxn);
            workerPool.schedule(workRequest);
        }

        /**
         * 终止已被分配给该线程但尚未放置在选择器上的接受连接的队列。
         */
        private void processAcceptedConnections() {
            SocketChannel accepted;
            while (!stopped && (accepted = acceptedQueue.poll()) != null) {
                SelectionKey key = null;
                try {
                    key = accepted.register(selector, SelectionKey.OP_READ);
                    NIOServerCnxn cnxn = createConnection(accepted, key, this);
                    key.attach(cnxn);
                    addCnxn(cnxn);
                } catch (IOException e) {
                    // register, createConnection
                    cleanupSelectionKey(key);
                    fastCloseSock(accepted);
                }
            }
        }

        /**
         * 遍历准备恢复选择的连接队列，并恢复其兴趣操作选择掩码。
         */
        private void processInterestOpsUpdateRequests() {
            SelectionKey key;
            while (!stopped && (key = updateQueue.poll()) != null) {
                if (!key.isValid()) {
                    cleanupSelectionKey(key);
                }
                NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                if (cnxn.isSelectable()) {
                    key.interestOps(cnxn.getInterestOps());
                }
            }
        }
    }

    /**
     * IOWorkRequest是一个小的包装器类，允许使用WorkerService在连接上运行doIO（）调用。
     */
    private class IOWorkRequest extends WorkerService.WorkRequest {
        private final SelectorThread selectorThread;
        private final SelectionKey key;
        private final NIOServerCnxn cnxn;

        IOWorkRequest(SelectorThread selectorThread, SelectionKey key) {
            this.selectorThread = selectorThread;
            this.key = key;
            this.cnxn = (NIOServerCnxn) key.attachment();
        }

        public void doWork() throws InterruptedException {
            if (!key.isValid()) {
                selectorThread.cleanupSelectionKey(key);
                return;
            }

            if (key.isReadable() || key.isWritable()) {
                cnxn.doIO(key);

                // 检查我们是否关闭或doIO（）关闭了此连接
                if (stopped) {
                    cnxn.close();
                    return;
                }
                if (!key.isValid()) {
                    selectorThread.cleanupSelectionKey(key);
                    return;
                }
                touchCnxn(cnxn);
            }

            // Mark this connection as once again ready for selection
            cnxn.enableSelectable();
            // Push an update request on the queue to resume selecting
            // on the current set of interest ops, which may have changed
            // as a result of the I/O operations we just performed.
            if (!selectorThread.addInterestOpsUpdateRequest(key)) {
                cnxn.close();
            }
        }

        @Override
        public void cleanup() {
            cnxn.close();
        }
    }

    /**
     * This thread is responsible for closing stale connections so that
     * connections on which no session is established are properly expired.
     */
    private class ConnectionExpirerThread extends ZooKeeperThread {
        ConnectionExpirerThread() {
            super("ConnnectionExpirer");
        }

        public void run() {
            try {
                while (!stopped) {
                    long waitTime = cnxnExpiryQueue.getWaitTime();
                    if (waitTime > 0) {
                        Thread.sleep(waitTime);
                        continue;
                    }
                    for (NIOServerCnxn conn : cnxnExpiryQueue.poll()) {
                        conn.close();
                    }
                }

            } catch (InterruptedException e) {
                LOG.info("ConnnectionExpirerThread interrupted");
            }
        }
    }

    ServerSocketChannel ss;

    /**
     * We use this buffer to do efficient socket I/O. Because I/O is handled
     * by the worker threads (or the selector threads directly, if no worker
     * thread pool is created), we can create a fixed set of these to be
     * shared by connections.
     */
    private static final ThreadLocal<ByteBuffer> directBuffer =
            new ThreadLocal<ByteBuffer>() {
                @Override
                protected ByteBuffer initialValue() {
                    return ByteBuffer.allocateDirect(directBufferBytes);
                }
            };

    public static ByteBuffer getDirectBuffer() {
        return directBufferBytes > 0 ? directBuffer.get() : null;
    }

    // sessionMap is used by closeSession()
    private final ConcurrentHashMap<Long, NIOServerCnxn> sessionMap =
            new ConcurrentHashMap<Long, NIOServerCnxn>();
    // ipMap is used to limit connections per IP
    private final ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>> ipMap =
            new ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>>();

    protected int maxClientCnxns = 60;

    int sessionlessCnxnTimeout;
    private ExpiryQueue<NIOServerCnxn> cnxnExpiryQueue;


    protected WorkerService workerPool;

    private static int directBufferBytes;
    private int numSelectorThreads;
    private int numWorkerThreads;
    private long workerShutdownTimeoutMS;

    /**
     * Construct a new server connection factory which will accept an unlimited number
     * of concurrent connections from each client (up to the file descriptor
     * limits of the operating system). startup(zks) must be called subsequently.
     */
    public NIOServerCnxnFactory() {
    }

    private volatile boolean stopped = true;
    private ConnectionExpirerThread expirerThread;
    private AcceptThread acceptThread;
    private final Set<SelectorThread> selectorThreads =
            new HashSet<SelectorThread>();

    @Override
    public void configure(InetSocketAddress addr, int maxcc, boolean secure) throws IOException {
        if (secure) {
            throw new UnsupportedOperationException("SSL isn't supported in NIOServerCnxn");
        }
        configureSaslLogin();// SASL 客户端身份验证

        maxClientCnxns = maxcc;
        sessionlessCnxnTimeout = Integer.getInteger(
                ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT, 10000);
        // 我们还将sessionlessCnxnTimeout用作 cnxnExpiryQueue的到期间隔。
        // 这些不需要相同，但是传递到下面的ExpiryQueue（）构造函数中的到期间隔应该小于或等于超时。
        // interval passed into the ExpiryQueue() constructor below should be
        // less than or equal to the timeout.
        cnxnExpiryQueue =
                new ExpiryQueue<NIOServerCnxn>(sessionlessCnxnTimeout);
        expirerThread = new ConnectionExpirerThread();

        int numCores = Runtime.getRuntime().availableProcessors(); // 可用处理器
        // 32核心的最佳选择似乎是4个选择器线程
        numSelectorThreads = Integer.getInteger(
                ZOOKEEPER_NIO_NUM_SELECTOR_THREADS,
                Math.max((int) Math.sqrt((float) numCores / 2), 1));
        if (numSelectorThreads < 1) {
            throw new IOException("numSelectorThreads must be at least 1");
        }

        numWorkerThreads = Integer.getInteger(
                ZOOKEEPER_NIO_NUM_WORKER_THREADS, 2 * numCores);
        workerShutdownTimeoutMS = Long.getLong(
                ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT, 5000);

        LOG.info("Configuring NIO connection handler with "
                + (sessionlessCnxnTimeout / 1000) + "s sessionless connection"
                + " timeout, " + numSelectorThreads + " selector thread(s), "
                + (numWorkerThreads > 0 ? numWorkerThreads : "no")
                + " worker threads, and "
                + (directBufferBytes == 0 ? "gathered writes." :
                ("" + (directBufferBytes / 1024) + " kB direct buffers.")));
        for (int i = 0; i < numSelectorThreads; ++i) {
            selectorThreads.add(new SelectorThread(i));
        }

        this.ss = ServerSocketChannel.open();
        ss.socket().setReuseAddress(true);
        LOG.info("binding to port " + addr);
        ss.socket().bind(addr);
        ss.configureBlocking(false);
        acceptThread = new AcceptThread(ss, addr, selectorThreads);
    }

    private void tryClose(ServerSocketChannel s) {
        try {
            s.close();
        } catch (IOException sse) {
            LOG.error("Error while closing server socket.", sse);
        }
    }

    @Override
    public void reconfigure(InetSocketAddress addr) {
        ServerSocketChannel oldSS = ss;
        try {
            acceptThread.setReconfiguring();
            tryClose(oldSS);
            acceptThread.wakeupSelector();
            try {
                acceptThread.join();
            } catch (InterruptedException e) {
                LOG.error("Error joining old acceptThread when reconfiguring client port {}",
                        e.getMessage());
                Thread.currentThread().interrupt();
            }
            this.ss = ServerSocketChannel.open();
            ss.socket().setReuseAddress(true);
            LOG.info("binding to port " + addr);
            ss.socket().bind(addr);
            ss.configureBlocking(false);
            acceptThread = new AcceptThread(ss, addr, selectorThreads);
            acceptThread.start();
        } catch (IOException e) {
            LOG.error("Error reconfiguring client port to {} {}", addr, e.getMessage());
            tryClose(oldSS);
        }
    }

    /**
     * {@inheritDoc}
     */
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    /**
     * {@inheritDoc}
     */
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }

    @Override
    public void start() {
        stopped = false;
        if (workerPool == null) {
            workerPool = new WorkerService(
                    "NIOWorker", numWorkerThreads, false);
        }
        // TODO 选择器线程
        for (SelectorThread thread : selectorThreads) {
            if (thread.getState() == Thread.State.NEW) {
                thread.start();
            }
        }
        // 确保线程仅启动一次
        if (acceptThread.getState() == Thread.State.NEW) {
            acceptThread.start();
        }
        if (expirerThread.getState() == Thread.State.NEW) {
            expirerThread.start();
        }
    }

    @Override
    public void startup(ZooKeeperServer zks, boolean startServer)
            throws IOException, InterruptedException {
        start();
        setZooKeeperServer(zks);
        if (startServer) {
            // 初始化ZKDataBase，并且从快照回复数据
            zks.startdata();
            // 启动Zookeeper，包括：创建会话跟踪器、启动会话淘汰策略任务、设置请求处理器
            zks.startup();
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) ss.socket().getLocalSocketAddress();
    }

    @Override
    public int getLocalPort() {
        return ss.socket().getLocalPort();
    }

    /**
     * De-registers the connection from the various mappings maintained
     * by the factory.
     */
    public boolean removeCnxn(NIOServerCnxn cnxn) {
        // If the connection is not in the master list it's already been closed
        if (!cnxns.remove(cnxn)) {
            return false;
        }
        cnxnExpiryQueue.remove(cnxn);

        long sessionId = cnxn.getSessionId();
        if (sessionId != 0) {
            sessionMap.remove(sessionId);
        }

        InetAddress addr = cnxn.getSocketAddress();
        if (addr != null) {
            Set<NIOServerCnxn> set = ipMap.get(addr);
            if (set != null) {
                set.remove(cnxn);
                // Note that we make no effort here to remove empty mappings
                // from ipMap.
            }
        }

        // unregister from JMX
        unregisterConnection(cnxn);
        return true;
    }

    /**
     * 在我们的cnxnExpiryQueue中添加或更新cnxn
     *
     * @param cnxn
     */
    public void touchCnxn(NIOServerCnxn cnxn) {
        cnxnExpiryQueue.update(cnxn, cnxn.getSessionTimeout());
    }

    private void addCnxn(NIOServerCnxn cnxn) throws IOException {
        InetAddress addr = cnxn.getSocketAddress();
        if (addr == null) {
            throw new IOException("Socket of " + cnxn + " has been closed");
        }
        Set<NIOServerCnxn> set = ipMap.get(addr);
        if (set == null) {
            // in general we will see 1 connection from each
            // host, setting the initial cap to 2 allows us
            // to minimize mem usage in the common case
            // of 1 entry --  we need to set the initial cap
            // to 2 to avoid rehash when the first entry is added
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(
                    new ConcurrentHashMap<NIOServerCnxn, Boolean>(2));
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<NIOServerCnxn> existingSet = ipMap.putIfAbsent(addr, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(cnxn);

        cnxns.add(cnxn);
        touchCnxn(cnxn);
    }

    protected NIOServerCnxn createConnection(SocketChannel sock,
                                             SelectionKey sk, SelectorThread selectorThread) throws IOException {
        return new NIOServerCnxn(zkServer, sock, sk, this, selectorThread);
    }

    private int getClientCnxnCount(InetAddress cl) {
        Set<NIOServerCnxn> s = ipMap.get(cl);
        if (s == null) return 0;
        return s.size();
    }

    /**
     * clear all the connections in the selector
     */
    @Override
    @SuppressWarnings("unchecked")
    public void closeAll() {
        // clear all the connections on which we are selecting
        for (ServerCnxn cnxn : cnxns) {
            try {
                // This will remove the cnxn from cnxns
                cnxn.close();
            } catch (Exception e) {
                LOG.warn("Ignoring exception closing cnxn sessionid 0x"
                        + Long.toHexString(cnxn.getSessionId()), e);
            }
        }
    }

    public void stop() {
        stopped = true;

        // Stop queuing connection attempts
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Error closing listen socket", e);
        }

        if (acceptThread != null) {
            if (acceptThread.isAlive()) {
                acceptThread.wakeupSelector();
            } else {
                acceptThread.closeSelector();
            }
        }
        if (expirerThread != null) {
            expirerThread.interrupt();
        }
        for (SelectorThread thread : selectorThreads) {
            if (thread.isAlive()) {
                thread.wakeupSelector();
            } else {
                thread.closeSelector();
            }
        }
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        try {
            // close listen socket and signal selector threads to stop
            stop();

            // wait for selector and worker threads to shutdown
            join();

            // close all open connections
            closeAll();

            if (login != null) {
                login.shutdown();
            }
        } catch (InterruptedException e) {
            LOG.warn("Ignoring interrupted exception during shutdown", e);
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    public void addSession(long sessionId, NIOServerCnxn cnxn) {
        sessionMap.put(sessionId, cnxn);
    }

    @Override
    public boolean closeSession(long sessionId) {
        NIOServerCnxn cnxn = sessionMap.remove(sessionId);
        if (cnxn != null) {
            cnxn.close();
            return true;
        }
        return false;
    }

    @Override
    public void join() throws InterruptedException {
        if (acceptThread != null) {
            acceptThread.join();
        }
        for (SelectorThread thread : selectorThreads) {
            thread.join();
        }
        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return cnxns;
    }

    public void dumpConnections(PrintWriter pwriter) {
        pwriter.print("Connections ");
        cnxnExpiryQueue.dump(pwriter);
    }

    @Override
    public void resetAllConnectionStats() {
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            c.resetStats();
        }
    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        HashSet<Map<String, Object>> info = new HashSet<Map<String, Object>>();
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            info.add(c.getConnectionInfo(brief));
        }
        return info;
    }
}
