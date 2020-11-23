/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 * 此RequestProcessor将请求记录到磁盘。
 * 它分批处理请求以有效地执行io。
 * 在将请求的日志同步到磁盘之前，该请求不会传递到下一个RequestProcessor
 *
 * SyncRequestProcessor在3种不同情况下使用
 * 1. Leader - 将请求同步到磁盘，并将其转发到AckRequestProcessor，后者将ack发送回自身。
 *
 * 2. Follower - 将请求同步到磁盘，并将请求转发到 SendAckRequestProcessor，后者将数据包发送到领导者。
 *              SendAckRequestProcessor是可刷新的，这使我们能够强制将数据包推向领导者。
 *
 * 3. Observer - 将提交的请求同步到磁盘（作为INFORM数据包接收）。
 *              永远不会将确认发送回给领导者，因此nextProcessor将为空。
 *              这改变了观察者上txnlog的语义，因为它仅包含提交的txns。
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private final ZooKeeperServer zks;
    /**
     * 请求队列
     */
    private final LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();
    /**
     * 下一个请求处理器
     */
    private final RequestProcessor nextProcessor;

    private Thread snapInProcess = null;
    volatile private boolean running;

    /**
     * 已写入并等待刷新到磁盘的事务。基本上，这是SyncItems的列表，在刷新成功返回之后，将调用它们的回调。
     */
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    private final Random r = new Random();
    /**
     * 开始快照之前要记录的日志条目数
     */
    private static int snapCount = ZooKeeperServer.getSnapCount();

    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    /**
     * 由测试用于检查快照数量的更改
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * 测试用于获取快照数量
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    @Override
    public void run() {
        try {
            int logCount = 0;

            // 我们这样做是为了确保并非集合中的所有服务器都同时拍摄快照
            int randRoll = r.nextInt(snapCount/2);
            while (true) {
                Request si = null;
                // 已写入并等待刷新到磁盘的事务.isEmpty()
                if (toFlush.isEmpty()) {
                    // take()：以阻塞的方式从队列头部取出一个元素，没有元素将一直阻塞
                    si = queuedRequests.take();
                } else {
                    // poll()：获取并移除此队列的头，或者返回null ，如果此队列为空。
                    si = queuedRequests.poll();
                    if (si == null) {
                        // 将事务写入到磁盘
                        flush(toFlush);
                        continue;
                    }
                }
                // 死亡请求
                if (si == requestOfDeath) {
                    break;
                }
                if (si != null) {
                    // 跟踪写入日志的记录数
                    if (zks.getZKDatabase().append(si)) {
                        logCount++;
                        if (logCount > (snapCount / 2 + randRoll)) {
                            randRoll = r.nextInt(snapCount/2);
                            // 滚动日志
                            zks.getZKDatabase().rollLog();
                            // 生成快照
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                // 太忙而无法捕捉，跳过
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                // 快照线程
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                        public void run() {
                                            try {
                                                zks.takeSnapshot();
                                            } catch(Exception e) {
                                                LOG.warn("Unexpected exception", e);
                                            }
                                        }
                                    };
                                snapInProcess.start();
                            }
                            logCount = 0;
                        }
                    } else if (toFlush.isEmpty()) {
                        // 读取大量工作负载的优化.
                        // 如果这是读取，并且没有挂起的刷新（写入），将其传递给下一个处理器
                        if (nextProcessor != null) {
                            // Leader的nextProcessor为AckProcessRequest
                            // Follower的nextProcessor为SendAckRequestProcessor，后者将数据包发送到领导者：LeaderRequestProcessor
                            // 根据责任链进行调用，从ZooKeeperServer.setupRequestProcessors得知，SyncRequestProcessor 的下一个处理者是 FinalRequestProcessor
                            // SyncRequestProcessor 负责将事务持久化到本地磁盘。实际上就是将事务数据按照顺序追加到事务日志中，并形成快照数据。
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                // 下一个处理器可持久化日志，则将数据刷新到磁盘
                                ((Flushable)nextProcessor).flush();
                            }
                        }
                        continue;
                    }
                    toFlush.add(si);
                    if (toFlush.size() > 1000) {
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        } finally{
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush(LinkedList<Request> toFlush)
        throws IOException, RequestProcessorException
    {
        if (toFlush.isEmpty())
            return;

        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if(running){
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
