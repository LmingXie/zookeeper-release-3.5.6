/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages cleanup of container ZNodes. This class is meant to only
 * be run from the leader. There's no harm in running from followers/observers
 * but that will be extra work that's not needed. Once started, it periodically
 * checks container nodes that have a cversion > 0 and have no children. A
 * delete is attempted on the node. The result of the delete is unimportant.
 * If the proposal fails or the container node is not empty there's no harm.
 */
public class ContainerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);
    private final ZKDatabase zkDb;
    private final RequestProcessor requestProcessor;
    private final int checkIntervalMs;
    private final int maxPerMinute;
    private final Timer timer;
    private final AtomicReference<TimerTask> task = new AtomicReference<TimerTask>(null);

    /**
     * @param zkDb the ZK database
     * @param requestProcessor request processer - used to inject delete
     *                         container requests
     * @param checkIntervalMs how often to check containers in milliseconds
     * @param maxPerMinute the max containers to delete per second - avoids
     *                     herding of container deletions
     */
    public ContainerManager(ZKDatabase zkDb, RequestProcessor requestProcessor,
                            int checkIntervalMs, int maxPerMinute) {
        this.zkDb = zkDb;
        this.requestProcessor = requestProcessor;
        this.checkIntervalMs = checkIntervalMs;
        this.maxPerMinute = maxPerMinute;
        timer = new Timer("ContainerManagerTask", true);

        LOG.info(String.format("Using checkIntervalMs=%d maxPerMinute=%d",
                checkIntervalMs, maxPerMinute));
    }

    /**
     * 启动/重新启动计时器以运行检查。可以安全地多次调用。
     */
    public void start() {
        if (task.get() == null) {
            // 创建一个定时任务
            TimerTask timerTask = new TimerTask() {
                @Override
                public void run() {
                    try {
                        // 任务的内容是：定时检查容器，删除无用节点，包括：超时的TTL节点、空的容器节点
                        checkContainers();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.info("interrupted");
                        cancel();
                    } catch ( Throwable e ) {
                        LOG.error("Error checking containers", e);
                    }
                }
            };
            if (task.compareAndSet(null, timerTask)) {
                timer.scheduleAtFixedRate(timerTask, checkIntervalMs,
                        checkIntervalMs);
            }
        }
    }

    /**
     * stop the timer if necessary. Can safely be called multiple times.
     */
    public void stop() {
        TimerTask timerTask = task.getAndSet(null);
        if (timerTask != null) {
            timerTask.cancel();
        }
        timer.cancel();
    }

    /**
     * 手动检查容器。通常不直接使用
     */
    public void checkContainers()
            throws InterruptedException {
        // 获取最小间隔时间
        long minIntervalMs = getMinIntervalMs();
        for (String containerPath : getCandidates()) {
            // 计算耗时
            long startMs = Time.currentElapsedTime();

            // 将数据写入到缓存区
            ByteBuffer path = ByteBuffer.wrap(containerPath.getBytes());

            Request request = new Request(null, 0, 0,
                    ZooDefs.OpCode.deleteContainer, path, null);
            try {
                // 尝试删除候选容器：{}
                LOG.info("Attempting to delete candidate container: {}",
                        containerPath);
                requestProcessor.processRequest(request);
            } catch (Exception e) {
                LOG.error("Could not delete container: {}",
                        containerPath, e);
            }

            long elapsedMs = Time.currentElapsedTime() - startMs;
            long waitMs = minIntervalMs - elapsedMs;
            if (waitMs > 0) {
                Thread.sleep(waitMs);
            }
        }
    }

    // VisibleForTesting
    protected long getMinIntervalMs() {
        return TimeUnit.MINUTES.toMillis(1) / maxPerMinute;
    }

    // VisibleForTesting TODO 这里在做什么事情？
    protected Collection<String> getCandidates() {
        Set<String> candidates = new HashSet<String>();
        for (String containerPath : zkDb.getDataTree().getContainers()) {
            DataNode node = zkDb.getDataTree().getNode(containerPath);
            /*
               cversion> 0：在添加任何子级之前，请勿删除新创建的容器。
               如果要在清洁容器之前创建容器，则将立即删除该容器。
             */
            // 删除空的容器节点
            if ((node != null) && (node.stat.getCversion() > 0) &&
                    (node.getChildren().size() == 0)) {
                // 添加到候选人？
                candidates.add(containerPath);
            }
        }
        for (String ttlPath : zkDb.getDataTree().getTtls()) {
            DataNode node = zkDb.getDataTree().getNode(ttlPath);
            if (node != null) {
                Set<String> children = node.getChildren();
                if ((children == null) || (children.size() == 0)) {
                    // 获取临时所有者
                    // TTL节点是在3.5.3版本增加的，TTL节点
                    if ( EphemeralType.get(node.stat.getEphemeralOwner()) == EphemeralType.TTL ) {
                        long elapsed = getElapsed(node);
                        long ttl = EphemeralType.TTL.getValue(node.stat.getEphemeralOwner());
                        // 删除过期的TTL节点 TODO System.nanoTime() 是在JVM中基于任意的一个随机时间点为基点，计算出来的相对时间，与系统时间无关，因此更适合用于计算时间差值，需要注意的是他可能会产生负数，因此不能直接进行比较。
                        if ((ttl != 0) && (getElapsed(node) > ttl)) {
                            candidates.add(ttlPath);
                        }
                    }
                }
            }
        }
        return candidates;
    }

    // VisibleForTesting
    protected long getElapsed(DataNode node) {
        return Time.currentWallTime() - node.stat.getMtime();
    }
}
