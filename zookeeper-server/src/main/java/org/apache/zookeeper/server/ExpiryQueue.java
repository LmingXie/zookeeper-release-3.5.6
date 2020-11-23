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

import org.apache.zookeeper.common.Time;

import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  ExpiryQueue跟踪按时间排序的固定持续时间段中的元素。  SessionTrackerImpl使用它来终止会话，而NIOServerCnxnFactory使用来终止连接。
 */
public class ExpiryQueue<E> {
    /**
     * 记录Session在哪个过期时间段
     */
    private final ConcurrentHashMap<E, Long> elemMap =
            new ConcurrentHashMap<E, Long>();
    /**
     * The maximum number of buckets is equal to max timeout/expirationInterval,
     * so the expirationInterval should not be too small compared to the
     * max timeout that this expiry queue needs to maintain.
     */
    private final ConcurrentHashMap<Long, Set<E>> expiryMap =
            new ConcurrentHashMap<Long, Set<E>>();

    private final AtomicLong nextExpirationTime = new AtomicLong();
    private final int expirationInterval;

    public ExpiryQueue(int expirationInterval) {
        this.expirationInterval = expirationInterval;
        // 首次，在构造中计算出 下一次淘汰时间
        nextExpirationTime.set(roundToNextInterval(Time.currentElapsedTime()));
    }

    private long roundToNextInterval(long time) {
        return (time / expirationInterval + 1) * expirationInterval;
    }

    /**
     * Removes element from the queue.
     *
     * @param elem element to remove
     * @return time at which the element was set to expire, or null if
     * it wasn't present
     */
    public Long remove(E elem) {
        Long expiryTime = elemMap.remove(elem);
        if (expiryTime != null) {
            Set<E> set = expiryMap.get(expiryTime);
            if (set != null) {
                set.remove(elem);
                // We don't need to worry about removing empty sets,
                // they'll eventually be removed when they expire.
            }
        }
        return expiryTime;
    }

    /**
     * 添加或更新队列中元素的到期时间，将舍入为该队列使用的存储桶的到期时间间隔。
     *
     * @param elem    element to add/update
     * @param timeout timout in milliseconds
     * @return 如果更改，则元素现在设置为过期的时间；如果未更改，则设置为null的时间
     */
    public Long update(E elem, int timeout) {
        Long prevExpiryTime = elemMap.get(elem);
        long now = Time.currentElapsedTime();
        // 计算当前Session在哪个过期时间内
        Long newExpiryTime = roundToNextInterval(now + timeout);

        // 过期时间无变化，则不进行更新
        if (newExpiryTime.equals(prevExpiryTime)) {
            // 没有变化，所以没有更新
            return null;
        }

        // 首先，将元素添加到expiryMap中的新到期时间段。
        // 获取新的过期时间桶
        Set<E> set = expiryMap.get(newExpiryTime);
        if (set == null) {
            // Set默认使用HashMap实现，但也提供了newSetFromMap，用以指定所使用的Mao集合实例
            // 这里，使用ConcurrentHashMap构造一个线程安全的ConcurrentHashSet
            set = Collections.newSetFromMap(
                    new ConcurrentHashMap<E, Boolean>());
            // 将新集合放到地图中，但前提是另一个线程没有击败我们
            Set<E> existingSet = expiryMap.putIfAbsent(newExpiryTime, set);
            if (existingSet != null) {
                // put 失败，使用已存在的 桶
                set = existingSet;
            }
        }
        // 放入新桶中
        set.add(elem);

        // 将elem映射到新的到期时间。如果存在另一个以前的映射，请清理以前的过期存储桶。
        prevExpiryTime = elemMap.put(elem, newExpiryTime);
        if (prevExpiryTime != null && !newExpiryTime.equals(prevExpiryTime)) {
            // 获取之前的过期时间桶
            Set<E> prevSet = expiryMap.get(prevExpiryTime);
            if (prevSet != null) {
                // 将当前元素从旧桶中移除 创建
                prevSet.remove(elem);
            }
        }
        return newExpiryTime;
    }

    /**
     * @return milliseconds until next expiration time, or 0 if has already past
     */
    public long getWaitTime() {
        // 获取纳秒相对时间
        long now = Time.currentElapsedTime();
        // 下一个到期时间，默认3000ms检查一次，创建会话时 将会根据纳秒计算出相对的 到期时间。
        // 在 nextExpirationTime.set(roundToNextInterval(Time.currentElapsedTime())); 行
        long expirationTime = nextExpirationTime.get();
        return now < expirationTime ? (expirationTime - now) : 0L;
    }

    /**
     * 从expireMap中删除下一组过期的元素。
     * 需要通过检查getWaitTime（）来足够频繁地调用此方法，否则将会在expiryMap中排队等待空集。
     *
     * @return 下一组过期的元素；如果尚未准备好，则为空集
     */
    public Set<E> poll() {
        long now = Time.currentElapsedTime();
        long expirationTime = nextExpirationTime.get();
        // 未超过下一次过期时间 == 未过期
        if (now < expirationTime) {
            return Collections.emptySet();
        }

        Set<E> set = null;
        // 计算下一次过期时间
        long newExpirationTime = expirationTime + expirationInterval;
        // 原子操作 -> 更新下一次过期时间
        if (nextExpirationTime.compareAndSet(
                expirationTime, newExpirationTime)) {
            // 移除当前过期时间”桶“
            set = expiryMap.remove(expirationTime);
        }
        if (set == null) {
            return Collections.emptySet();
        }
        return set;
    }

    public void dump(PrintWriter pwriter) {
        pwriter.print("Sets (");
        pwriter.print(expiryMap.size());
        pwriter.print(")/(");
        pwriter.print(elemMap.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(expiryMap.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            Set<E> set = expiryMap.get(time);
            if (set != null) {
                pwriter.print(set.size());
                pwriter.print(" expire at ");
                pwriter.print(Time.elapsedTimeToDate(time));
                pwriter.println(":");
                for (E elem : set) {
                    pwriter.print("\t");
                    pwriter.println(elem.toString());
                }
            }
        }
    }

    /**
     * Returns an unmodifiable view of the expiration time -> elements mapping.
     */
    public Map<Long, Set<E>> getExpiryMap() {
        return Collections.unmodifiableMap(expiryMap);
    }
}

