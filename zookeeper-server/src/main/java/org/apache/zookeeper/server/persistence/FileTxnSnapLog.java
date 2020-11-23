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

package org.apache.zookeeper.server.persistence;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is a helper class
 * above the implementations
 * of txnlog and snapshot
 * classes
 */
public class FileTxnSnapLog {
    //the direcotry containing the
    //the transaction logs
    private final File dataDir;
    //the directory containing the
    //the snapshot directory
    private final File snapDir;
    private TxnLog txnLog;
    private SnapShot snapLog;
    private final boolean trustEmptySnapshot;
    public final static int VERSION = 2;
    public final static String version = "version-";

    private static final Logger LOG = LoggerFactory.getLogger(FileTxnSnapLog.class);

    public static final String ZOOKEEPER_DATADIR_AUTOCREATE =
            "zookeeper.datadir.autocreate";

    public static final String ZOOKEEPER_DATADIR_AUTOCREATE_DEFAULT = "true";

    public static final String ZOOKEEPER_SNAPSHOT_TRUST_EMPTY = "zookeeper.snapshot.trust.empty";

    private static final String EMPTY_SNAPSHOT_WARNING = "No snapshot found, but there are log entries. ";

    /**
     * 该侦听器有助于外部api调用恢复以在收集数据时收集信息。
     */
    public interface PlayBackListener {
        void onTxnLoaded(TxnHeader hdr, Record rec);
    }

    /**
     * the constructor which takes the datadir and
     * snapdir.
     *
     * @param dataDir the transaction directory
     * @param snapDir the snapshot directory
     */
    public FileTxnSnapLog(File dataDir, File snapDir) throws IOException {
        LOG.debug("Opening datadir:{} snapDir:{}", dataDir, snapDir);

        this.dataDir = new File(dataDir, version + VERSION);
        this.snapDir = new File(snapDir, version + VERSION);

        // 启用自动创建。默认情况下，创建快照/日志目录，否则会抱怨有关更多详细信息，请参见ZOOKEEPER-1161
        boolean enableAutocreate = Boolean.valueOf(
                System.getProperty(ZOOKEEPER_DATADIR_AUTOCREATE,
                        ZOOKEEPER_DATADIR_AUTOCREATE_DEFAULT));

        // 信任空快照
        trustEmptySnapshot = Boolean.getBoolean(ZOOKEEPER_SNAPSHOT_TRUST_EMPTY);
        LOG.info(ZOOKEEPER_SNAPSHOT_TRUST_EMPTY + " : " + trustEmptySnapshot);

        if (!this.dataDir.exists()) {
            // 不存在，并且未启用自动创建 报错
            if (!enableAutocreate) {
                throw new DatadirException("Missing data directory "
                        + this.dataDir
                        + ", automatic data directory creation is disabled ("
                        + ZOOKEEPER_DATADIR_AUTOCREATE
                        + " is false). Please create this directory manually.");
            }

            if (!this.dataDir.mkdirs()) {
                throw new DatadirException("Unable to create data directory "
                        + this.dataDir);
            }
        }
        if (!this.dataDir.canWrite()) {
            throw new DatadirException("Cannot write to data directory " + this.dataDir);
        }

        if (!this.snapDir.exists()) {
            // by default create this directory, but otherwise complain instead
            // See ZOOKEEPER-1161 for more details
            if (!enableAutocreate) {
                throw new DatadirException("Missing snap directory "
                        + this.snapDir
                        + ", automatic data directory creation is disabled ("
                        + ZOOKEEPER_DATADIR_AUTOCREATE
                        + " is false). Please create this directory manually.");
            }

            if (!this.snapDir.mkdirs()) {
                throw new DatadirException("Unable to create snap directory "
                        + this.snapDir);
            }
        }
        if (!this.snapDir.canWrite()) {
            throw new DatadirException("Cannot write to snap directory " + this.snapDir);
        }

        // 检查目录下是否存在 快照/日志 文件，不存在时将会报错
        if (!this.dataDir.getPath().equals(this.snapDir.getPath())) {
            checkLogDir();
            checkSnapDir();
        }

        // 将 快照/日志 初始化
        txnLog = new FileTxnLog(this.dataDir);
        snapLog = new FileSnap(this.snapDir);
    }

    public void setServerStats(ServerStats serverStats) {
        txnLog.setServerStats(serverStats);
    }

    private void checkLogDir() throws LogDirContentCheckException {
        // listFiles 过滤目录下的文件
        File[] files = this.dataDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                // 仅接受文件名以 ”snapshot“ 前缀的快照文件
                return Util.isSnapshotFileName(name);
            }
        });
        if (files != null && files.length > 0) {
            throw new LogDirContentCheckException("Log directory has snapshot files. Check if dataLogDir and dataDir configuration is correct.");
        }
    }

    private void checkSnapDir() throws SnapDirContentCheckException {
        File[] files = this.snapDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                // 仅接受文件名以 ”log“ 前缀的 日志文件
                return Util.isLogFileName(name);
            }
        });
        if (files != null && files.length > 0) {
            throw new SnapDirContentCheckException("Snapshot directory has log files. Check if dataLogDir and dataDir configuration is correct.");
        }
    }

    /**
     * get the datadir used by this filetxn
     * snap log
     *
     * @return the data dir
     */
    public File getDataDir() {
        return this.dataDir;
    }

    /**
     * get the snap dir used by this
     * filetxn snap log
     *
     * @return the snap dir
     */
    public File getSnapDir() {
        return this.snapDir;
    }

    /**
     * 从快照和事务日志中读取后，此功能将恢复服务器数据库
     *
     * @param dt       要还原的数据树
     * @param sessions 要恢复的会话
     * @param listener 在数据库还原上运行的回放侦听器
     * @return 恢复的最高zxid
     * @throws IOException
     */
    public long restore(DataTree dt, Map<Long, Integer> sessions,
                        PlayBackListener listener) throws IOException {
        // 反序列化
        long deserializeResult = snapLog.deserialize(dt, sessions);

        FileTxnLog txnLog = new FileTxnLog(dataDir);
        if (-1L == deserializeResult) {
            /* 这意味着我们找不到任何快照，因此我们需要初始化一个空数据库（在ZOOKEEPER-2325中报告）
             */
            if (txnLog.getLastLoggedZxid() != -1) {
                // ZOOKEEPER-3056：为从旧版Zookeeper（3.4.x，3.5.3之前）升级的用户提供一个逃生舱口。
                if (!trustEmptySnapshot) {
                    // 文件损坏
                    throw new IOException(EMPTY_SNAPSHOT_WARNING + "Something is broken!");
                } else {
                    // 仅应在升级期间允许这样做。
                    LOG.warn(EMPTY_SNAPSHOT_WARNING + "This should only be allowed during upgrading.");
                }
            }
            /* TODO: (br33d）我们应该将ConcurrentHashMap放在restore（）上，或者在Save（）上使用Map
             */
            save(dt, (ConcurrentHashMap<Long, Integer>) sessions);
            /* 由于数据库为空，因此返回zxid为零 */
            return 0;
        }
        // 快速恢复反序列化的数据
        return fastForwardFromEdits(dt, sessions, listener);
    }

    /**
     * 此功能将快速转发服务器数据库以在其中包含最新的事务。
     * 这与还原相同，但仅从事务日志读取，而不从快照还原。
     *
     * @param dt       要向其中写入事务的数据树。
     * @param sessions 要还原的会话。
     * @param listener 回放侦听器在数据库事务上运行。
     * @return 恢复的最高zxid。
     * @throws IOException
     */
    public long fastForwardFromEdits(DataTree dt, Map<Long, Integer> sessions,
                                     PlayBackListener listener) throws IOException {
        TxnIterator itr = txnLog.read(dt.lastProcessedZxid + 1);
        // 最高zxid
        long highestZxid = dt.lastProcessedZxid;
        TxnHeader hdr;
        try {
            while (true) {
                // 迭代器在初始化时指向第一个有效的txn
                hdr = itr.getHeader();
                if (hdr == null) {
                    // empty logs
                    return dt.lastProcessedZxid;
                }
                if (hdr.getZxid() < highestZxid && highestZxid != 0) {
                    LOG.error("{}(highestZxid) > {}(next log) for type {}", highestZxid, hdr.getZxid(), hdr.getType());
                } else {
                    highestZxid = hdr.getZxid();
                }
                try {
                    // 处理事务
                    processTransaction(hdr, dt, sessions, itr.getTxn());
                } catch (KeeperException.NoNodeException e) {
                    throw new IOException("Failed to process transaction type: " +
                            hdr.getType() + " error: " + e.getMessage(), e);
                }
                listener.onTxnLoaded(hdr, itr.getTxn());
                if (!itr.next())
                    break;
            }
        } finally {
            if (itr != null) {
                itr.close();
            }
        }
        return highestZxid;
    }

    /**
     * Get TxnIterator for iterating through txnlog starting at a given zxid
     *
     * @param zxid starting zxid
     * @return TxnIterator
     * @throws IOException
     */
    public TxnIterator readTxnLog(long zxid) throws IOException {
        return readTxnLog(zxid, true);
    }

    /**
     * Get TxnIterator for iterating through txnlog starting at a given zxid
     *
     * @param zxid        starting zxid
     * @param fastForward true if the iterator should be fast forwarded to point
     *                    to the txn of a given zxid, else the iterator will point to the
     *                    starting txn of a txnlog that may contain txn of a given zxid
     * @return TxnIterator
     * @throws IOException
     */
    public TxnIterator readTxnLog(long zxid, boolean fastForward)
            throws IOException {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.read(zxid, fastForward);
    }

    /**
     * 在 datatree上处理事务
     *
     * @param hdr      交易的hdr
     * @param dt        将交易应用到的datatree
     * @param sessions 要恢复的会话
     * @param txn      要应用的交易
     */
    public void processTransaction(TxnHeader hdr, DataTree dt,
                                   Map<Long, Integer> sessions, Record txn)
            throws KeeperException.NoNodeException {
        ProcessTxnResult rc;
        switch (hdr.getType()) {
            case OpCode.createSession:
                sessions.put(hdr.getClientId(),
                        ((CreateSessionTxn) txn).getTimeOut());
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                            "playLog --- create session in log: 0x"
                                    + Long.toHexString(hdr.getClientId())
                                    + " with timeout: "
                                    + ((CreateSessionTxn) txn).getTimeOut());
                }
                // 给dataTree一个同步其lastProcessedZxid的机会
                rc = dt.processTxn(hdr, txn);
                break;
            case OpCode.closeSession:
                sessions.remove(hdr.getClientId());
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                            "playLog --- close session in log: 0x"
                                    + Long.toHexString(hdr.getClientId()));
                }
                rc = dt.processTxn(hdr, txn);
                break;
            default:
                rc = dt.processTxn(hdr, txn);
        }

        /**
         * 快照是延迟创建的。因此，在进行快照时，以后的交易有机会进入快照。然后，当恢复快照时，可能会发生NONODE/NODEEXISTS 错误。忽略这些应该是安全的。
         */
        if (rc.err != Code.OK.intValue()) {
            LOG.debug(
                    "Ignoring processTxn failure hdr: {}, error: {}, path: {}",
                    hdr.getType(), rc.err, rc.path);
        }
    }

    /**
     * the last logged zxid on the transaction logs
     *
     * @return the last logged zxid
     */
    public long getLastLoggedZxid() {
        FileTxnLog txnLog = new FileTxnLog(dataDir);
        return txnLog.getLastLoggedZxid();
    }

    /**
     * save the datatree and the sessions into a snapshot
     *
     * @param dataTree             the datatree to be serialized onto disk
     * @param sessionsWithTimeouts the session timeouts to be
     *                             serialized onto disk
     * @throws IOException
     */
    public void save(DataTree dataTree,
                     ConcurrentHashMap<Long, Integer> sessionsWithTimeouts)
            throws IOException {
        long lastZxid = dataTree.lastProcessedZxid;
        File snapshotFile = new File(snapDir, Util.makeSnapshotName(lastZxid));
        LOG.info("Snapshotting: 0x{} to {}", Long.toHexString(lastZxid),
                snapshotFile);
        snapLog.serialize(dataTree, sessionsWithTimeouts, snapshotFile);

    }

    /**
     * truncate the transaction logs the zxid
     * specified
     *
     * @param zxid the zxid to truncate the logs to
     * @return true if able to truncate the log, false if not
     * @throws IOException
     */
    public boolean truncateLog(long zxid) throws IOException {
        // close the existing txnLog and snapLog
        close();

        // truncate it
        FileTxnLog truncLog = new FileTxnLog(dataDir);
        boolean truncated = truncLog.truncate(zxid);
        truncLog.close();

        // re-open the txnLog and snapLog
        // I'd rather just close/reopen this object itself, however that 
        // would have a big impact outside ZKDatabase as there are other
        // objects holding a reference to this object.
        txnLog = new FileTxnLog(dataDir);
        snapLog = new FileSnap(snapDir);

        return truncated;
    }

    /**
     * the most recent snapshot in the snapshot
     * directory
     *
     * @return the file that contains the most
     * recent snapshot
     * @throws IOException
     */
    public File findMostRecentSnapshot() throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findMostRecentSnapshot();
    }

    /**
     * the n most recent snapshots
     *
     * @param n the number of recent snapshots
     * @return the list of n most recent snapshots, with
     * the most recent in front
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        FileSnap snaplog = new FileSnap(snapDir);
        return snaplog.findNRecentSnapshots(n);
    }

    /**
     * 拿到这可能包含交易大于给定zxid新的快照日志。 这包括开始zxid大于给定zxid，以及最新的事务日志开始zxid小于给定zxid日志。 后者的日志文件可能包含超出给定zxid交易
     *
     * @param zxid the zxid that contains logs greater than
     *             zxid
     * @return
     */
    public File[] getSnapshotLogs(long zxid) {
        return FileTxnLog.getLogFiles(dataDir.listFiles(), zxid);
    }

    /**
     * append the request to the transaction logs
     *
     * @param si the request to be appended
     *           returns true iff something appended, otw false
     * @throws IOException
     */
    public boolean append(Request si) throws IOException {
        return txnLog.append(si.getHdr(), si.getTxn());
    }

    /**
     * 提交日志事务
     *
     * @throws IOException
     */
    public void commit() throws IOException {
        txnLog.commit();
    }

    /**
     * @return elapsed sync time of transaction log commit in milliseconds
     */
    public long getTxnLogElapsedSyncTime() {
        return txnLog.getTxnLogSyncElapsedTime();
    }

    /**
     * roll the transaction logs
     *
     * @throws IOException
     */
    public void rollLog() throws IOException {
        txnLog.rollLog();
    }

    /**
     * close the transaction log files
     *
     * @throws IOException
     */
    public void close() throws IOException {
        txnLog.close();
        snapLog.close();
    }

    @SuppressWarnings("serial")
    public static class DatadirException extends IOException {
        public DatadirException(String msg) {
            super(msg);
        }

        public DatadirException(String msg, Exception e) {
            super(msg, e);
        }
    }

    @SuppressWarnings("serial")
    public static class LogDirContentCheckException extends DatadirException {
        public LogDirContentCheckException(String msg) {
            super(msg);
        }
    }

    @SuppressWarnings("serial")
    public static class SnapDirContentCheckException extends DatadirException {
        public SnapDirContentCheckException(String msg) {
            super(msg);
        }
    }
}
