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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * this class is used to clean up the 
 * snapshot and data log dir's. This is usually
 * run as a cronjob on the zookeeper server machine.
 * Invocation of this class will clean up the datalogdir
 * files and snapdir files keeping the last "-n" snapshot files
 * and the corresponding logs.
 */
@InterfaceAudience.Public
public class PurgeTxnLog {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeTxnLog.class);

    private static final String COUNT_ERR_MSG = "count should be greater than or equal to 3";

    static void printUsage(){
        System.out.println("Usage:");
        System.out.println("PurgeTxnLog dataLogDir [snapDir] -n count");
        System.out.println("\tdataLogDir -- path to the txn log directory");
        System.out.println("\tsnapDir -- path to the snapshot directory");
        System.out.println("\tcount -- the number of old snaps/logs you want " +
            "to keep, value should be greater than or equal to 3");
    }

    private static final String PREFIX_SNAPSHOT = "snapshot";
    private static final String PREFIX_LOG = "log";

    /**
     * Purges the snapshot and logs keeping the last num snapshots and the
     * corresponding logs. If logs are rolling or a new snapshot is created
     * during this process, these newest N snapshots or any data logs will be
     * excluded from current purging cycle.
     *
     * @param dataDir the dir that has the logs
     * @param snapDir the dir that has the snapshots
     * @param num the number of snapshots to keep
     * @throws IOException
     */
    public static void purge(File dataDir, File snapDir, int num) throws IOException {
        if (num < 3) {
            throw new IllegalArgumentException(COUNT_ERR_MSG);
        }

        // 初始化 快照/日志 目录
        FileTxnSnapLog txnLog = new FileTxnSnapLog(dataDir, snapDir);

        // 查找N个“最近”的快照，N由zoo.cfg SnapRetainCount 配置
        List<File> snaps = txnLog.findNRecentSnapshots(num);
        int numSnaps = snaps.size();
        if (numSnaps > 0) {
            // 清除旧快照
            purgeOlderSnapshots(txnLog, snaps.get(numSnaps - 1));
        }
    }

    // VisibleForTesting
    static void purgeOlderSnapshots(FileTxnSnapLog txnLog, File snapShot) {
        final long leastZxidToBeRetain = Util.getZxidFromName(
                snapShot.getName(), PREFIX_SNAPSHOT);

        /**
         * 我们删除名称中带有zxid且小于minimumZxidToBeRetain的所有文件。
         * 此规则同时适用于快照文件和日志文件，但以下日志文件例外。
         *
         * zxid小于X的日志文件可能包含zxid大于X的事务。更确切地说，名为log。（Xa）的日志文件可能包含比快照更新的事务。
         * 如果没有其他以zxid开头的日志文件间隔（Xa，X]。
         * 假设后一个条件为真，则必须保留log。（Xa）以确保快照.Xa是可恢复的。实际上，此日志文件很可能会扩展到快照以上。
         * 如果这些较新的快照未附带日志翻转（在撰写本文时，该文件可能位于学习者状态机中），则快照文件。我们可以更精确地确定是否为最小'a的日志。
         * （leastZxidToBeRetain-a）实际上是是否需要（例如，如果有一个名为log。（leastZxidToBeRetain + 1）的日志文件，则不需要），
         * 但是仅在不常见的情况下，复杂度很快就会增加。仅保存日志是安全且简单的。
         * （leastZxidToBeRetain-a）表示最小的“ a”，以确保所有快照的可恢复性被保留。我们通过调用txnLog.getSnapshotLogs（）来确定该日志文件。
         */
        final Set<File> retainedTxnLogs = new HashSet<File>();
        // 简单来说，需要保留XA事务相关的快照，zookeeper是2PC的事务，通过zxid实现。
        retainedTxnLogs.addAll(Arrays.asList(txnLog.getSnapshotLogs(leastZxidToBeRetain)));

        /**
         * 查找所有要删除的候选文件，即名称中zxid小于minimumZxidToBeRetain的文件。如上所述，该规则有一个例外。
         */
        class MyFileFilter implements FileFilter{
            private final String prefix;
            MyFileFilter(String prefix){
                this.prefix=prefix;
            }
            public boolean accept(File f){
                if(!f.getName().startsWith(prefix + "."))
                    return false;
                if (retainedTxnLogs.contains(f)) {
                    return false;
                }
                long fZxid = Util.getZxidFromName(f.getName(), prefix);
                if (fZxid >= leastZxidToBeRetain) {
                    return false;
                }
                return true;
            }
        }
        // 添加所有非排除的日志文件
        File[] logs = txnLog.getDataDir().listFiles(new MyFileFilter(PREFIX_LOG));
        List<File> files = new ArrayList<>();
        if (logs != null) {
            files.addAll(Arrays.asList(logs));
        }

        // 将所有未排除的快照文件添加到删除列表中
        File[] snapshots = txnLog.getSnapDir().listFiles(new MyFileFilter(PREFIX_SNAPSHOT));
        if (snapshots != null) {
            files.addAll(Arrays.asList(snapshots));
        }

        // 删除旧文件
        for(File f: files)
        {
            final String msg = "Removing file: "+
                DateFormat.getDateTimeInstance().format(f.lastModified())+
                "\t"+f.getPath();
            LOG.info(msg);
            System.out.println(msg);
            if(!f.delete()){
                System.err.println("Failed to remove "+f.getPath());
            }
        }

    }
    
    /**
     * @param args dataLogDir [snapDir] -n count
     * dataLogDir -- path to the txn log directory
     * snapDir -- path to the snapshot directory
     * count -- the number of old snaps/logs you want to keep, value should be greater than or equal to 3<br>
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 3 || args.length > 4) {
            printUsageThenExit();
        }
        File dataDir = validateAndGetFile(args[0]);
        File snapDir = dataDir;
        int num = -1;
        String countOption = "";
        if (args.length == 3) {
            countOption = args[1];
            num = validateAndGetCount(args[2]);
        } else {
            snapDir = validateAndGetFile(args[1]);
            countOption = args[2];
            num = validateAndGetCount(args[3]);
        }
        if (!"-n".equals(countOption)) {
            printUsageThenExit();
        }
        purge(dataDir, snapDir, num);
    }

    /**
     * validates file existence and returns the file
     *
     * @param path
     * @return File
     */
    private static File validateAndGetFile(String path) {
        File file = new File(path);
        if (!file.exists()) {
            System.err.println("Path '" + file.getAbsolutePath()
                    + "' does not exist. ");
            printUsageThenExit();
        }
        return file;
    }

    /**
     * Returns integer if parsed successfully and it is valid otherwise prints
     * error and usage and then exits
     *
     * @param number
     * @return count
     */
    private static int validateAndGetCount(String number) {
        int result = 0;
        try {
            result = Integer.parseInt(number);
            if (result < 3) {
                System.err.println(COUNT_ERR_MSG);
                printUsageThenExit();
            }
        } catch (NumberFormatException e) {
            System.err
                    .println("'" + number + "' can not be parsed to integer.");
            printUsageThenExit();
        }
        return result;
    }

    private static void printUsageThenExit() {
        printUsage();
        System.exit(1);
    }
}
