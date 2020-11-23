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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.DatadirException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.security.sasl.SaslException;
import java.io.IOException;

/**
 *
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in
 * docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "1" that contains the server id as an ASCII decimal value.
 *
 */
@InterfaceAudience.Public
public class QuorumPeerMain {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";

    protected QuorumPeer quorumPeer;

    /**
     * To start the replicated server specify the configuration file name on
     * the command line.
     * @param args path to the configfile
     */
    public static void main(String[] args) {
        QuorumPeerMain main = new QuorumPeerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            System.exit(2);
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(2);
        } catch (DatadirException e) {
            LOG.error("Unable to access datadir, exiting abnormally", e);
            System.err.println("Unable to access datadir, exiting abnormally");
            System.exit(3);
        } catch (AdminServerException e) {
            LOG.error("Unable to start AdminServer, exiting abnormally", e);
            System.err.println("Unable to start AdminServer, exiting abnormally");
            System.exit(4);
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(1);
        }
        LOG.info("Exiting normally");
        System.exit(0);
    }

    protected void initializeAndRun(String[] args)
            throws ConfigException, IOException, AdminServerException {
        QuorumPeerConfig config = new QuorumPeerConfig();
        if (args.length == 1) {
            config.parse(args[0]);
        }

        // 启动 计划清除任务
        /**
         * 由于ZK的任何变更都将产生事务，事务日志需要持久化到磁盘，同时写操作到达一定量或者一定时间间隔后，也会对内存中的数据进行一次快照，并写入磁盘的snapshop中，快照可以有效缩短启动加载时间。
         * DatadirCleanupManager 任务的左右就是定时清理DataDir中的snapshot及对应的 transaction log。
         */
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
                .getDataDir(), config.getDataLogDir(), config
                .getSnapRetainCount(), config.getPurgeInterval());
        purgeMgr.start();

        if (args.length == 1 && config.isDistributed()) {
            // 集群模式
            runFromConfig(config);
        } else {
            LOG.warn("Either no config or no quorum defined in config, running "
                    + " in standalone mode");
            // 单机运行 -- run as standalone
            ZooKeeperServerMain.main(args);
        }
    }

    public void runFromConfig(QuorumPeerConfig config)
            throws IOException, AdminServerException {
        try {
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }

        LOG.info("Starting quorum peer");
        try {
            // ServerCnxnFactory 负责管理所有ServerCnxn，一个cnxn连接代表一个会话Socket（服务端-客户端）。
            ServerCnxnFactory cnxnFactory = null;
            ServerCnxnFactory secureCnxnFactory = null;

            // 从配置文件获取客户端 端口地址
            if (config.getClientPortAddress() != null) {
                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(config.getClientPortAddress(),
                        config.getMaxClientCnxns(),
                        false);
            }

            // 获取安全客户端端口地址
            if (config.getSecureClientPortAddress() != null) {
                secureCnxnFactory = ServerCnxnFactory.createFactory();
                secureCnxnFactory.configure(config.getSecureClientPortAddress(),
                        config.getMaxClientCnxns(),
                        true);
            }

            // 创建一个新的对等节点
            quorumPeer = getQuorumPeer();
            // 日志和快照
            quorumPeer.setTxnFactory(new FileTxnSnapLog(
                    config.getDataLogDir(),
                    config.getDataDir()));
            // 启用本地会话
            quorumPeer.enableLocalSessions(config.areLocalSessionsEnabled());
            // 启用本地会话升级
            quorumPeer.enableLocalSessionsUpgrading(
                    config.isLocalSessionsUpgradingEnabled());
            // 获取所有成员节点
            //quorumPeer.setQuorumPeers(config.getAllMembers());
            // 设置选举类型
            quorumPeer.setElectionType(config.getElectionAlg());
            // 设置MyId
            quorumPeer.setMyId(config.getServerId());
            // 设定时间
            quorumPeer.setTickTime(config.getTickTime());
            // 设置最小会话超时
            quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
            // 设置最大会话超时
            quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
            // 设置初始限制
            quorumPeer.setInitLimit(config.getInitLimit());
            // 设置同步限制
            quorumPeer.setSyncLimit(config.getSyncLimit());
            // 设置配置文件名
            quorumPeer.setConfigFileName(config.getConfigFilename());
            // 设置ZK数据库
            quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
            // 设置验证者 TODO 默认以当前节点作为 最后的验证节点，验证节点验证选举结果
            quorumPeer.setQuorumVerifier(config.getQuorumVerifier(), false);
            // 获取最后一次仲裁验证者（上一位Leader）
            if (config.getLastSeenQuorumVerifier() != null) {
                // 设置最后看到的法定人数验证者，如果config中有配置默认 验证节点的话
                quorumPeer.setLastSeenQuorumVerifier(config.getLastSeenQuorumVerifier(), false);
            }
            // 根据配置初始化ZK数据库
            quorumPeer.initConfigInZKDatabase();
            // 设置CNXN工厂
            quorumPeer.setCnxnFactory(cnxnFactory);
            // 设置安全cnxn工厂
            quorumPeer.setSecureCnxnFactory(secureCnxnFactory);
            // ssl证书
            quorumPeer.setSslQuorum(config.isSslQuorum());
            // 设置使用端口统一
            quorumPeer.setUsePortUnification(config.shouldUsePortUnification());
            // 设置学习者类型
            quorumPeer.setLearnerType(config.getPeerType());
            // 设置启用同步
            quorumPeer.setSyncEnabled(config.getSyncEnabled());
            // 设置仲裁听所有I Ps
            quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());
            if (config.sslQuorumReloadCertFiles) {
                quorumPeer.getX509Util().enableCertFileReloading();
            }

            // 设置仲裁sasl身份验证配置
            quorumPeer.setQuorumSaslEnabled(config.quorumEnableSasl);
            if (quorumPeer.isQuorumSaslAuthEnabled()) {
                quorumPeer.setQuorumServerSaslRequired(config.quorumServerRequireSasl);
                quorumPeer.setQuorumLearnerSaslRequired(config.quorumLearnerRequireSasl);
                quorumPeer.setQuorumServicePrincipal(config.quorumServicePrincipal);
                quorumPeer.setQuorumServerLoginContext(config.quorumServerLoginContext);
                quorumPeer.setQuorumLearnerLoginContext(config.quorumLearnerLoginContext);
            }
            // 设置法定Cnxn线程大小
            quorumPeer.setQuorumCnxnThreadsSize(config.quorumCnxnThreadsSize);
            // 初始化
            quorumPeer.initialize();

            // 启动
            quorumPeer.start();
            // 建立连接
            quorumPeer.join();
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Quorum Peer interrupted", e);
        }
    }

    // @VisibleForTesting
    protected QuorumPeer getQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }
}
