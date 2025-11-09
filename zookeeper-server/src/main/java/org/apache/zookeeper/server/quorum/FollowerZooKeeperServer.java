/*
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

package org.apache.zookeeper.server.quorum;

import org.apache.jute.Record;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.metrics.MetricsContext;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Just like the standard ZooKeeperServer. We just replace the request
 * processors: FollowerRequestProcessor -&gt; CommitProcessor -&gt;
 * FinalRequestProcessor
 *
 * A SyncRequestProcessor is also spawned off to log proposals from the leader.
 */
public class FollowerZooKeeperServer extends LearnerZooKeeperServer {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerZooKeeperServer.class);

    /*
     * Pending sync requests
     */ ConcurrentLinkedQueue<Request> pendingSyncs;

    /**
     * @throws IOException
     */
    FollowerZooKeeperServer(FileTxnSnapLog logFactory, QuorumPeer self, ZKDatabase zkDb) throws IOException {
        super(logFactory, self.tickTime, self.minSessionTimeout, self.maxSessionTimeout, self.clientPortListenBacklog, zkDb, self);
        this.pendingSyncs = new ConcurrentLinkedQueue<Request>();
    }

    public Follower getFollower() {
        return self.follower;
    }

    @Override
    protected void setupRequestProcessors() {
        /*
         *    FollowerRequestProcessor
         * -> CommitProcessor             队列 + 阻塞住 + 等待 Leader 发送 commit 进入 this.commit() 唤醒 + 拼凑协议 + next
         * -> FinalRequestProcessor       创建数据节点, 将数据写到内存节点树 (NodeHashMap) 中
         *
         *    SyncRequestProcessor        队列 + 数据写入本地事务日志文件 + next
         * -> SendAckRequestProcessor     回复 Leader ack 给自己记一票
         */
        RequestProcessor finalProcessor = new FinalRequestProcessor(this);
        commitProcessor = new CommitProcessor(finalProcessor, Long.toString(getServerId()), true, getZooKeeperServerListener());
        commitProcessor.start(); // 阻塞住
        firstProcessor = new FollowerRequestProcessor(this, commitProcessor);
        ((FollowerRequestProcessor) firstProcessor).start();
        syncProcessor = new SyncRequestProcessor(this, new SendAckRequestProcessor(getFollower()));
        syncProcessor.start();
    }

    /**
     * 待 commit 的 zxid
     */
    LinkedBlockingQueue<Request> pendingTxns = new LinkedBlockingQueue<Request>();

    public void logRequest(TxnHeader hdr, Record txn, TxnDigest digest) {
        Request request = new Request(hdr.getClientId(), hdr.getCxid(), hdr.getType(), hdr, txn, hdr.getZxid());
        request.setTxnDigest(digest);
        if ((request.zxid & 0xffffffffL) != 0) {
            pendingTxns.add(request); // 加入队列, 等待 Leader 发送 commit
        }
        syncProcessor.processRequest(request); // 责任链
        // 将 Leader 提议的数据写入本地事务日志文件 + 回复 Leader ack 给自己记一票
        // Leader 收到 Follower 集群返回过半的 ack 后, 会再次发起 commit 请求给 Follower, 会进入 this.commit() 进行处理
    }

    /**
     * When a COMMIT message is received, eventually this method is called,
     * which matches up the zxid from the COMMIT with (hopefully) the head of
     * the pendingTxns queue and hands it to the commitProcessor to commit.
     * @param zxid - must correspond to the head of pendingTxns if it exists
     */
    public void commit(long zxid) {
        if (pendingTxns.size() == 0) {
            LOG.warn("Committing " + Long.toHexString(zxid) + " without seeing txn");
            return;
        }
        long firstElementZxid = pendingTxns.element().zxid;
        if (firstElementZxid != zxid) {
            // 数据一致性异常
            LOG.error("Committing zxid 0x" + Long.toHexString(zxid)
                      + " but next pending txn 0x" + Long.toHexString(firstElementZxid));
            // 终止进程
            ServiceUtils.requestSystemExit(ExitCode.UNMATCHED_TXN_COMMIT.getValue());
        }
        Request request = pendingTxns.remove(); // 移除队列头
        request.logLatency(ServerMetrics.getMetrics().COMMIT_PROPAGATION_LATENCY);
        commitProcessor.commit(request); // 提交, 会唤醒 commitProcessor.run() 将数据写到内存节点树
    }

    public synchronized void sync() {
        if (pendingSyncs.size() == 0) {
            LOG.warn("Not expecting a sync.");
            return;
        }

        Request r = pendingSyncs.remove();
        if (r instanceof LearnerSyncRequest) {
            LearnerSyncRequest lsr = (LearnerSyncRequest) r;
            lsr.fh.queuePacket(new QuorumPacket(Leader.SYNC, 0, null, null));
        }
        commitProcessor.commit(r);
    }

    @Override
    public int getGlobalOutstandingLimit() {
        int divisor = self.getQuorumSize() > 2 ? self.getQuorumSize() - 1 : 1;
        int globalOutstandingLimit = super.getGlobalOutstandingLimit() / divisor;
        return globalOutstandingLimit;
    }

    @Override
    public String getState() {
        return "follower";
    }

    @Override
    public Learner getLearner() {
        return getFollower();
    }

    /**
     * Process a request received from external Learner through the LearnerMaster
     * These requests have already passed through validation and checks for
     * session upgrade and can be injected into the middle of the pipeline.
     *
     * @param request received from external Learner
     */
    void processObserverRequest(Request request) {
        ((FollowerRequestProcessor) firstProcessor).processRequest(request, false);
    }

    boolean registerJMX(LearnerHandlerBean handlerBean) {
        try {
            MBeanRegistry.getInstance().register(handlerBean, jmxServerBean);
            return true;
        } catch (JMException e) {
            LOG.warn("Could not register connection", e);
        }
        return false;
    }

    @Override
    protected void registerMetrics() {
        super.registerMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();

        rootContext.registerGauge("synced_observers", self::getSynced_observers_metric);

    }

    @Override
    protected void unregisterMetrics() {
        super.unregisterMetrics();

        MetricsContext rootContext = ServerMetrics.getMetrics().getMetricsProvider().getRootContext();
        rootContext.unregisterGauge("synced_observers");

    }

}
