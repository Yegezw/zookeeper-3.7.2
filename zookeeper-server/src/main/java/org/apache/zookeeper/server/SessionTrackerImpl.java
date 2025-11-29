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

package org.apache.zookeeper.server;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

// 负责创建 Session 和维护 Session 的过期时间
/**
 * This is a full featured SessionTracker. It tracks session in grouped by tick
 * interval. It always rounds up the tick interval to provide a sort of grace
 * period. Sessions are thus expired in batches made up of sessions that expire
 * in a given interval.
 */
public class SessionTrackerImpl extends ZooKeeperCriticalThread implements SessionTracker {

    private static final Logger LOG = LoggerFactory.getLogger(SessionTrackerImpl.class);

    /**
     * SessionId -> Session
     */
    protected final ConcurrentHashMap<Long, SessionImpl> sessionsById = new ConcurrentHashMap<Long, SessionImpl>();

    /**
     * 过期队列: 用于维护会话的过期, 并且使用 bucket 来维护会话, 每个 bucket 对应某时间范围内过期的会话
     */
    private final ExpiryQueue<SessionImpl> sessionExpiryQueue;

    protected final ConcurrentMap<Long, Integer> sessionsWithTimeout;
    private final AtomicLong nextSessionId = new AtomicLong();

    public static class SessionImpl implements Session {

        SessionImpl(long sessionId, int timeout) {
            this.sessionId = sessionId;
            this.timeout = timeout;
            isClosing = false;
        }

        final long sessionId;
        final int timeout;
        boolean isClosing;

        Object owner;

        public long getSessionId() {
            return sessionId;
        }
        public int getTimeout() {
            return timeout;
        }
        public boolean isClosing() {
            return isClosing;
        }

        public String toString() {
            return "0x" + Long.toHexString(sessionId);
        }

    }

    /**
     * Generates an initial sessionId.
     *
     * <p>High order 1 byte is serverId, next 5 bytes are from timestamp, and low order 2 bytes are 0s.
     * Use {@literal ">>> 8"}, not {@literal ">> 8"} to make sure that the high order 1 byte is entirely
     * up to the server Id.
     *
     * <p>See also http://jira.apache.org/jira/browse/ZOOKEEPER-1622
     *
     * @param id server Id
     * @return the session Id
     */
    public static long initializeNextSessionId(long id) {
        /*
         * 参数 id 为 sid
         * 高 8 位确定了所在机器, 建议值为 [1 ... 127]
         * 中间 40 位使用当前时间戳 (毫秒), 保证单机环境唯一性
         * 最后低 16 位都是 0 可用于并发自增, 1 ms 最多 65535, 6553.5W qps/s
         */
        long nextSid;
        nextSid = (Time.currentElapsedTime() << 24) >>> 8;
        nextSid = nextSid | (id << 56);
        if (nextSid == EphemeralType.CONTAINER_EPHEMERAL_OWNER) {
            ++nextSid;  // this is an unlikely edge case, but check it just in case
        }
        return nextSid;
    }

    private final SessionExpirer expirer;

    public SessionTrackerImpl(SessionExpirer expirer, ConcurrentMap<Long, Integer> sessionsWithTimeout, int tickTime, long serverId, ZooKeeperServerListener listener) {
        super("SessionTracker", listener);
        this.expirer = expirer;
        this.sessionExpiryQueue = new ExpiryQueue<SessionImpl>(tickTime);
        this.sessionsWithTimeout = sessionsWithTimeout;
        this.nextSessionId.set(initializeNextSessionId(serverId));
        for (Entry<Long, Integer> e : sessionsWithTimeout.entrySet()) {
            trackSession(e.getKey(), e.getValue());
        }

        EphemeralType.validateServerId(serverId);
    }

    volatile boolean running = true;

    public void dumpSessions(PrintWriter pwriter) {
        pwriter.print("Session ");
        sessionExpiryQueue.dump(pwriter);
    }

    /**
     * Returns a mapping from time to session IDs of sessions expiring at that time.
     */
    public synchronized Map<Long, Set<Long>> getSessionExpiryMap() {
        // Convert time -> sessions map to time -> session IDs map
        Map<Long, Set<SessionImpl>> expiryMap = sessionExpiryQueue.getExpiryMap();
        Map<Long, Set<Long>> sessionExpiryMap = new TreeMap<Long, Set<Long>>();
        for (Entry<Long, Set<SessionImpl>> e : expiryMap.entrySet()) {
            Set<Long> ids = new HashSet<Long>();
            sessionExpiryMap.put(e.getKey(), ids);
            for (SessionImpl s : e.getValue()) {
                ids.add(s.sessionId);
            }
        }
        return sessionExpiryMap;
    }

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpSessions(pwriter);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    @Override
    public void run() {
        try {
            while (running) {
                // 1. 获取下一个过期时间点
                // 2. 如果下一个过期时间点 >  当前时间: 阻塞等待, 当前时间 - 下一个过期时间点
                long waitTime = sessionExpiryQueue.getWaitTime();
                if (waitTime > 0) {
                    Thread.sleep(waitTime);
                    continue;
                }

                // 3. 如果下一个过期时间点 <= 当前时间: 更新下一个过期时间点, 剔除 Session, 断开连接
                for (SessionImpl s : sessionExpiryQueue.poll()) {
                    ServerMetrics.getMetrics().STALE_SESSIONS_EXPIRED.add(1);
                    setSessionClosing(s.sessionId);
                    expirer.expire(s);
                }
            }
        } catch (InterruptedException e) {
            handleException(this.getName(), e);
        }
        LOG.info("SessionTrackerImpl exited loop!");
    }

    public synchronized boolean touchSession(long sessionId, int timeout) {
        SessionImpl s = sessionsById.get(sessionId);

        if (s == null) {
            logTraceTouchInvalidSession(sessionId, timeout);
            return false;
        }

        if (s.isClosing()) {
            logTraceTouchClosingSession(sessionId, timeout);
            return false;
        }

        // 没关闭则更新过期时间
        updateSessionExpiry(s, timeout);
        return true;
    }

    private void updateSessionExpiry(SessionImpl s, int timeout) {
        logTraceTouchSession(s.sessionId, timeout, "");
        sessionExpiryQueue.update(s, timeout);
    }

    private void logTraceTouchSession(long sessionId, int timeout, String sessionStatus) {
        if (LOG.isTraceEnabled()) {
            String msg = MessageFormat.format(
                "SessionTrackerImpl --- Touch {0}session: 0x{1} with timeout {2}",
                sessionStatus,
                Long.toHexString(sessionId),
                Integer.toString(timeout));

            ZooTrace.logTraceMessage(LOG, ZooTrace.CLIENT_PING_TRACE_MASK, msg);
        }
    }

    private void logTraceTouchInvalidSession(long sessionId, int timeout) {
        logTraceTouchSession(sessionId, timeout, "invalid ");
    }

    private void logTraceTouchClosingSession(long sessionId, int timeout) {
        logTraceTouchSession(sessionId, timeout, "closing ");
    }

    public int getSessionTimeout(long sessionId) {
        return sessionsWithTimeout.get(sessionId);
    }

    public synchronized void setSessionClosing(long sessionId) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Session closing: 0x{}", Long.toHexString(sessionId));
        }

        SessionImpl s = sessionsById.get(sessionId);
        if (s == null) {
            return;
        }
        s.isClosing = true;
    }

    public synchronized void removeSession(long sessionId) {
        LOG.debug("Removing session 0x{}", Long.toHexString(sessionId));
        SessionImpl s = sessionsById.remove(sessionId);
        sessionsWithTimeout.remove(sessionId);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(
                LOG,
                ZooTrace.SESSION_TRACE_MASK,
                "SessionTrackerImpl --- Removing session 0x" + Long.toHexString(sessionId));
        }
        if (s != null) {
            sessionExpiryQueue.remove(s);
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        running = false;
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(), "Shutdown SessionTrackerImpl!");
        }
    }

    public long createSession(int sessionTimeout) {
        long sessionId = nextSessionId.getAndIncrement();
        trackSession(sessionId, sessionTimeout); // 维护 SessionId -> SessionImpl 和 SessionImpl 过期时间
        return sessionId;
    }

    /**
     * 维护 SessionId -> SessionImpl 和 SessionImpl 过期时间
     */
    @Override
    public synchronized boolean trackSession(long id, int sessionTimeout) {
        boolean added = false;

        SessionImpl session = sessionsById.get(id);
        if (session == null) {
            session = new SessionImpl(id, sessionTimeout);
        }

        // findbugs2.0.3 complains about get after put.
        // long term strategy would be use computeIfAbsent after JDK 1.8
        SessionImpl existedSession = sessionsById.putIfAbsent(id, session); // 维护 SessionId -> SessionImpl

        if (existedSession != null) {
            session = existedSession;
        } else {
            added = true;
            LOG.debug("Adding session 0x{}", Long.toHexString(id));
        }

        if (LOG.isTraceEnabled()) {
            String actionStr = added ? "Adding" : "Existing";
            ZooTrace.logTraceMessage(
                LOG,
                ZooTrace.SESSION_TRACE_MASK,
                "SessionTrackerImpl --- " + actionStr
                + " session 0x" + Long.toHexString(id) + " " + sessionTimeout);
        }

        updateSessionExpiry(session, sessionTimeout); // 维护 SessionImpl 过期时间
        return added;
    }

    public synchronized boolean commitSession(long id, int sessionTimeout) {
        return sessionsWithTimeout.put(id, sessionTimeout) == null;
    }

    public boolean isTrackingSession(long sessionId) {
        return sessionsById.containsKey(sessionId);
    }

    public synchronized void checkSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException, KeeperException.UnknownSessionException {
        LOG.debug("Checking session 0x{}", Long.toHexString(sessionId));
        SessionImpl session = sessionsById.get(sessionId);

        if (session == null) {
            throw new KeeperException.UnknownSessionException();
        }

        if (session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }

        if (session.owner == null) {
            session.owner = owner;
        } else if (session.owner != owner) {
            throw new KeeperException.SessionMovedException();
        }
    }

    public synchronized void setOwner(long id, Object owner) throws SessionExpiredException {
        SessionImpl session = sessionsById.get(id);
        if (session == null || session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }
        session.owner = owner;
    }

    public void checkGlobalSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException {
        try {
            checkSession(sessionId, owner);
        } catch (KeeperException.UnknownSessionException e) {
            throw new KeeperException.SessionExpiredException();
        }
    }

    public long getLocalSessionCount() {
        return 0;
    }

    @Override
    public boolean isLocalSessionsEnabled() {
        return false;
    }

    public Set<Long> globalSessions() {
        return sessionsById.keySet();
    }

    public Set<Long> localSessions() {
        return Collections.emptySet();
    }
}
