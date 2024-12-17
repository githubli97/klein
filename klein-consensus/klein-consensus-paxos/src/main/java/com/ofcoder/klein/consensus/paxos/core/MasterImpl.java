/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ofcoder.klein.consensus.paxos.core;

import com.ofcoder.klein.serializer.hessian2.Hessian2Util;
import com.ofcoder.klein.common.util.ThreadExecutor;
import com.ofcoder.klein.common.util.TrueTime;
import com.ofcoder.klein.common.util.timer.RepeatedTimer;
import com.ofcoder.klein.consensus.facade.AbstractInvokeCallback;
import com.ofcoder.klein.consensus.facade.NoopCommand;
import com.ofcoder.klein.consensus.facade.config.ConsensusProp;
import com.ofcoder.klein.consensus.facade.quorum.Quorum;
import com.ofcoder.klein.consensus.facade.quorum.QuorumFactory;
import com.ofcoder.klein.consensus.facade.quorum.SingleQuorum;
import com.ofcoder.klein.consensus.paxos.PaxosNode;
import com.ofcoder.klein.consensus.paxos.Proposal;
import com.ofcoder.klein.consensus.paxos.core.sm.ElectionOp;
import com.ofcoder.klein.consensus.paxos.core.sm.MasterSM;
import com.ofcoder.klein.consensus.paxos.core.sm.MemberRegistry;
import com.ofcoder.klein.consensus.paxos.core.sm.PaxosMemberConfiguration;
import com.ofcoder.klein.consensus.paxos.rpc.generated.AbstractBaseReqProto;
import com.ofcoder.klein.consensus.paxos.rpc.generated.NewMasterReqProto;
import com.ofcoder.klein.consensus.paxos.rpc.generated.NewMasterResProto;
import com.ofcoder.klein.consensus.paxos.rpc.generated.NodeStateProto;
import com.ofcoder.klein.consensus.paxos.rpc.generated.PingReqProto;
import com.ofcoder.klein.consensus.paxos.rpc.generated.PongResProto;
import com.ofcoder.klein.consensus.paxos.rpc.generated.PreElectReqProto;
import com.ofcoder.klein.consensus.paxos.rpc.vo.PreElectRes;
import com.ofcoder.klein.rpc.facade.Endpoint;
import com.ofcoder.klein.rpc.facade.RpcClient;
import com.ofcoder.klein.spi.ExtensionLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master implement.
 *
 * @author 释慧利
 */
public class MasterImpl implements Master {
    private static final Logger LOG = LoggerFactory.getLogger(MasterImpl.class);
    private Endpoint master;
    private ElectState masterState;
    private final List<Listener> listeners = new ArrayList<>();

    private final PaxosNode self;
    private final PaxosMemberConfiguration memberConfig;
    private RepeatedTimer waitHeartbeatTimer;
    private RepeatedTimer sendHeartbeatTimer;
    private RepeatedTimer electTimer;
    private RpcClient client;
    private ConsensusProp prop;
    private final AtomicBoolean electing = new AtomicBoolean(false);

    public MasterImpl(final PaxosNode self) {
        this.self = self;
        this.memberConfig = MemberRegistry.getInstance().getMemberConfiguration();
    }

    @Override
    public void shutdown() {
        try {
            if (waitHeartbeatTimer != null) {
                waitHeartbeatTimer.destroy();
            }
            if (sendHeartbeatTimer != null) {
                sendHeartbeatTimer.destroy();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void init(final ConsensusProp op) {
        this.prop = op;
        this.client = ExtensionLoader.getExtensionLoader(RpcClient.class).getJoin();
        if (!prop.getPaxosProp().isEnableMaster() || prop.getSelf().isOutsider()) {
            this.master = null;
            this.masterState = ElectState.DISABLE;
            return;
        }

        waitHeartbeatTimer = new RepeatedTimer("follower-wait-heartbeat", prop.getPaxosProp().getMasterHeartbeatInterval()) {
            @Override
            protected void onTrigger() {
                resetMaster();
                restartElect();
            }
        };

        electTimer = new RepeatedTimer("elect-master", calculateElectionMasterInterval()) {
            @Override
            protected void onTrigger() {
                election();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                return calculateElectionMasterInterval();
            }
        };

        sendHeartbeatTimer = new RepeatedTimer("master-send-heartbeat", (int) (prop.getPaxosProp().getMasterHeartbeatInterval() * 0.95)) {
            @Override
            protected void onTrigger() {
                if (!sendHeartbeat(false)) {
                    restartElect();
                }
            }
        };
    }

    private void changeMaster(final String nodeId) {
        if (memberConfig.isValid(nodeId)) {
            this.master = memberConfig.getEndpointById(nodeId);
            this.masterState = self.getSelf().equals(master) ? ElectState.LEADING : ElectState.FOLLOWING;
            LOG.info("node-{} was promoted to master, version: {}", nodeId, memberConfig.getVersion());
        }
        MasterState state = getMaster();
        listeners.forEach(listener -> listener.onChange(state));
    }

    private void resetMaster() {
        this.master = null;
        this.masterState = ElectState.ELECTING;

        MasterState state = getMaster();
        listeners.forEach(listener -> listener.onChange(state));
    }

    private int calculateElectionMasterInterval() {
        return ThreadLocalRandom.current().nextInt(prop.getPaxosProp().getMasterElectMinInterval(), prop.getPaxosProp().getMasterElectMaxInterval());
    }

    @Override
    public MasterState getMaster() {
        return new MasterState(master, masterState, self.getSelf().equals(master));
    }

    @Override
    public void addListener(final Listener listener) {
        listeners.add(listener);
        listener.onChange(getMaster());
    }

    @Override
    public void searchMaster() {
        PreElectReqProto req = PreElectReqProto.newBuilder()
            .setAbstractBaseReq(AbstractBaseReqProto.newBuilder()
                .setMemberConfigurationVersion(memberConfig.getVersion())
                .setNodeId(self.getSelf().getId())
                .setProposalNo(self.getCurProposalNo()))
            .build();
        for (Endpoint it : memberConfig.getMembersWithout(self.getSelf().getId())) {
            try {
                PreElectRes res = client.sendRequestSync(it, req);
                LOG.debug("looking for master, node-{}: {}", it.getId(), res);
                if (res != null && res.getMaster() != null) {
//                restartWaitHb();
                    changeMaster(res.getMaster().getId());
                    return;
                }
            } catch (Exception e) {
                LOG.warn("looking for master fail, {}", e.getMessage());
            }
        }
        // there is no master in the cluster, do elect.
        restartElect();
    }

    @Override
    public void transferMaster() {

    }

    private void election() {

        if (!electing.compareAndSet(false, true)) {
            return;
        }

        try {
            LOG.info("start electing master.");
            ElectionOp req = new ElectionOp();
            req.setNodeId(self.getSelf().getId());

            CountDownLatch latch = new CountDownLatch(1);
            Proposal proposal = new Proposal(MasterSM.GROUP, Hessian2Util.serialize(req));

            RuntimeAccessor.getProposer().propose(proposal, (result, changed) -> {
                if (result && !changed) {
                    newMaster(latch);
                } else {
                    latch.countDown();
                }
            }, true);

            try {
                boolean await = latch.await(this.prop.getRoundTimeout() * this.prop.getRetry(), TimeUnit.MILLISECONDS);
                // do nothing for await's result, stop this timer in {@link #handleNewMasterRes}
            } catch (InterruptedException e) {
                LOG.debug(e.getMessage());
            }
        } finally {
            electing.compareAndSet(true, false);
        }
    }

    private void newMaster(final CountDownLatch latch) {
        LOG.info("start new master.");

        PaxosMemberConfiguration memberConfiguration = memberConfig.createRef();
        NewMasterReqProto req = NewMasterReqProto.newBuilder()
            .setAbstractBaseReq(AbstractBaseReqProto.newBuilder()
                .setNodeId(self.getSelf().getId())
                .setProposalNo(self.getCurProposalNo())
                .setMemberConfigurationVersion(memberConfiguration.getVersion()))
            .build();
        Quorum quorum = QuorumFactory.createWriteQuorum(memberConfiguration);
        AtomicBoolean next = new AtomicBoolean(false);

        // for self
        NewMasterResProto masterRes = onReceiveNewMaster(req, true);
        handleNewMasterRes(self.getSelf(), masterRes, quorum, next, latch);

        // for other members
        memberConfiguration.getMembersWithout(self.getSelf().getId()).forEach(it ->
                client.sendRequestAsync(it, req, new AbstractInvokeCallback<NewMasterResProto>() {
                    @Override
                    public void error(final Throwable err) {
                        quorum.refuse(it);
                        if (quorum.isGranted() == Quorum.GrantResult.REFUSE && next.compareAndSet(false, true)) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void complete(final NewMasterResProto result) {
                        handleNewMasterRes(it, result, quorum, next, latch);
                    }
                }));
    }

    private void handleNewMasterRes(final Endpoint it, final NewMasterResProto result, final Quorum quorum, final AtomicBoolean next, final CountDownLatch latch) {
        self.updateCurInstanceId(result.getCurInstanceId());

        if (result.getGranted()) {
            quorum.grant(it);
            if (quorum.isGranted() == Quorum.GrantResult.PASS && next.compareAndSet(false, true)) {
                restartSendHbNow();

                RuntimeAccessor.getProposer().propose(NoopCommand.NOOP, (noopResult, dataChange) -> {
                    if (noopResult) {
                        changeMaster(self.getSelf().getId());
                    } else {
                        restartElect();
                    }
                    latch.countDown();
                }, true);
            }
        } else {
            quorum.refuse(it);
            if (quorum.isGranted() == SingleQuorum.GrantResult.REFUSE && next.compareAndSet(false, true)) {
                latch.countDown();
            }
        }
    }

    /**
     * Send Heartbeat Msg.
     *
     * @param probe probe msg
     * @return true if the majority responds
     */
    private boolean sendHeartbeat(final boolean probe) {
        final long curInstanceId = self.getCurInstanceId();
        long lastCheckpoint = RuntimeAccessor.getLearner().getLastCheckpoint();
        final PaxosMemberConfiguration memberConfiguration = memberConfig.createRef();

        final Quorum quorum = QuorumFactory.createWriteQuorum(memberConfiguration);
        final PingReqProto req = PingReqProto.newBuilder()
            .setAbstractBaseReq(AbstractBaseReqProto.newBuilder()
                .setNodeId(self.getSelf().getId())
                .setProposalNo(self.getCurProposalNo())
                .setMemberConfigurationVersion(memberConfiguration.getVersion())
                .build())
            .setNodeState(NodeStateProto.newBuilder()
                .setNodeId(self.getSelf().getId())
                .setMaxInstanceId(curInstanceId)
                .setLastCheckpoint(lastCheckpoint)
                .setLastAppliedInstanceId(RuntimeAccessor.getLearner().getLastAppliedInstanceId())
                .build())
            .setTimestampMs(TrueTime.currentTimeMillis())
            .setProbe(probe)
            .build();

        final CompletableFuture<SingleQuorum.GrantResult> complete = new CompletableFuture<>();
        // for self
        if (onReceiveHeartbeat(req, true)) {
            quorum.grant(self.getSelf());
            if (quorum.isGranted() == SingleQuorum.GrantResult.PASS) {
                complete.complete(quorum.isGranted());
            }
        }

        // for other members
        memberConfiguration.getMembersWithout(self.getSelf().getId()).forEach(it -> {
            client.sendRequestAsync(it, req, new AbstractInvokeCallback<PongResProto>() {
                @Override
                public void error(final Throwable err) {
                    LOG.debug("heartbeat, node: " + it.getId() + ", " + err.getMessage());
                    quorum.refuse(it);
                    if (quorum.isGranted() == SingleQuorum.GrantResult.REFUSE) {
                        complete.complete(SingleQuorum.GrantResult.REFUSE);
                    }
                }

                @Override
                public void complete(final PongResProto result) {
                    quorum.grant(it);
                    if (quorum.isGranted() == SingleQuorum.GrantResult.PASS) {
                        complete.complete(SingleQuorum.GrantResult.PASS);
                    }
                }
            });
        });
        try {
            SingleQuorum.GrantResult grantResult = complete.get(client.requestTimeout() + 10, TimeUnit.MILLISECONDS);
            return grantResult == SingleQuorum.GrantResult.PASS;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.warn("master send heartbeat occur exception, {}", e.getMessage());
            return false;
        }
    }

    @Override
    public boolean onReceiveHeartbeat(final PingReqProto request, final boolean isSelf) {
        NodeStateProto nodeStateProto = request.getNodeState();

        self.updateCurInstanceId(nodeStateProto.getMaxInstanceId());

        if (request.getAbstractBaseReq().getMemberConfigurationVersion() >= memberConfig.getVersion()) {

            if (!request.getProbe() && !isSelf) {
                // check and update instance
                ThreadExecutor.execute(() -> {
                    RuntimeAccessor.getLearner().alignData(nodeStateProto);
                });
            }

            TrueTime.heartbeat(request.getTimestampMs());

            // reset and restart election timer
            if (!isSelf) {
                restartWaitHb();
            }
            LOG.debug("receive heartbeat from node-{}, result: true.", request.getAbstractBaseReq().getNodeId());
            return true;
        } else {
            LOG.debug("receive heartbeat from node-{}, result: false. local.master: {}, req.version: {}", request.getAbstractBaseReq().getNodeId(),
                    memberConfig, request.getAbstractBaseReq().getMemberConfigurationVersion());
            return false;
        }
    }

    @Override
    public NewMasterResProto onReceiveNewMaster(final NewMasterReqProto request, final boolean isSelf) {
        if (request.getAbstractBaseReq().getMemberConfigurationVersion() >= memberConfig.getVersion()) {
            if (!isSelf) {
                changeMaster(request.getAbstractBaseReq().getNodeId());
                restartWaitHb();
            }
            return NewMasterResProto.newBuilder()
                .setCheckpoint(RuntimeAccessor.getLearner().getLastCheckpoint())
                .setCurInstanceId(self.getCurInstanceId())
                .setLastAppliedId(RuntimeAccessor.getLearner().getLastAppliedInstanceId())
                .setGranted(true)
                .build();
        } else {
            return NewMasterResProto.newBuilder()
                .setCheckpoint(RuntimeAccessor.getLearner().getLastCheckpoint())
                .setCurInstanceId(self.getCurInstanceId())
                .setLastAppliedId(RuntimeAccessor.getLearner().getLastAppliedInstanceId())
                .setGranted(false)
                .build();
        }
    }

    private void restartWaitHb() {
        sendHeartbeatTimer.stop();
        waitHeartbeatTimer.stop();
        electTimer.stop();
        waitHeartbeatTimer.restart(false);
    }

    private void restartSendHbNow() {
        waitHeartbeatTimer.stop();
        sendHeartbeatTimer.stop();
        electTimer.stop();
        sendHeartbeatTimer.restart(true);
    }

    private void restartElect() {
        waitHeartbeatTimer.stop();
        sendHeartbeatTimer.stop();
        electTimer.stop();
        electTimer.restart(false);
    }

}
