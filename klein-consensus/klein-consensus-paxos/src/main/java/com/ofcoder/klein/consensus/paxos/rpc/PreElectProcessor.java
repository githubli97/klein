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
package com.ofcoder.klein.consensus.paxos.rpc;

import com.ofcoder.klein.consensus.facade.AbstractRpcProcessor;
import com.ofcoder.klein.consensus.paxos.PaxosNode;
import com.ofcoder.klein.consensus.paxos.core.RuntimeAccessor;
import com.ofcoder.klein.consensus.paxos.rpc.vo.PreElectReq;
import com.ofcoder.klein.consensus.paxos.rpc.vo.PreElectRes;
import com.ofcoder.klein.rpc.facade.RpcContext;

/**
 * before election master to use, find master in the cluster.
 *
 * @author 释慧利
 */
public class PreElectProcessor extends AbstractRpcProcessor<PreElectReq> {
    private PaxosNode self;

    public PreElectProcessor(final PaxosNode self) {
        this.self = self;
    }

    @Override
    public void handleRequest(final PreElectReq request, final RpcContext context) {
        response(PreElectRes.Builder.aPreElectRes().master(
            RuntimeAccessor.getMaster().getMaster().getMaster()
        ).build(), context);
    }

    @Override
    public String service() {
        return PreElectReq.class.getSimpleName();
    }
}
