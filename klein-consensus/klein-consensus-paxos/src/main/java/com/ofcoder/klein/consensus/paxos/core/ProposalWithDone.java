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

import com.ofcoder.klein.common.disruptor.DisruptorEvent;
import com.ofcoder.klein.storage.facade.Command;

/**
 * Proposal and Propose Callback.
 *
 * @author 释慧利
 */
public class ProposalWithDone extends DisruptorEvent {
    private Command proposal;
    private ProposeDone done;

    public ProposalWithDone() {
    }

    public ProposalWithDone(final Command proposal, final ProposeDone done) {
        this.proposal = proposal;
        this.done = done;
    }

    public Command getProposal() {
        return proposal;
    }

    public void setProposal(final Command proposal) {
        this.proposal = proposal;
    }

    public ProposeDone getDone() {
        return done;
    }

    public void setDone(final ProposeDone done) {
        this.done = done;
    }
}
