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
package com.ofcoder.klein.consensus.paxos;

import com.ofcoder.klein.consensus.facade.Result;

/**
 * Proxy is used for forwarding write requests.
 *
 * @author 释慧利
 */
public interface ProposeProxy {
    /**
     * propose proposal.
     *
     * @param data  proposal
     * @param apply Whether you need to wait until the state machine is applied
     *              If true, wait until the state machine is applied before returning
     * @return propose result
     */
    Result propose(Proposal data, boolean apply);

    /**
     * todo: read index.
     *
     * @param group group name
     * @return instance id
     */
    Long readIndex(String group);

}
