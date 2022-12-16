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
package com.ofcoder.klein.consensus.paxos.rpc.vo;

/**
 * prepare request data.
 *
 * @author far.liu
 */
public class PrepareReq extends BaseReq {

    public static final class Builder {
        private String nodeId;
        private long proposalNo;
        private int memberConfigurationVersion;

        private Builder() {
        }

        /**
         * aPrepareReq.
         *
         * @return Builder
         */
        public static Builder aPrepareReq() {
            return new Builder();
        }

        /**
         * nodeId.
         *
         * @param nodeId nodeId
         * @return Builder
         */
        public Builder nodeId(final String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        /**
         * proposalNo.
         *
         * @param proposalNo proposalNo
         * @return Builder
         */
        public Builder proposalNo(final long proposalNo) {
            this.proposalNo = proposalNo;
            return this;
        }

        /**
         * memberConfigurationVersion.
         *
         * @param memberConfigurationVersion memberConfigurationVersion
         * @return Builder
         */
        public Builder memberConfigurationVersion(final int memberConfigurationVersion) {
            this.memberConfigurationVersion = memberConfigurationVersion;
            return this;
        }

        /**
         * build.
         *
         * @return PrepareReq
         */
        public PrepareReq build() {
            PrepareReq prepareReq = new PrepareReq();
            prepareReq.setNodeId(nodeId);
            prepareReq.setProposalNo(proposalNo);
            prepareReq.setMemberConfigurationVersion(memberConfigurationVersion);
            return prepareReq;
        }
    }
}
