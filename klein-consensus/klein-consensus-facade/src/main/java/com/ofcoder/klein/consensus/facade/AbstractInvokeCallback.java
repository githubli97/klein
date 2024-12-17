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
package com.ofcoder.klein.consensus.facade;

import com.google.protobuf.GeneratedMessageV3;
import com.ofcoder.klein.rpc.facade.InvokeCallback;
import com.ofcoder.klein.serializer.protobuf.ProtobufUtil;
import java.nio.ByteBuffer;

/**
 * Invoke callback.
 *
 * @author far.liu
 */
public abstract class AbstractInvokeCallback<RES extends GeneratedMessageV3> implements InvokeCallback {
    /**
     * invoke completed.
     *
     * @param result invoke result
     */
    public abstract void complete(RES result);

    @Override
    public void complete(final ByteBuffer result) {
        RES deserialize = ProtobufUtil.deserialize(result.array());
        complete(deserialize);
    }
}
