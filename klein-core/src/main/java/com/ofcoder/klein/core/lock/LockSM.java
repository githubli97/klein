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
package com.ofcoder.klein.core.lock;

import com.ofcoder.klein.common.util.TrueTime;
import com.ofcoder.klein.consensus.facade.sm.AbstractSM;
import com.ofcoder.klein.core.cache.CacheSM;
import com.ofcoder.klein.serializer.Serializer;
import com.ofcoder.klein.spi.ExtensionLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Lock State Machine.
 */
public class LockSM extends AbstractSM {
    public static final String GROUP = "lock";
    private static final Logger LOG = LoggerFactory.getLogger(CacheSM.class);
    private final Map<String, LockInstance> locks = new HashMap<>();
    private final Serializer serializer = ExtensionLoader.getExtensionLoader(Serializer.class).register("hessian2");

    @Override
    public byte[] apply(final byte[] original) {
        Object data = serializer.deserialize(original);
        if (!(data instanceof LockMessage)) {
            LOG.warn("apply data, UNKNOWN PARAMETER TYPE, data type is {}", data.getClass().getName());
            return null;
        }
        LockMessage message = (LockMessage) data;
        locks.putIfAbsent(message.getKey(), new LockInstance());
        LockInstance instance = locks.get(message.getKey());

        switch (message.getOp()) {
            case LockMessage.LOCK:
                if (instance.lockState == LockInstance.UNLOCK_STATE || (instance.expire != LockMessage.TTL_PERPETUITY && instance.expire < TrueTime.currentTimeMillis())) {
                    instance.lockState = LockInstance.LOCKED_STATE;
                    instance.expire = message.getExpire();
                    return serializer.serialize(true);
                } else {
                    return serializer.serialize(false);
                }
            case LockMessage.UNLOCK:
                instance.lockState = LockInstance.UNLOCK_STATE;
                instance.expire = 0;
                break;
            default:
                break;
        }
        return null;
    }

    @Override
    public byte[] makeImage() {
        return serializer.serialize(locks);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void loadImage(final byte[] original) {
        Object snap = serializer.deserialize(original);
        if (!(snap instanceof Map)) {
            return;
        }
        Map<String, LockInstance> snapInstance = (Map<String, LockInstance>) snap;
        locks.clear();
        locks.putAll(snapInstance);
    }

    static class LockInstance implements Serializable {
        private static final byte UNLOCK_STATE = 0x00;
        private static final byte LOCKED_STATE = 0x01;
        private Byte lockState = UNLOCK_STATE;
        private long expire = 0;

        public Byte getLockState() {
            return lockState;
        }

        public void setLockState(final Byte lockState) {
            this.lockState = lockState;
        }

        public long getExpire() {
            return expire;
        }

        public void setExpire(final long expire) {
            this.expire = expire;
        }
    }
}
