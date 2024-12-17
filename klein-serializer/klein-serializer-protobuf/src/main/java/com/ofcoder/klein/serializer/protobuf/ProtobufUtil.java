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
package com.ofcoder.klein.serializer.protobuf;

import com.google.protobuf.GeneratedMessageV3;

/**
 * Protobuf util.
 *
 * @author hang.li
 */
public class ProtobufUtil {

    private static final ProtobufSerializer PROTOBUF_SERIALIZER = new ProtobufSerializer();

    /**
     * serialize object.
     *
     * @param javaBean object
     * @return serialized data
     */
    public static byte[] serialize(final GeneratedMessageV3 javaBean) {
        return PROTOBUF_SERIALIZER.serialize(javaBean);
    }

    /**
     * deserialize data to java object.
     *
     * @param serializeData serialized data
     * @param <T>           java object type
     * @return java object
     */
    @SuppressWarnings("unchecked")
    public static <T extends GeneratedMessageV3> T deserialize(final byte[] serializeData) {
        return (T) PROTOBUF_SERIALIZER.deserialize(serializeData);
    }
}
