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

import com.ofcoder.klein.serializer.SerializationException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * ProtobufInnerSerializer.
 * @author leizhiyuan
 */
public class ProtobufInnerSerializer {

    private static final ProtobufHelper PROTOBUF_HELPER = new ProtobufHelper();

    /**
     * Decode method name.
     */
    private static final String METHOD_PARSEFROM = "parseFrom";

    /**
     * reflection instance.
     * @param responseClazz class
     * @param content byte[]
     * @param <T> type
     * @return object
     */
    @SuppressWarnings("unchecked")
    public static <T> T deserializeContent(final String responseClazz, final byte[] content) {
        if (content == null || content.length == 0) {
            return null;
        }
        Class clazz = PROTOBUF_HELPER.getPbClass(responseClazz);

        Method method = PROTOBUF_HELPER.getParseFromMethodMap().computeIfAbsent(clazz, key -> {
            try {
                Method m = clazz.getMethod(METHOD_PARSEFROM, byte[].class);
                if (!Modifier.isStatic(m.getModifiers())) {
                    throw new SerializationException("Cannot found static method " + clazz.getName()
                        + ".parseFrom(byte[]), please check the generated code");
                }
                m.setAccessible(true);
                return m;
            } catch (NoSuchMethodException e) {
                throw new SerializationException("Cannot found method " + clazz.getName()
                    + ".parseFrom(byte[]), please check the generated code", e);
            }
        });

        try {
            return (T) method.invoke(null, content);
        } catch (Exception e) {
            throw new SerializationException("Error when invoke " + clazz.getName() + ".parseFrom(byte[]).", e);
        }
    }
}
