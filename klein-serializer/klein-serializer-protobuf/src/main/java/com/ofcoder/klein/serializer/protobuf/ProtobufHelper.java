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

import com.google.protobuf.MessageLite;
import com.ofcoder.klein.serializer.SerializationException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ProtobufHelper.
 *
 * @author leizhiyuan
 */
public class ProtobufHelper {

    /**
     * Cache of parseFrom method.
     */
    private ConcurrentMap<Class, Method> parseFromMethodMap = new ConcurrentHashMap<>();

    /**
     * requestClassCache.
     *  {className:class}
     */
    private ConcurrentMap<String, Class> requestClassCache = new ConcurrentHashMap<>();

    /**
     * getPbClass.
     *
     * @param clazzName class name
     * @return the protobuf class
     */
    public Class getPbClass(final String clazzName) {
        return requestClassCache.computeIfAbsent(clazzName, key -> {
            // get the parameter and result
            Class clazz = null;
            try {
                clazz = Class.forName(clazzName);
            } catch (ClassNotFoundException ignored) {
            }
            if (clazz == void.class || !isProtoBufMessageClass(clazz)) {
                throw new SerializationException("class based protobuf: " + clazz.getName()
                        + ", only support return protobuf message!");
            }
            return clazz;
        });
    }

    /**
     * Is this class is assignable from MessageLite.
     *
     * @param clazz unknown class
     * @return is assignable from MessageLite
     */
    boolean isProtoBufMessageClass(final Class clazz) {
        return clazz != null && MessageLite.class.isAssignableFrom(clazz);
    }

    public ConcurrentMap<Class, Method> getParseFromMethodMap() {
        return parseFromMethodMap;
    }
}
