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
import com.ofcoder.klein.serializer.SerializationException;
import com.ofcoder.klein.serializer.Serializer;
import com.ofcoder.klein.spi.Join;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Protobuf Serializer.
 *
 * @author hang.li
 */
@Join
public class ProtobufSerializer implements Serializer<GeneratedMessageV3> {

    private static final Charset UTF8 = StandardCharsets.UTF_8;

    @Override
    public byte[] serialize(final GeneratedMessageV3 t) {
        final String name = t.getClass().getName();
        final byte[] nameBytes = name.getBytes(UTF8);

        byte[] body = t.toByteArray();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + nameBytes.length + body.length);
        byteBuffer.putInt(nameBytes.length);
        byteBuffer.put(nameBytes);
        byteBuffer.put(body);
        byteBuffer.flip();
        byte[] content = new byte[byteBuffer.limit()];
        byteBuffer.get(content);
        return content;
    }

    @Override
    public GeneratedMessageV3 deserialize(final byte[] bytes) throws SerializationException {
        if (bytes == null) {
            throw new NullPointerException();
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int clazzNameLength = byteBuffer.getInt();
        byte[] clazzName = new byte[clazzNameLength];
        byteBuffer.get(clazzName);
        byte[] body = new byte[bytes.length - clazzNameLength - 4];
        byteBuffer.get(body);
        final String descriptorName = new String(clazzName, UTF8);
        Class protobufClazz;
        try {
            protobufClazz = Class.forName(descriptorName);
        } catch (ClassNotFoundException e) {
            throw new SerializationException("class " + descriptorName
                + "no found");
        }
        Object protobufObject = ProtobufInnerSerializer.deserializeContent(protobufClazz.getName(), body);
        return (GeneratedMessageV3) protobufObject;
    }
}
