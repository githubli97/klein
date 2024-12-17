package com.ofcoder.klein.serializer.protobuf;

import com.ofcoder.klein.serializer.Serializer;
import com.ofcoder.klein.serializer.protobuf.generated.TestProto;
import com.ofcoder.klein.spi.ExtensionLoader;
import junit.framework.TestCase;
import org.junit.Assert;

public class ProtobufSerializerTest extends TestCase {

    public void testSerialize() throws Exception {
        Serializer serializer = ExtensionLoader.getExtensionLoader(Serializer.class).register("protobuf");
        TestProto testProto = TestProto.newBuilder()
            .setA("1")
            .setB(2)
            .setC(3)
            .setD(false)
            .build();
        byte[] serialize = serializer.serialize(testProto);
        Assert.assertNotNull(serialize);
        TestProto deserialize = (TestProto) serializer.deserialize(serialize);
        Assert.assertEquals(deserialize, testProto);
    }
}