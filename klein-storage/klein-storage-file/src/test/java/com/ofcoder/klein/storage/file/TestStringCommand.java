package com.ofcoder.klein.storage.file;

import com.ofcoder.klein.storage.facade.Command;
import java.nio.charset.StandardCharsets;

public class TestStringCommand implements Command {
    private String target;

    public TestStringCommand(String target) {
        this.target = target;
    }

    @Override
    public boolean ifNoop() {
        return false;
    }

    @Override
    public String getGroup() {
        return "DEFAULT";
    }

    @Override
    public byte[] getData() {
        return target.getBytes(StandardCharsets.UTF_8);
    }
}
