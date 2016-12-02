package com.carbonat.common;

import javax.jws.WebService;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@WebService(endpointInterface = "com.carbonat.common.FileContent")
public class FileContentImpl implements FileContent {

    private ErrorMsg errorMsg;

    @Override
    public String getText(String path) {
        String text = "";
        try {
            text = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            errorMsg = new ErrorMsg(ExceptionType.InputOutput, ex.getMessage());
        } catch (Exception ex) {
            errorMsg = new ErrorMsg(ExceptionType.InputOutput, ex.getMessage());
        }
        return text;
    }

    @Override
    public ExceptionType getExceptionType() {
        return errorMsg != null ? errorMsg.getExceptionType() : null;
    }

    @Override
    public String getError() {
        return errorMsg != null ? errorMsg.getMsg() : null;
    }

    @Override
    public boolean isErrorExists() {
        return errorMsg != null && errorMsg.getIsNew();
    }

    @Override
    public void setErrorMsgNull() {
        errorMsg = null;
    }
}
