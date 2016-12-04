package com.carbonat.common;

public class ErrorMsg {
    private String msg;
    private ExceptionType exceptionType;
    private boolean isNew;

    public ErrorMsg(ExceptionType exceptionType, String msg) {
        this.msg = msg;
        this.exceptionType = exceptionType;
        this.isNew = true;
    }

    public String getMsg() {
        isNew = false;
        return msg;
    }

    public ExceptionType getExceptionType() {
        isNew = false;
        return exceptionType;
    }

    public boolean getIsNew() {
        return isNew;
    }
}
