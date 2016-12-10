package com.carbonat.common;

public abstract class DBMSUnit {
    protected ErrorMsg errorMsg;

    public DBMSUnit() {};

    public String getErrorMsg() {
        return errorMsg.getMsg();
    }

    public ExceptionType getExceptionType() {
        return errorMsg.getExceptionType();
    }

    public boolean isNewError() {
        return errorMsg.getIsNew();
    }
}
