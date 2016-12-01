package com.carbonat.databases;

import com.carbonat.common.ExceptionType;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;

@WebService
@SOAPBinding(style = SOAPBinding.Style.RPC)
public interface Databases {
    @WebMethod
    String getDatabases();

    @WebMethod
    ExceptionType getExceptionType();

    @WebMethod
    String getError();

    @WebMethod
    boolean isErrorExists();

    @WebMethod
    void setErrorMsgNull();
}
