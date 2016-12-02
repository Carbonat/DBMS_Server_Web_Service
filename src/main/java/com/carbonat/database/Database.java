package com.carbonat.database;

import com.carbonat.common.ExceptionType;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;

@WebService
@SOAPBinding(style = SOAPBinding.Style.RPC)
public interface Database {
    @WebMethod
    String getName();

    @WebMethod
    void setName(String name);

    @WebMethod
    String getTables();

    @WebMethod
    int uniqueName();

    @WebMethod
    boolean save();

    @WebMethod
    boolean delete();

    @WebMethod
    ExceptionType getExceptionType();

    @WebMethod
    String getError();

    @WebMethod
    boolean isErrorExists();

    @WebMethod
    void setErrorMsgNull();
}
