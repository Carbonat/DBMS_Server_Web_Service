package com.carbonat.common;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import java.io.IOException;

@WebService
@SOAPBinding(style = SOAPBinding.Style.RPC)
public interface FileContent {
    @WebMethod
    String getText(String path);

    @WebMethod
    ExceptionType getExceptionType();

    @WebMethod
    String getError();

    @WebMethod
    boolean isErrorExists();

    @WebMethod
    void setErrorMsgNull();
}
