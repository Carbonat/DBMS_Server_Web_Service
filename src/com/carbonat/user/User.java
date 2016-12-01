package com.carbonat.user;

import com.carbonat.common.ExceptionType;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;

@WebService
@SOAPBinding(style = SOAPBinding.Style.RPC)
public interface User {
    @WebMethod
    String getUsername();

    @WebMethod
    void setUsername(String username);

    @WebMethod
    String getPassword();

    @WebMethod
    void setPassword(String password);

    @WebMethod
    UserType getUserType();

    @WebMethod
    void setUserType(UserType userType);

    @WebMethod
    UserType userExists();

    @WebMethod
    boolean usernameIsUnique();

    @WebMethod
    boolean save();

    @WebMethod
    ExceptionType getExceptionType();

    @WebMethod
    String getError();

    @WebMethod
    boolean isErrorExists();

    @WebMethod
    void setErrorMsgNull();
}
