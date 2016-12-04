package com.carbonat.dbms;

import com.carbonat.common.ExceptionType;
import com.carbonat.table.DataType;
import com.carbonat.user.UserType;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;

@WebService
@SOAPBinding(style = SOAPBinding.Style.RPC)
public interface DBMS {
    // databases
    @WebMethod
    String getDatabases();

    // file content
    @WebMethod
    String getText(String path);

    @WebMethod
    UserType userExists(String userName, String userPassword);

    @WebMethod
    boolean saveUser(String userName, String userPassword);

    @WebMethod
    String getTables(String databaseName);

    @WebMethod
    boolean saveDatabase(String databaseName);

    @WebMethod
    boolean deleteDatabase(String databaseName);

    @WebMethod
    DataType[] getTableDataTypes(String tableName, String databaseName);

    @WebMethod
    String getTableColumnsNames(String tableName, String databaseName);

    @WebMethod
    boolean saveTableStructure(String jsonStructure, String tableName, String databaseName);

    @WebMethod
    String getTableData(String tableName, String databaseName);

    @WebMethod
    boolean saveTableData(String tableName, String databaseName, String jsonData, boolean rewrite);

    @WebMethod
    boolean saveTable(String tableName, String databaseName, String jsonStructure, String jsonData, boolean rewrite);

    @WebMethod
    boolean deleteTable(String tableName, String databaseName);

    @WebMethod
    String sort(String tableName, String databaseName, int columnNumber);

    @WebMethod
    String getErrorMsg();

    @WebMethod
    ExceptionType getExceptionType();

    @WebMethod
    boolean isNewError();

}
