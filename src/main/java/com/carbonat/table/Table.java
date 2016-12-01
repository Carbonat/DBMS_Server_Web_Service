package com.carbonat.table;

import com.carbonat.common.ExceptionType;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;

@WebService
@SOAPBinding(style = SOAPBinding.Style.RPC)
public interface Table {
    @WebMethod
    String getName();

    @WebMethod
    void setName(String name);

    @WebMethod
    String getDatabaseName();

    @WebMethod
    void setDatabaseName(String databaseName);

    @WebMethod
    DataType[] getDataTypes();

    @WebMethod
    String getColumnsNames();

    @WebMethod
    boolean setStructure(String jsonStructure);

    @WebMethod
    String getData();

    @WebMethod
    boolean setData(String jsonData);

    @WebMethod
    boolean addRow(String jsonRow);

    @WebMethod
    boolean saveStructure();

    @WebMethod
    boolean saveData();

    @WebMethod
    boolean save(boolean rewrite);

    @WebMethod
    boolean delete();

    @WebMethod
    boolean readData();

    @WebMethod
    boolean sort(int i, String name);

    @WebMethod
    boolean connectTo(String table2Name, int columnN1, int columnN2, String newTableName);

    @WebMethod
    boolean saveBuffer();

    @WebMethod
    DataType[] getDataTypesBuffer();

    @WebMethod
    String getDataBuffer();

    @WebMethod
    String getColumnsNamesBuffer();

    @WebMethod
    void setTableAndBufferNull();

    @WebMethod
    ExceptionType getExceptionType();

    @WebMethod
    String getError();

    @WebMethod
    boolean isErrorExists();

    @WebMethod
    void setErrorMsgNull();
}
