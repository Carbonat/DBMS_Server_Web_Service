package com.carbonat.dbms;

import com.carbonat.common.DBMSUnit;
import com.carbonat.common.ExceptionType;
import com.carbonat.common.FileContent;
import com.carbonat.database.Database;
import com.carbonat.databases.Databases;
import com.carbonat.table.DataType;
import com.carbonat.table.Table;
import com.carbonat.user.User;
import com.carbonat.user.UserType;

import javax.jws.WebService;

@WebService(endpointInterface = "com.carbonat.dbms.DBMS")
public class DBMSImpl implements DBMS {
    DBMSUnit dbmsUnit;

    @Override
    public String getDatabases() {
        Databases databases = new Databases();
        dbmsUnit = databases;
        return databases.getDatabases() ;
    }

    @Override
    public String getText(String path) {
        FileContent fileContent = new FileContent();
        dbmsUnit = fileContent;
        return fileContent.getText(path);
    }

    @Override
    public UserType userExists(String userName, String userPassword) {
        User user = new User(userName, userPassword);
        dbmsUnit = user;
        return user.userExists();
    }

    @Override
    public boolean saveUser(String userName, String userPassword) {
        User user = new User(userName, userPassword, UserType.REGULAR_USER);
        dbmsUnit = user;
        return user.save();
    }

    @Override
    public String getTables(String databaseName) {
        Database database = new Database(databaseName);
        dbmsUnit = database;
        return database.getTables();
    }

    @Override
    public boolean saveDatabase(String databaseName) {
        Database database = new Database(databaseName);
        dbmsUnit = database;
        return database.save();
    }

    @Override
    public boolean deleteDatabase(String databaseName) {
        Database database = new Database(databaseName);
        dbmsUnit = database;
        return database.delete();
    }

    @Override
    public DataType[] getTableDataTypes(String tableName, String databaseName) {
        Table table = new Table(tableName, databaseName);
        dbmsUnit = table;
        return table.getDataTypes();
    }

    @Override
    public String getTableColumnsNames(String tableName, String databaseName) {
        Table table = new Table(tableName, databaseName);
        dbmsUnit = table;
        return table.getColumnsNames();
    }

    @Override
    public boolean saveTableStructure(String tableName, String databaseName, String jsonStructure) {
        Table table = new Table(tableName, databaseName);
        dbmsUnit = table;
        boolean isStructureSet = table.setStructure(jsonStructure);
        boolean isTableSaved = table.save(false);
        return isStructureSet && isTableSaved;
    }

    @Override
    public String getTableData(String tableName, String databaseName) {
        Table table = new Table(tableName, databaseName);
        dbmsUnit = table;
        return table.getData();
    }

    @Override
    public boolean saveTableData(String tableName, String databaseName, String jsonData, boolean rewrite) {
        Table table = new Table(tableName, databaseName);
        dbmsUnit = table;
        boolean isDataSet = table.readData() && table.setData(jsonData);
        boolean isTableSaved = isDataSet && table.save(rewrite);
        return isDataSet && isTableSaved;
    }

    @Override
    public boolean saveTable(String tableName, String databaseName, String jsonStructure, String jsonData, boolean rewrite) {
        Table table = new Table(tableName, databaseName);
        dbmsUnit = table;
        boolean isStructureSet = table.setStructure(jsonStructure);
        boolean isDataSet = table.setData(jsonData);
        boolean isTableSaved = table.save(rewrite);
        return isStructureSet && isDataSet && isTableSaved;
    }

    @Override
    public boolean deleteTable(String tableName, String databaseName) {
        Table table = new Table(tableName, databaseName);
        dbmsUnit = table;
        return table.delete();
    }

    @Override
    public String sort(String tableName, String databaseName, int columnNumber) {
        Table table = new Table(tableName, databaseName);
        dbmsUnit = table;
        return table.sort(columnNumber);
    }

    @Override
    public String getErrorMsg() {
        return dbmsUnit != null ? dbmsUnit.getErrorMsg() : "";
    }

    @Override
    public ExceptionType getExceptionType() {
        return dbmsUnit != null ? dbmsUnit.getExceptionType() : null;
    }

    @Override
    public boolean isNewError() {
        return dbmsUnit != null && dbmsUnit.isNewError();
    }
}
