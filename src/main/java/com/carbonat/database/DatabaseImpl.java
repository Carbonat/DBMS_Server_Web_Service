package com.carbonat.database;

import com.carbonat.common.ErrorMsg;
import com.carbonat.common.ExceptionType;
import com.carbonat.common.Main;
import com.carbonat.table.DataType;
import com.carbonat.table.Table;
import com.carbonat.table.TableImpl;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;

import javax.jws.WebService;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@WebService(endpointInterface = "com.carbonat.database.Database")
public class DatabaseImpl implements Database {
    private String name;
    private String[] tables;

    private ErrorMsg errorMsg;

    public DatabaseImpl(String name) {
        super();
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getTables() {
        String result = "";
        if (loadTablesList()) {
            result = Main.arrayToJson(tables);
        }

        return result;
    }

    @Override
    public int uniqueName() {
        String path = Main.getPath(Main.DB_FILE);
        JSONArray dbnames = (JSONArray) Main.readParseJson(path);
        if (dbnames == null) {
            errorMsg = Main.getErrorMsg();
            return 0;
        }
        if (dbnames.indexOf(name) != -1)
            return 1;
        return 2;
    }

    @Override
    public boolean save() {
        // check database name unicity
        int unicity = uniqueName();
        if (unicity == 1) {
            errorMsg = new ErrorMsg(ExceptionType.FileAlreadyExists, name);
            return false;
        } else if (unicity == 2) {
            // create database folder
            String path = Main.getPath(name + "/" + Main.TABLESLIST_FILE);
            if (!Main.createFileFolder(path)) {
                errorMsg = new ErrorMsg(ExceptionType.FileAlreadyExists, name);
                return false;
            }

            // create table list file with empty json array
            JSONArray array = new JSONArray();
            if (!Main.updateJson(array.toJSONString(), path)) {
                errorMsg = Main.getErrorMsg();
                return false;
            }

            // add database name to list of databases
            String generalListPath = Main.getPath(Main.DB_FILE);
            JSONArray databases = (JSONArray) Main.readParseJson(generalListPath);
            if (databases != null) {
                databases.add(name);
                if (Main.updateJson(databases.toJSONString(), generalListPath)) {
                    return true;
                }
            } else {
                try {
                    FileUtils.deleteDirectory((new File(path)).getParentFile());
                } catch (IOException ex) {
                    errorMsg = new ErrorMsg(ExceptionType.InputOutput, ex.getMessage());
                }
            }
        }
        return false;
    }

    @Override
    public boolean delete() {
        // delete folder for current database
        String path = Main.getPath(name);
        try {
            FileUtils.deleteDirectory(new File(path));
        } catch (IOException ex) {
            errorMsg = new ErrorMsg(ExceptionType.InputOutput, ex.getMessage());
            return false;
        }

        // delete database from list of databases
        String databasesPath = Main.getPath(Main.DB_FILE);
        JSONArray databases = (JSONArray) Main.readParseJson(databasesPath);
        if (databases != null) {
            databases.remove(name);
            if (!Main.updateJson(databases.toJSONString(), databasesPath)) {
                errorMsg = Main.getErrorMsg();
                return false;
            }
        }

        return true;
    }

    private boolean loadTablesList() {
        // read list of tables
        String tablesListPath = Main.getPath(name + "/" + Main.TABLESLIST_FILE);
        JSONArray tablesList = (JSONArray) Main.readParseJson(tablesListPath);
        if (tablesList == null) {
            errorMsg = Main.getErrorMsg();
            return false;
        }

        int size = tablesList.size();
        tables = new String[size];
        for (int i = 0; i < size; ++i) {
            tables[i] = (String) tablesList.get(i);
//                if (!tables.get(i).readData())
//                    return false;
        }
        return true;
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
