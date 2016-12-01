package com.carbonat.databases;

import com.carbonat.common.ErrorMsg;
import com.carbonat.common.ExceptionType;
import com.carbonat.common.Main;
import org.json.simple.JSONArray;

import javax.jws.WebService;

@WebService(endpointInterface = "com.carbonat.databases.Databases")
public class DatabasesImpl implements Databases {

    private String[] databases;

    private ErrorMsg errorMsg;

    private boolean readDatabases() {
        String path = Main.getPath(Main.DB_FILE);
        JSONArray array = (JSONArray) Main.readParseJson(path);
        if (array == null) {
            errorMsg = Main.getErrorMsg();
            return false;
        }
        int size = array.size();
        databases = new String[size];
        for (int i = 0; i < size; ++i) {
            databases[i] = (String) array.get(i);
        }
        return true;
    }

    @Override
    public String getDatabases() {
        String result = "";
        if (readDatabases()) {
            result = Main.arrayToJson(databases);
        }
        return result;
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
