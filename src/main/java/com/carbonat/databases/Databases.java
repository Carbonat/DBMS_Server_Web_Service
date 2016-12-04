package com.carbonat.databases;

import com.carbonat.common.DBMSUnit;
import com.carbonat.common.ErrorMsg;
import com.carbonat.common.ExceptionType;
import com.carbonat.common.Main;
import org.json.simple.JSONArray;

import javax.jws.WebService;

public class Databases extends DBMSUnit {

    private String[] databases;

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

    public String getDatabases() {
        String result = "";
        if (readDatabases()) {
            result = Main.arrayToJson(databases);
        }
        return result;
    }
}
