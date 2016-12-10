package com.carbonat.common;

import com.carbonat.database.Database;
import com.carbonat.databases.Databases;
import com.carbonat.dbms.DBMSImpl;
import com.carbonat.table.Table;
import com.carbonat.user.User;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.carbonat.table.DataType;

import javax.xml.ws.Endpoint;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.regex.Pattern;

public class Main {
    public final static String USERS_FILE = "users.json";
    public final static String DB_FILE = "databases.json";
    public final static String TABLESLIST_FILE = "tableslist.json";
    public final static String TABLE_STRUCTURE_FILE = "structure.json";
    public final static String DATA_FILE = "data.json";
    private static final boolean IS_WINDOWS = System.getProperty("os.name").contains("indow");

    private static ErrorMsg errorMsg;

    public static ErrorMsg getErrorMsg() {
        return errorMsg;
    }

    public static String getPath(String filename) {
        URL url = Main.class.getClassLoader().getResource(filename);
        String path;
        if (url == null) {
            path = Main.class.getClassLoader().getResource("").getPath() + filename;
        } else {
            path = Main.class.getClassLoader().getResource(filename).getPath();
        }
        String str = "";
        try {
            str = URLDecoder.decode(path, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            errorMsg = new ErrorMsg(ExceptionType.UnsupportedEncoding, ex.getMessage());
        }

        return IS_WINDOWS ? str.substring(1) : str;
    }

    public static Object readParseJson(String path) {
        try {
            String text = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
            JSONParser parser = new JSONParser();
            return parser.parse(text);
        } catch (IOException ex) {
            errorMsg = new ErrorMsg(ExceptionType.InputOutput, "Cannot read file " + Main.DB_FILE + "\n" + ex);
        } catch (ParseException ex) {
            errorMsg = new ErrorMsg(ExceptionType.JsonParse, "Cannot parse data in file " + Main.DB_FILE + "\n" + ex);
        }
        return null;
    }

    public static boolean createFileFolder(String filepath) {
        File targetFile = new File(filepath);
        File parent = targetFile.getParentFile();
        if (!parent.exists() && !parent.mkdirs()) {
            return false;
        }
        return true;
    }

    public static boolean updateJson(String jsonString, String path) {
        try (FileWriter file = new FileWriter(path)) {
            file.write(jsonString);
        } catch (IOException ex) {
            errorMsg = new ErrorMsg(ExceptionType.InputOutput, "Problem with writing data to " + path + "\n" + ex);
            return false;
        }
        return true;
    }

    public static boolean dataTypeTest(Object data, DataType dataType) {
        if (dataType == DataType.INTEGER && (data instanceof Integer || data instanceof Long))
            return true;
        if (dataType == DataType.REAL && (data instanceof Double || data instanceof Integer || data instanceof Long))
            return true;
        if (dataType == DataType.CHARACTER)
            return Pattern.matches(".?", (String) data);
        if (dataType == DataType.TEXT_FILE && data instanceof String)
            return true;
        if (dataType == DataType.INTEGER_INVL) {
            String integer = "([+|-]?[1-9]{1}[\\d]*|[0]{1})";
            return Pattern.matches("^" + integer + "[.]{2}" + integer + "$", (String) data);
        }
        return false;
    }

    public static int[] parseIntInvl(String data) {
        if (!dataTypeTest(data, DataType.INTEGER_INVL))
            return null;
        String vals[] = data.split("[.]{2}");
        int[] res = new int[2];
        res[0] = Integer.parseInt(vals[0]);
        res[1] = Integer.parseInt(vals[1]);
        return res;
    }

    public static double convertToDouble(Object real) {
        if (real instanceof Integer) {
            return (Integer) real;
        } else if (real instanceof Long) {
            return (Long) real;
        } else {
            return (Double) real;
        }
    }

    public static Object parseString(Object str, DataType dataType) {
        if (!(str instanceof String))
            return str;
        try {
            if (dataType == DataType.INTEGER) {
                int val = Integer.parseInt((String) str);
                return val;
            }
            if (dataType == DataType.REAL) {
                double val = Double.parseDouble((String) str);
                return val;
            }
            return str;
        } catch (Exception ex) {
            return null;
        }
    }

    public static Table parseJsonTableStructure(String tableName, String databaseName, String jsonStructure) {
        JSONParser parser = new JSONParser();
        JSONArray structure;
        try {
            structure = (JSONArray) parser.parse(jsonStructure);
        } catch (ParseException ex) {
            errorMsg = new ErrorMsg(ExceptionType.JsonParse, ex.getMessage());
            return null;
        }

        JSONObject obj;
        String type;
        int size = structure.size();
        DataType[] dataTypes = new DataType[size];
        String[] columnsNames = new String[size];

        for (int i = 0; i < size; ++i) {
            obj = (JSONObject) structure.get(i);
            columnsNames[i] = (String) obj.keySet().iterator().next();
            type = (String) obj.get(columnsNames[i]);
            try {
                dataTypes[i] = DataType.valueOf(type);
            } catch (IllegalArgumentException ex) {
                errorMsg = new ErrorMsg(ExceptionType.IllegalArgument, ex.getMessage());
                return null;
            }
        }

        return new Table(tableName, databaseName, dataTypes, columnsNames);
    }

    public static String arrayToJson(String[] array) {
        JSONArray jsonArray = new JSONArray();
        Collections.addAll(jsonArray, array);
        return jsonArray.toJSONString();
    }

    public static String array2ToJson(String[][] array) {
        JSONArray jsonArray = new JSONArray();

        for (String[] row : array) {
            JSONArray tmp = new JSONArray();
            Collections.addAll(tmp, row);
            jsonArray.add(tmp);
        }

        return jsonArray.toJSONString();
    }

    public static void main(String[] args) {
        int port = 7777;

        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        String address = "http://0.0.0.0:" + port + "/";
        Endpoint.publish(address + "dbms", new DBMSImpl());
        System.out.println("Server is ready");
    }
}
