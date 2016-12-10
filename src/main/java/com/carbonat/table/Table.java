package com.carbonat.table;

import com.carbonat.common.DBMSUnit;
import com.carbonat.common.ErrorMsg;
import com.carbonat.common.ExceptionType;
import com.carbonat.common.Main;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Table extends DBMSUnit {
    private String name;
    private String databaseName;

    private DataType[] dataTypes;
    private String[] columnsNames;

    private ArrayList<ArrayList<Object>> data;

    public Table(String name, String databaseName) {
        this.name = name;
        this.databaseName = databaseName;
    }

    public Table(String name, String databaseName, DataType[] columnsDataTypes,
                 String[] columnsNames) {
        this.name = name;
        this.databaseName = databaseName;
        this.dataTypes = columnsDataTypes;
        this.columnsNames = columnsNames;
    }

    // swap(list[i1], list[i2])
    private static void swap(ArrayList<ArrayList<Object>> list, int i1, int i2) {
        ArrayList<Object> row = list.get(i2);
        ArrayList<Object> tmp = list.get(i1);
        list.set(i1, row);
        list.set(i2, tmp);
    }

    public DataType[] getDataTypes() {
        columnsNames = null;
        dataTypes = null;
        if (!readStructure())
            return null;

        return dataTypes;
    }

    public String getColumnsNames() {
        columnsNames = null;
        dataTypes = null;
        if (!readStructure())
            return "";
        return Main.arrayToJson(columnsNames);
    }

    public boolean setStructure(String jsonStructure) {
        Table table = Main.parseJsonTableStructure(this.name, this.databaseName, jsonStructure);

        if (table == null) {
            errorMsg = Main.getErrorMsg();
            return false;
        }

        this.dataTypes = table.dataTypes;
        this.columnsNames = table.columnsNames;

        return true;
    }

    private String dataToJson() {
        int rowsNum = data.size();
        int columnsNum = columnsNames.length;
        String[][] dataArray = new String[rowsNum][columnsNum];

        for (int i = 0; i < rowsNum; ++i) {
            List<Object> row = data.get(i);
            for (int j = 0; j < columnsNum; ++j) {
                dataArray[i][j] = row.get(j).toString();
            }
        }
        return Main.array2ToJson(dataArray);
    }

    public String getData() {
        if (data == null && !readData()) {
            return "";
        }
        return dataToJson();
    }

    public boolean setData(String jsonData) {
        if (dataTypes == null) {
            errorMsg = new ErrorMsg(ExceptionType.IllegalArgument, "At first Set columns data types and than set data");
            return false;
        }

        JSONParser parser = new JSONParser();
        JSONArray array;
        try {
            array = (JSONArray) parser.parse(jsonData);
        } catch (ParseException ex) {
            errorMsg = new ErrorMsg(ExceptionType.JsonParse, ex.getMessage());
            return false;
        }

        JSONArray jsonRow;
        int size = array.size();
        Table tmpTable = new Table("", "", dataTypes, columnsNames);
        for (int i = 0; i < size; ++i) {
            jsonRow = (JSONArray) array.get(i);

            boolean isSuccess = tmpTable.addRow(jsonRow.toJSONString());
            if (!isSuccess) {
                if (tmpTable.errorMsg.getIsNew() &&
                        tmpTable.errorMsg.getExceptionType() == ExceptionType.ValueDoesNotCorrespondToDataType) {

                    int j = Integer.parseInt(tmpTable.errorMsg.getMsg());
                    errorMsg = new ErrorMsg(ExceptionType.ValueDoesNotCorrespondToDataType,
                            "Value at (" + i + ", " + j + ") does not correspond to column's data type " + dataTypes[j]);

                    return false;
                }
            }
        }
        data = tmpTable.data;
        return true;
    }

    private int uniqueTableName() {
        String path = Main.getPath(databaseName + "/" + Main.TABLESLIST_FILE);
        JSONArray dbnames = (JSONArray) Main.readParseJson(path);
        if (dbnames == null) {
            errorMsg = Main.getErrorMsg();
            return 0;
        }
        if (dbnames.indexOf(name) != -1)
            return 1;
        return 2;
    }

    public boolean addRow(String jsonRow) {
        if (dataTypes == null) {
            errorMsg = new ErrorMsg(ExceptionType.IllegalArgument, "Set columns data types");
            return false;
        }

        JSONParser parser = new JSONParser();
        JSONArray jsonArray;
        try {
            jsonArray = (JSONArray) parser.parse(jsonRow);
        } catch (ParseException ex) {
            errorMsg = new ErrorMsg(ExceptionType.JsonParse, ex.getMessage());
            return false;
        }

        int columnsNum = jsonArray.size();
        if (columnsNum != dataTypes.length) {
            errorMsg = new ErrorMsg(ExceptionType.SizesAreNotEqual,
                    "Number of data columns is not equal to number table columns");
            return false;
        }

        ArrayList<Object> row = new ArrayList<>();
        for (int i = 0; i < columnsNum; ++i) {
            String strValue = (String) jsonArray.get(i);

            Object value = Main.parseString(strValue, dataTypes[i]);
            if (value == null) {
                errorMsg = new ErrorMsg(ExceptionType.ValueDoesNotCorrespondToDataType,
                        Integer.toString(i));
                return false;
            }
            row.add(value);
        }
        return addRow(row);
    }

    private boolean addRow(ArrayList<Object> row) {
        if (row.size() != columnsNames.length) {
            errorMsg = new ErrorMsg(ExceptionType.SizesAreNotEqual,
                    "Columns number is not equal to number of row's columns");
            return false;
        }

        for (int i = 0; i < row.size(); ++i) {
            if (!Main.dataTypeTest(row.get(i), dataTypes[i])) {
                errorMsg = new ErrorMsg(ExceptionType.ValueDoesNotCorrespondToDataType,
                        row.get(i).toString() + " is not " + dataTypes[i]);
                return false;
            }
        }
        if (data == null) {
            data = new ArrayList<>();
        }
        return data.add(new ArrayList<>(row));
    }

    // after connection the tables that have similar columns
    // it change columnName to columnName_
    private void changeColumnsNamesUnique() {
        HashSet<String> set = new HashSet<>();
        for (int i = 0; i < columnsNames.length; ++i) {
            while (!set.add(columnsNames[i])) {
                columnsNames[i] += "_";
            }
        }
    }

    public boolean saveStructure() {
        changeColumnsNamesUnique();
        String structurePath = Main.getPath(databaseName + "/" + name + "/" + Main.TABLE_STRUCTURE_FILE);
        JSONArray array = new JSONArray();
        for (int i = 0; i < columnsNames.length; ++i) {
            JSONObject object = new JSONObject();
            object.put(columnsNames[i], dataTypes[i].toString());
            array.add(object);
        }
        String jsonString = array.toJSONString();

        if (!Main.updateJson(jsonString, structurePath)) {
            errorMsg = Main.getErrorMsg();
        }

        return true;
    }

    public boolean saveData() {
        String path = Main.getPath(databaseName + "/" + name + "/" + Main.DATA_FILE);
        JSONArray array = new JSONArray();
        if (data != null) {
            for (int i = 0; i < data.size(); ++i) {
                int size = data.get(i).size();
                JSONObject object = new JSONObject();
                for (int j = 0; j < size; ++j) {
                    object.put(columnsNames[j], data.get(i).get(j));
                }
                array.add(object);
            }
        }


        if (!Main.updateJson(array.toJSONString(), path)) {
            errorMsg = Main.getErrorMsg();
            return false;
        }

        return true;
    }

    public boolean save(boolean rewrite) {
        int unicity = uniqueTableName();
        if (unicity == 0) {
            return false;
        }
        if (!rewrite && unicity == 1) {
            errorMsg = new ErrorMsg(ExceptionType.FileAlreadyExists, name);
            return false;
        } else {
            String structurePath = Main.getPath(databaseName + "/" + name + "/" + Main.TABLE_STRUCTURE_FILE);
            JSONArray array = new JSONArray();

            if (!Main.createFileFolder(structurePath)) {
                errorMsg = new ErrorMsg(ExceptionType.FileAlreadyExists, name);
                return false;
            }
            if (!Main.updateJson(array.toJSONString(), structurePath)) {
                errorMsg = Main.getErrorMsg();
                return false;
            }

            String tablesListPath = Main.getPath(databaseName + "/" + Main.TABLESLIST_FILE);
            JSONArray tables = (JSONArray) Main.readParseJson(tablesListPath);
            if (tables != null) {
                if (!tables.contains(name))
                    tables.add(name);
                if (Main.updateJson(tables.toJSONString(), tablesListPath)
                        && saveStructure() && saveData()) {
                    return true;
                }
            } else {
                try {
                    FileUtils.deleteDirectory((new File(structurePath)).getParentFile());
                } catch (Exception ex) {
                    System.out.println(ex);
                }
            }
        }
        return false;
    }

    public boolean delete() {
        String path = Main.getPath(databaseName + "/" + name);
        try {
            FileUtils.deleteDirectory(new File(path));
        } catch (Exception ex) {
            System.out.println(ex);
            return false;
        }

        String tablesListPath = Main.getPath(databaseName + "/" + Main.TABLESLIST_FILE);
        JSONArray tablesList = (JSONArray) Main.readParseJson(tablesListPath);
        if (tablesList != null) {
            tablesList.remove(name);
            if (Main.updateJson(tablesList.toJSONString(), tablesListPath)) {
                return true;
            }
        }

        return false;
    }

    private boolean readStructure() {
        String path = Main.getPath(databaseName + "/" + name + "/" + Main.TABLE_STRUCTURE_FILE);
        String structure;
        try {
            structure = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            errorMsg = new ErrorMsg(ExceptionType.InputOutput, ex.toString());
            return false;
        }
        Table table = Main.parseJsonTableStructure(this.name, this.databaseName, structure);
        if (table == null) {
            errorMsg = new ErrorMsg(ExceptionType.JsonParse, "");
            return false;
        }

        this.dataTypes = table.dataTypes;
        this.columnsNames = table.columnsNames;
        return true;
    }

    public boolean readData() {
        if (!readStructure())
            return false;

        String path = Main.getPath(databaseName + "/" + name + "/" + Main.DATA_FILE);
        JSONArray array = (JSONArray) Main.readParseJson(path);
        if (array == null) {
            errorMsg = Main.getErrorMsg();
            return false;
        }

        data = new ArrayList<>();
        for (int i = 0; i < array.size(); ++i) {
            JSONObject jsonObject = (JSONObject) array.get(i);
            ArrayList<Object> row = new ArrayList<>();
            for (int j = 0; j < jsonObject.size(); ++j) {
                row.add(jsonObject.get(columnsNames[j]));
            }
            data.add(row);
        }

        return true;
    }

    public String sort(int i) {
        if (!readData()) {
            return "";
        }
        if (i > dataTypes.length || i < 0) {
            errorMsg = new ErrorMsg(ExceptionType.IllegalArgument,
                    "Column number i is not between 0 and " +  dataTypes.length);
            return "";
        }

        Table table = new Table(name, this.databaseName);
        if (!table.readData()) {
            return "";
        }

        table.dataTypes = this.dataTypes;
        table.columnsNames = this.columnsNames;

        int size = this.data.size();
        table.data = new ArrayList<>(size);
        for (int j = 0; j < size; ++j) {
            table.data.add(new ArrayList<>(this.data.get(j)));
        }

        table.qsort(0, size - 1, i);
        return table.getData();
    }

    private void qsort(int lo, int hi, int columnN) {
        if (lo < hi) {
            int p = partition(this.data, lo, hi, columnN);
            qsort(lo, p - 1, columnN);
            qsort(p + 1, hi, columnN);
        }
    }

    private int partition(ArrayList<ArrayList<Object>> list, int lo, int hi, int columnN) {
        Object pivot = list.get(hi).get(columnN);
        int i = lo;
        for (int j = lo; j < hi; ++j) {
            Object val = list.get(j).get(columnN);

            if (lessOrEqual(val, pivot, columnN)) {
                swap(list, i, j);
                ++i;
            }
        }
        swap(list, i, hi);
        return i;
    }

    private boolean lessOrEqual(Object obj1, Object obj2, int columnN) {
        DataType dataType = dataTypes[columnN];
        if (dataType == DataType.INTEGER) {
            long val1, val2;
            val1 = obj1 instanceof Integer ? (Integer) obj1 : (Long) obj1;
            val2 = obj2 instanceof Integer ? (Integer) obj2 : (Long) obj2;
            return val1 <= val2;
        }
        if (dataType == DataType.REAL) {
            double val1 = Main.convertToDouble(obj1);
            double val2 = Main.convertToDouble(obj2);
            return val1 <= val2;
        }
        if (dataType == DataType.CHARACTER || dataType == DataType.TEXT_FILE) {
            String val1 = (String) obj1;
            String val2 = (String) obj2;
            return val1.compareTo(val2) <= 0;
        } else {  // dataType == com.carbonat.table.DataType.INTEGER_INVL
            int[] vals1 = Main.parseIntInvl((String) obj1);
            int[] vals2 = Main.parseIntInvl((String) obj2);
            if (vals1 == null || vals2 == null)
                return false;
            return (vals1[1] - vals1[0]) <= (vals2[1] - vals2[0]);
        }
    }
}