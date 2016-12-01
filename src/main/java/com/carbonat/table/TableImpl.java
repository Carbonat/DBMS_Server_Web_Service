package com.carbonat.table;

import com.carbonat.common.ErrorMsg;
import com.carbonat.common.ExceptionType;
import com.carbonat.common.Main;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.jws.WebService;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@WebService(endpointInterface = "com.carbonat.table.Table")
@XmlRootElement(name = "response")
@XmlAccessorType(XmlAccessType.FIELD)
public class TableImpl implements Table {
    private String name;
    private String databaseName;

    private ErrorMsg errorMsg;

    private DataType[] dataTypes;
    private String[] columnsNames;

    private ArrayList<ArrayList<Object>> data;

    // save result of sorting and connection
    private TableImpl buffer;

    public TableImpl(String name, String databaseName) {
        this.name = name;
        this.databaseName = databaseName;
    }

    public TableImpl(String name, String databaseName, DataType[] columnsDataTypes,
                     String[] columnsNames) {
        this.name = name;
        this.databaseName = databaseName;
        this.dataTypes = columnsDataTypes;
        this.columnsNames = columnsNames;
    }

    public TableImpl(String name, String databaseName, DataType[] columnsDataTypes,
                     String[] columnsNames, ArrayList<ArrayList<Object>> data) {
        this.name = name;
        this.databaseName = databaseName;
        this.dataTypes = columnsDataTypes;
        this.columnsNames = columnsNames;
        this.data = new ArrayList<>();
        for (ArrayList<Object> row : data) {
            this.addRow(row);
        }
    }

    public TableImpl(TableImpl table) {
        this.name = table.name;
        this.databaseName = table.databaseName;
        if (table.dataTypes != null)
            this.dataTypes = Arrays.copyOf(table.dataTypes, table.dataTypes.length);
        if (table.columnsNames != null)
            this.columnsNames = Arrays.copyOf(table.columnsNames, table.columnsNames.length);

        int size = table.data.size();
        this.data = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            this.data.add(new ArrayList<>(table.data.get(i)));
        }
    }

    // swap(list[i1], list[i2])
    private static void swap(ArrayList<ArrayList<Object>> list, int i1, int i2) {
        ArrayList<Object> row = list.get(i2);
        ArrayList<Object> tmp = list.get(i1);
        list.set(i1, row);
        list.set(i2, tmp);
    }

    private static void addDefaultValues(List<Object> list, DataType[] dataTypes, int i) {
        for (int j = 0; j < dataTypes.length; ++j) {
            if (j != i) {
                DataType dataType = dataTypes[j];

                if (dataType == DataType.INTEGER) {
                    list.add(Main.DEFAULT_INT);
                } else if (dataType == DataType.REAL) {
                    list.add(Main.DEFAULT_REAL);
                } else if (dataType == DataType.CHARACTER) {
                    list.add(Main.DEFAULT_CHAR);
                } else if (dataType == DataType.TEXT_FILE) {
                    list.add(Main.DEFAULT_TEXT_FILE);
                } else if (dataType == DataType.INTEGER_INVL) {
                    list.add(Main.DEFAULT_INTEGER_INVL);
                }
            }
        }
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
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public DataType[] getDataTypes() {
        columnsNames = null;
        dataTypes = null;
        if (!readStructure())
            return null;

        return dataTypes;
    }

    @Override
    public String getColumnsNames() {
        columnsNames = null;
        dataTypes = null;
        if (!readStructure())
            return "";
        return Main.arrayToJson(columnsNames);
    }

    @Override
    public boolean setStructure(String jsonStructure) {
        TableImpl table = Main.parseJsonTableStructure(this.name, this.databaseName, jsonStructure);

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

    @Override
    public String getData() {
        if (data == null && !readData()) {
            return "";
        }
        return dataToJson();
    }

    @Override
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

        ArrayList<Object> row;

        JSONArray jsonRow;
        int size = array.size();
        TableImpl tmpTable = new TableImpl("", "", dataTypes, columnsNames);
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

    @Override
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

    @Override
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

    @Override
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

    @Override
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

    @Override
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
        TableImpl table = Main.parseJsonTableStructure(this.name, this.databaseName, structure);
        if (table == null) {
            errorMsg = new ErrorMsg(ExceptionType.JsonParse, "");
            return false;
        }

        this.dataTypes = table.dataTypes;
        this.columnsNames = table.columnsNames;
        return true;
    }

    @Override
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

    @Override
    public boolean sort(int i, String name) {
        if (i > dataTypes.length || i < 0)
            return false;

        TableImpl table = new TableImpl(name, this.databaseName);
        table.dataTypes = this.dataTypes;
        table.columnsNames = this.columnsNames;

        int size = this.data.size();
        table.data = new ArrayList<>(size);
        for (int j = 0; j < size; ++j) {
            table.data.add(new ArrayList<>(this.data.get(j)));
        }

        table.qsort(0, size - 1, i);
        buffer = table;
        return true;
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

    private boolean uniqueColumn(int columnN) {
        HashSet<Object> map = new HashSet<>();
        Object key;
        for (int i = 0; i < data.size(); ++i) {
            key = data.get(i).get(columnN);
            if (!map.add(key))
                return false;
        }
        return true;
    }

    @Override
    public boolean connectTo(String table2Name, int columnN1, int columnN2, String newTableName) {
        TableImpl table2 = new TableImpl(table2Name, this.databaseName);
        if (!table2.readData())
            return false;

        if (this.dataTypes[columnN1] != table2.dataTypes[columnN2])
            return false;

        if (!table2.uniqueColumn(columnN2))
            return false;

        int size = this.dataTypes.length + table2.dataTypes.length - 1;
        DataType[] dataTypes = new DataType[size];
        String[] columnsNames = new String[size];
        int i = 0;
        for (int j = 0; j < this.dataTypes.length; ++j) {
            dataTypes[i] = this.dataTypes[j];
            columnsNames[i] = this.columnsNames[j];
            ++i;
        }
        for (int j = 0; j < table2.dataTypes.length; ++j) {
            if (j != columnN2) {
                dataTypes[i] = table2.dataTypes[j];
                columnsNames[i] = table2.columnsNames[j];
                ++i;
            }
        }

        ArrayList<ArrayList<Object>> list1, list2, list3;
        list1 = this.data;
        list2 = table2.data;
        list3 = new ArrayList<>();
        boolean[] usedRows = new boolean[list2.size()];
        Arrays.fill(usedRows, false);

        ArrayList<Object> row3;
        for (int i1 = 0; i1 < list1.size(); ++i1) {
            Object val1 = list1.get(i1).get(columnN1);
            row3 = new ArrayList<>(list1.get(i1));

            for (int i2 = 0; i2 < list2.size(); ++i2) {
                if (!usedRows[i2]) {
                    Object val2 = list2.get(i2).get(columnN2);

                    if (val1.equals(val2)) {
                        usedRows[i2] = true;
                        List<Object> tmpList = list2.get(i2);
                        row3.addAll(tmpList.subList(0, columnN2));
                        row3.addAll(tmpList.subList(columnN2 + 1, tmpList.size()));
                        break;
                    }
                }
            }

            if (row3.size() == list1.get(i1).size()) {
                addDefaultValues(row3, table2.dataTypes, columnN2);
            }
            list3.add(row3);
        }

        buffer = new TableImpl(newTableName, this.databaseName, dataTypes, columnsNames, list3);
        return true;
    }

    @Override
    public boolean saveBuffer() {
        if (buffer != null) {
            if (buffer.save(false)) {
                return true;
            }
            errorMsg = buffer.errorMsg;
        }
        return false;
    }

    @Override
    public DataType[] getDataTypesBuffer() {
//        String result = "";
//        if (buffer != null) {
//            result = buffer.getDataTypes();
//            if (result.isEmpty()) {
//                errorMsg = buffer.errorMsg;
//            }
//        }
//        return result;
        return buffer.dataTypes;
    }

    @Override
    public String getDataBuffer() {
        String result = "";
        if (buffer != null) {
            result = buffer.dataToJson();
            if (result.isEmpty()) {
                errorMsg = buffer.errorMsg;
            }
        }
        return result;
    }

    @Override
    public String getColumnsNamesBuffer() {
        String result = "";
        if (buffer != null) {
            result = buffer.getColumnsNames();
            if (result.isEmpty()) {
                errorMsg = buffer.errorMsg;
            }
        }
        return result;
    }

    @Override
    public void setTableAndBufferNull() {
        name = "";
        databaseName = "";
        columnsNames = null;
        dataTypes = null;
        data = null;
        errorMsg = null;
        buffer = null;
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