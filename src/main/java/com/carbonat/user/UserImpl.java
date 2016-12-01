package com.carbonat.user;

import com.carbonat.common.ErrorMsg;
import com.carbonat.common.ExceptionType;
import com.carbonat.common.Main;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.jws.WebService;

@WebService(endpointInterface = "com.carbonat.user.User")
public class UserImpl implements User {
    private String username;
    private String password;
    private UserType userType;

    private ErrorMsg errorMsg;

    public UserImpl(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public UserImpl(String username, String password, UserType userType) {
        this.username = username;
        this.password = password;
        this.userType = userType;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public UserType getUserType() {
        return userType;
    }

    @Override
    public void setUserType(UserType userType) {
        this.userType = userType;
    }

    @Override
    public UserType userExists() {
//        System.out.println("inside UserExists");
        UserType userType = UserType.UNKNOWN_USER;
        String path = Main.getPath(Main.USERS_FILE);
        JSONArray array = (JSONArray) Main.readParseJson(path);
        if (array != null) {
            JSONObject obj;
            String username, password, userT;

            boolean userFound = false;
            for (int i = 0; i < array.size() && !userFound; ++i) {
                obj = (JSONObject) array.get(i);
                username = (String) obj.get("username");
                password = (String) obj.get("password");
                if (this.getUsername().equals(username) &&
                        this.getPassword().equals(password)) {
                    userFound = true;
                    userT = (String) obj.get("UserType");
                    userType = UserType.valueOf(userT);
                }
            }
        }
        return userType;
    }

    @Override
    public boolean usernameIsUnique() {
        String path = Main.getPath(Main.USERS_FILE);
        JSONArray array = (JSONArray) Main.readParseJson(path);
        if (array != null) {
            JSONObject obj;
            String username;

            for (int i = 0; i < array.size(); ++i) {
                obj = (JSONObject) array.get(i);
                username = (String) obj.get("username");
                if (this.getUsername().equals(username)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean save() {
        if (usernameIsUnique()) {
            errorMsg = new ErrorMsg(ExceptionType.UserAlreadyExists, "");
            return false;
        }

        String path = Main.getPath(Main.USERS_FILE);
        JSONArray array = (JSONArray) Main.readParseJson(path);

        JSONObject object = new JSONObject();
        object.put("username", getUsername());
        object.put("password", getPassword());
        object.put("UserType", getUserType().toString());

        array.add(object);
        String jsonString = array.toJSONString();

        return Main.updateJson(jsonString, path);
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
