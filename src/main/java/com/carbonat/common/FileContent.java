package com.carbonat.common;

import javax.jws.WebService;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileContent extends DBMSUnit {

    public String getText(String path) {
        String text = "";
        try {
            text = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            errorMsg = new ErrorMsg(ExceptionType.InputOutput, ex.getMessage());
        } catch (Exception ex) {
            errorMsg = new ErrorMsg(ExceptionType.InputOutput, ex.getMessage());
        }
        return text;
    }

}
