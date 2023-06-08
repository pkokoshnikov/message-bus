package org.pak.messagebus.core;

import org.apache.commons.text.StringSubstitutor;

import java.util.Map;

public class Utils {
    public static String format(String message, Map<String, String> args) {
        return StringSubstitutor.replace(message, args, "${", "}");
    }
}
