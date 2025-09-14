package common;

import java.util.Collection;

public class JsonUtil {
    private JsonUtil(){} // prevent instantiation

    // Escape a string so it is safe for JSON values
    public static String escape(String s){
        if (s == null) return "";
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;   // escape quote
                case '\\': sb.append("\\\\"); break; // escape backslash
                case '\n': sb.append("\\n"); break;  // escape newline
                case '\r': sb.append("\\r"); break;  // escape carriage return
                case '\t': sb.append("\\t"); break;  // escape tab
                default:
                    if (c < 0x20) { // control character
                        sb.append(String.format("\\u%04x", (int)c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    // Combine multiple raw JSON objects into a JSON array string
    public static String joinObjectsToArray(Collection<String> rawObjects) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (String obj : rawObjects) {
            if (!first) sb.append(',');
            sb.append(obj == null ? "{}" : obj); // null safety
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }
}