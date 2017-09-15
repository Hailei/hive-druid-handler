package org.apache.hadoop.hive.druid.io;

import java.util.HashMap;
import java.util.Map;


public enum AGGTYPE {
    LONGSUM("longsum"),
    DOUBLESUM("doublesum"),
    HYPERUNIQUE("hyperunique"),
	COUNT("count");
    
    private String text;
    
    AGGTYPE(String text) {
        this.text = text;
    }
    
    public String getText() {
        return this.text;
    }
    
    // Implementing a fromString method on an enum type
    private static final Map<String,AGGTYPE>  stringToEnum = new HashMap<String,AGGTYPE>();
    static {
        // Initialize map from constant name to enum constant
        for(AGGTYPE agg : values()) {
            stringToEnum.put(agg.toString(), agg);
        }
    }
    
    // Returns Blah for string, or null if string is invalid
    public static AGGTYPE fromString(String symbol) {
        return stringToEnum.get(symbol);
    }

    @Override
    public String toString() {
        return text;
    }
}
