package main.com.liferay.portal.db.partition.util;

public class GetterUtil {

    public static long get(Object value, long defaultValue) {
        if (value instanceof String) {
            return get((String)value, defaultValue);
        }

        if (value instanceof Long) {
            return (Long)value;
        }

        if (value instanceof Number) {
            Number number = (Number)value;

            return number.longValue();
        }

        return defaultValue;
    }

    public static String get(Object value, String defaultValue) {
        if (value instanceof String) {
            return get((String)value, defaultValue);
        }

        return defaultValue;
    }

    public static long getLong(Object value) {
        return getLong(value, DEFAULT_LONG);
    }

    public static long getLong(Object value, long defaultValue) {
        return get(value, defaultValue);
    }

    public static String getString(Object value) {
        return getString(value, DEFAULT_STRING);
    }

    public static String getString(Object value, String defaultValue) {
        return get(value, defaultValue);
    }

    public static final long DEFAULT_LONG = 0;

    public static final String DEFAULT_STRING = "";
}