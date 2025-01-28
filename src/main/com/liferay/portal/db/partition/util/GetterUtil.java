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

    public static long get(String value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }

        return _parseLong(value.trim(), defaultValue);
    }

    public static String get(String value, String defaultValue) {
        if (value == null) {
            return defaultValue;
        }

        value = value.trim();

        if (value.indexOf('\r') != -1) {
            value = value.replaceAll(
                    "\r\n", "\n");
        }

        return value;
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

    private static long _parseLong(String value, long defaultValue) {
        int length = value.length();

        if (length <= 0) {
            return defaultValue;
        }

        int pos = 0;
        long limit = -Long.MAX_VALUE;
        boolean negative = false;

        char c = value.charAt(0);

        if (c < '0') {
            if (c == '-') {
                limit = Long.MIN_VALUE;
                negative = true;
            }
            else if (c != '+') {
                return defaultValue;
            }

            if (length == 1) {
                return defaultValue;
            }

            pos++;
        }

        long smallLimit = limit / 10;

        long result = 0;

        while (pos < length) {
            if (result < smallLimit) {
                return defaultValue;
            }

            c = value.charAt(pos++);

            if ((c < '0') || (c > '9')) {
                return defaultValue;
            }

            int number = c - '0';

            result *= 10;

            if (result < (limit + number)) {
                return defaultValue;
            }

            result -= number;
        }

        if (negative) {
            return result;
        }

        return -result;
    }

    public static final long DEFAULT_LONG = 0;

    public static final String DEFAULT_STRING = "";
}