package main.com.liferay.portal.db.partition.configuration;

public class ConfigurationProperties {

    public enum Scope {

        COMPANY("companyWebId", "companyId", "company"),
        GROUP("groupKey", "groupId", "group"),
        PORTLET_INSTANCE(
                "portletInstanceKey", "portletInstanceId", "portlet-instance"),
        SYSTEM(null, null, "system");

        public static Scope getScope(String value) {
            for (Scope scope : values()) {
                if (scope._value.equals(value)) {
                    return scope;
                }
            }

            throw new IllegalArgumentException("Invalid value " + value);
        }

        public boolean equals(Scope scope) {
            return equals(scope.getValue());
        }

        public boolean equals(String value) {
            return _value.equals(value);
        }

        public String getDelimiterString() {
            return _SEPARATOR +  name() + _SEPARATOR;
        }

        public String getPortablePropertyKey() {
            return _portablePropertyKey;
        }

        public String getPropertyKey() {
            return _propertyKey;
        }

        public String getValue() {
            return _value;
        }

        @Override
        public String toString() {
            return _value;
        }

        private Scope(
                String portablePropertyKey, String propertyKey, String value) {

            _portablePropertyKey = portablePropertyKey;
            _propertyKey = propertyKey;
            _value = value;
        }

        private static final String _SEPARATOR = "__";

        private final String _portablePropertyKey;
        private final String _propertyKey;
        private final String _value;

    }
}
