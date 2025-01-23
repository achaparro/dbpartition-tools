package main.com.liferay.portal.db.partition.configuration;

public class ConfigurationProperties {

    public enum Scope {

        COMPANY("companyId", "company"),
        GROUP("groupId", "group"),
        PORTLET_INSTANCE("portletInstanceId", "portlet-instance");

        public String getPropertyKey() {
            return _propertyKey;
        }

        @Override
        public String toString() {
            return _value;
        }

        private Scope(String propertyKey, String value) {

            _propertyKey = propertyKey;
            _value = value;
        }

        private final String _propertyKey;
        private final String _value;

    }
}
