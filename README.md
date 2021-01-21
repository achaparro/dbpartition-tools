# dbpartition-tools
Tools to use DBPartition in Liferay

## dbpartition-tools.jar
This tool migrates your companies from a regular environment to a DB Partition environment. It creates one schema per company except the default one. Aftert that, you can startup your server with db partition enabled.

- Example of usage:
```
java -jar dbpartition-tools.jar {current_schema_name} {db_user} {db_password}
```
