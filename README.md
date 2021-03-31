# dbpartition-tools
Tools to use DBPartition in Liferay

## migrate-to-dbpartition.jar
This tool migrates your companies from a regular environment to a DB Partition environment. It creates one schema per company except the default one. Aftert that, you can startup your server with db partition enabled.

- Requirements:
  - MySQL
  - Database user with DDL privileges
  - Liferay server must be down when executing it

- Example of usage:
```
java -jar migrate-to-dbpartition.jar {current_schema_name} {db_user} {db_password}
```

## validate-schemas.jar
This tool validates DB Partition schemas to be sure that all of them only contains data associated to the proper companyId

- Requirements:
  - MySQL
  - Database user with DDL privileges

 - Example of usage (optionally you can add the schema prefix):
```
java -jar validate-schemas.jar {current_schema_name} {db_user} {db_password} [{schema_prefix}]
```


