# Microsoft SQL Server DB Writer

Writes data to Microsoft SQL Server Database.

**The Azure Db connection** The Azure SQL server must have enabled the SQL authentication method, SQL login created according to the [documentation](https://help.keboola.com/components/writers/database/mssql/), and SQL server firewall has the rule to allow the communication from our IPs.

## Example configuration

```json
    {
      "db": {        
        "host": "HOST",
        "port": "PORT",
        "database": "DATABASE",
        "user": "USERNAME",
        "password": "PASSWORD",
        "ssh": {
          "enabled": true,
          "keys": {
            "private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tables": [
        {
          "tableId": "simple",
          "dbName": "dbo.simple",
          "export": true, 
          "incremental": true,
          "primaryKey": ["id"],
          "items": [
            {
              "name": "id",
              "dbName": "id",
              "type": "int",
              "size": null,
              "nullable": null,
              "default": null
            },
            {
              "name": "name",
              "dbName": "name",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            },
            {
              "name": "glasses",
              "dbName": "glasses",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            }
          ]                                
        }
      ]
    }
```

## Binary types

Binary types `binary`, `varbinary`, `image` are converted as follows:
- If CSV value starts with `0x` AND length of the value is even number, then it is a HEX value.
   - Example: `"0xabcdef"` is converted to binary value `0xabcdef`.
- Else, the value is treated as a string, MsSQL adds separator `\0` after each char code.
  - Example: `"0xabcde"` is converted to binary value `0\0x\0a\0b\0c\0d\0e`.
  - Example: `"dog"` is converted to binary value `d\0o\0g\0`.
   
## Development

1. Install dependencies

        docker-compose run --rm dev composer install
    
2. Write some code and run tests in dev mode to see what you did:

        docker-compose run --rm dev composer ci
    

