# Microsoft SQL Server DB Writer

This component writes data to a Microsoft SQL Server database.

**Azure DB Connection** 

The Azure SQL Server must have the SQL authentication method enabled, an SQL login created according to the [documentation](https://help.keboola.com/components/writers/database/mssql/), and a firewall rule allowing communication from our IPs.

## Example Configuration

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

## Binary Types

Binary types `binary`, `varbinary`, `image` are converted as follows:
- If a CSV value starts with `0x` and its length is an even number, it is treated as a HEX value.
   - Example: `"0xabcdef"` is converted to the binary value `0xabcdef`.
- Otherwise, the value is treated as a string, and MS SQL Server adds a `\0` separator after each character.
  - Example: `"0xabcde"` is converted to the binary value `0\0x\0a\0b\0c\0d\0e`.
  - Example: `"dog"` is converted to the binary value `d\0o\0g\0`.
   
## Development

1. Install dependencies:

        `docker-compose run --rm dev composer install`
    
2. Write code and run tests in development mode to see what you did:

        `docker-compose run --rm dev composer ci`
    

