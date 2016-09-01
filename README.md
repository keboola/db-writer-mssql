# Microsoft SQL Server DB Writer

[![Build Status](https://travis-ci.org/keboola/db-writer-mssql.svg?branch=master)](https://travis-ci.org/keboola/db-writer-mssql)

```json
    {
      "db": {
        "driver": "mssql",
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
          "primaryKey": [id],
          "items": [
            {
              "name": "id",
              "dbName": "id",
              "type": "int"
              "size": null
              "nullable": null
              "default": null
            },
            {
              "name": "name",
              "dbName": "name",
              "type": "nvarchar"
              "size": 255
              "nullable": null
              "default": null
            },
            {
              "name": "glasses",
              "dbName": "glasses",
              "type": "nvarchar"
              "size": 255
              "nullable": null
              "default": null
            }
          ]                                
        }
      ]
    }
```
