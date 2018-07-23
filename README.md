# Microsoft SQL Server DB Writer

[![Docker Repository on Quay](https://quay.io/repository/keboola/db-writer-mssql/status "Docker Repository on Quay")](https://quay.io/repository/keboola/db-writer-mssql)
[![Build Status](https://travis-ci.org/keboola/db-writer-mssql.svg?branch=master)](https://travis-ci.org/keboola/db-writer-mssql)
[![Code Climate](https://codeclimate.com/github/keboola/db-writer-mssql/badges/gpa.svg)](https://codeclimate.com/github/keboola/db-writer-mssql)
[![Test Coverage](https://codeclimate.com/github/keboola/db-writer-mssql/badges/coverage.svg)](https://codeclimate.com/github/keboola/db-writer-mssql/coverage)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/keboola/db-writer-mssql/blob/master/LICENSE.md)

Writes data to Microsoft SQL Server Database.

**When connecting to an Azure** database and using SSH tunnel, the username must be in format `<username>@<databasename>` instead of just `<username>` otherwise it will throw errors.

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

## Development

1. Generate SSH key pair for SSH proxy:

        source ./vendor/keboola/db-writer-common/tests/generate-ssh-keys.sh
    
2. Write some code and run tests in dev mode to see what you did:

        docker-compose run --rm dev composer ci
    

