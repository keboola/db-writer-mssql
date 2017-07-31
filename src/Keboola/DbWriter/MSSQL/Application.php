<?php
/**
 * Author: miro@keboola.com
 * Date: 03/04/2017
 */
namespace Keboola\DbWriter\MSSQL;

use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Writer\MSSQL;

class Application extends \Keboola\DbWriter\Application
{
    public function runAction()
    {
        $uploaded = [];
        $tables = array_filter($this['parameters']['tables'], function ($table) {
            return ($table['export']);
        });

        /** @var MSSQL $writer */
        $writer = $this['writer'];
        foreach ($tables as $table) {
            try {
                $targetTableExists = $writer->checkTargetTable($table);

                $csv = $this->getInputCsv($table['tableId']);

                $targetTableName = $table['dbName'];

                if ($table['incremental']) {
                    $table['dbName'] = $writer->generateTmpName($table);
                }

                $table['items'] = $this->reorderColumns($csv, $table['items']);

                if (empty($table['items'])) {
                    continue;
                }

                $writer->drop($table['dbName']);
                $writer->write($csv, $table);

                if ($table['incremental']) {
                    // create target table if not exists
                    if (!$targetTableExists) {
                        $destinationTable = $table;
                        $destinationTable['dbName'] = $targetTableName;
                        $destinationTable['incremental'] = false;
                        $writer->create($destinationTable);
                    }
                    $writer->upsert($table, $targetTableName);
                }
            } catch (UserException $e) {
                $this->handleUserException($e);
            } catch (\PDOException $e) {
                $this->handleUserException(new UserException($e->getMessage(), 400, $e));
            } catch (\Exception $e) {
                throw new ApplicationException($e->getMessage(), 500, $e, [
                    'errFile' => $e->getFile(),
                    'errLine' => $e->getLine()
                ]);
            }

            $uploaded[] = $table['tableId'];
        }

        return [
            'status' => 'success',
            'uploaded' => $uploaded
        ];
    }

    private function handleUserException(\Exception $e)
    {
        /** @var Logger $logger */
        $logger = $this['logger'];
        $logger->error($e->getMessage());
        throw $e;
    }
}
