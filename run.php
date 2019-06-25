<?php

declare(strict_types=1);

use Keboola\DbWriter\MSSQL\Application;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\MSSQL\Configuration\ConfigDefinition;
use Monolog\Handler\NullHandler;

require_once(dirname(__FILE__) . '/vendor/autoload.php');

$logger = new Logger('wr-db-mssql');

$action = 'run';

try {
    $arguments = getopt('d::', ['data::']);
    if (!isset($arguments['data'])) {
        throw new UserException('Data folder not set.');
    }
    $config = json_decode(file_get_contents($arguments['data'] . '/config.json'), true);
    $config['parameters']['data_dir'] = $arguments['data'];
    $config['parameters']['writer_class'] = 'MSSQL';

    $action = isset($config['action']) ? $config['action'] : $action;

    $app = new Application($config, $logger, new ConfigDefinition());

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers(array(new NullHandler(Logger::INFO)));
    }

    echo $app->run();
} catch (UserException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());

    if ($action !== 'run') {
        echo $e->getMessage();
    }

    exit(1);
} catch (ApplicationException $e) {
    $logger->log('error', $e->getMessage(), [
        'data' => $e->getData(),
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'trace' => $e->getTrace(),
    ]);
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch (\Throwable $e) {
    $logger->log('error', $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'trace' => $e->getTrace(),
    ]);
    exit(2);
}
exit(0);
