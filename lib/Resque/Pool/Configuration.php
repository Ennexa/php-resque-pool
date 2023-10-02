<?php

namespace Resque\Pool;

use Psr\Log\LogLevel;
use Symfony\Component\Yaml\Yaml,
    Symfony\Component\Yaml\Exception\ParseException;
use Psr\Log\LoggerInterface;
use Resque\Worker\ResqueWorker;

/**
 * Configuration manager for php-resque-pool
 *
 * @package   Resque-Pool
 * @author    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @author    Michael Kuan <michael34435@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class Configuration
{
    const LOG_NONE = 0;
    const LOG_NORMAL = 1;
    const LOG_VERBOSE = 2;

    const DEFAULT_WORKER_INTERVAL = 5;

    /**
     * @var null|callable
     */
    public $afterPreFork;
    /**
     * Tag used in log output
     *
     * @var null|string
     */
    public $appName;
    /**
     * Possible configuration file locations
     *
     * @var string[]
     */
    public $configFiles = array('resque-pool.yml', 'config/resque-pool.yml');
    /**
     * Environment to use from configuration
     * @var string
     */
    public $environment = 'dev';
    /**
     * Reset worker counts to 0 when SIGWINCH is received
     *
     * @var bool
     */
    public $handleWinch = false;
    /**
     * @var LoggerInterface
     */
    public $logger;
    /**
     * @var LogLevel::*
     */
    public $logLevel = LogLevel::WARNING;
    /**
     * @var Platform
     */
    public $platform;
    /**
     * Active configuration file location.  When null self::$configFiles will be tried.
     *
     * @var string|null
     */
    public $queueConfigFile;
    /**
     * @var integer
     */
    public $sleepTime = 60;
    /**
     * What to do when receiving SIGTERM
     *
     * @var string
     */
    public $termBehavior = '';
    /**
     * @var string
     */
    public $workerClass = ResqueWorker::class;
    /**
     * @var integer
     */
    public $workerInterval = self::DEFAULT_WORKER_INTERVAL;
    /**
     * @var LoggerInterface
     */
    public $workerLogger;

    /**
     * @var array<string,int>
     */
    protected $queueConfig;

    /**
     * @param array<string,int>|string|null $config   Either a configuration array, path to yml
     *                                      file containing config, or null
     * @param LoggerInterface|null          $logger   If not provided one will be instantiated
     * @param Platform|null                 $platform If not provided one will be instantiated
     * @param LoggerInterface|null          $workerLogger   If not provided one will be instantiated
     */
    public function __construct($config = null, $logger = null, $platform = null, $workerLogger = null)
    {
        $this->loadEnvironment();
        $this->logger = $logger ?: new Logger($this->appName, LogLevel::NOTICE);
        $this->workerLogger = $workerLogger ?: new Logger($this->appName, $this->logLevel);
        $this->platform = $platform ?: new Platform;

        if (is_array($config)) {
            $this->queueConfig = $config;
            $this->queueConfigFile = null;
        } elseif (is_string($config)) {
            $this->queueConfigFile  = $config;
        } elseif ($config !== null) { // @phpstan-ignore-line
            throw new \InvalidArgumentException('Unknown $config argument passed');
        }
    }

    /** @return void */
    public function initialize()
    {
        if (!$this->queueConfig) {
            $this->chooseConfigFile();
            $this->loadQueueConfig();
        }
        if ($this->environment && isset($this->queueConfig[$this->environment])) {
            $this->queueConfig = $this->queueConfig[$this->environment] + $this->queueConfig; // @phpstan-ignore-line
        }
        // filter out the environments
        $this->queueConfig = array_filter($this->queueConfig, 'is_integer');
        $this->logger->notice("Configured queues: " . implode(" ", $this->knownQueues()));
    }

    /**
     * @param string $queues
     *
     * @return integer Desired number of workers for specified queue combination
     */
    public function workerCount($queues)
    {
        return isset($this->queueConfig[$queues]) ? $this->queueConfig[$queues] : 0;
    }

    /**
     * @return string[] All configured queue combinations
     */
    public function knownQueues()
    {
        return $this->queueConfig ? array_keys($this->queueConfig) : array();
    }

    /**
     * @return array<string,int> Map of queue combination to desired worker count
     */
    public function queueConfig()
    {
        return $this->queueConfig;
    }

    /**
     * Resets the current queue configuration
     * @return void
     */
    public function resetQueues()
    {
        $this->queueConfig = array();
    }

    /**
     * @return void
     */
    protected function loadEnvironment()
    {
        $this->appName = basename(getcwd() ?: '.');
        $this->environment = (string)getenv('RESQUE_ENV');
        $this->workerInterval = (int)getenv('INTERVAL') ?: $this->workerInterval;
        $this->queueConfigFile = (string)getenv('RESQUE_POOL_CONFIG');

        if (getenv('VVERBOSE')) {
            $this->logLevel = LogLevel::DEBUG;
        } elseif (getenv('LOGGING') || getenv('VERBOSE')) {
            $this->logLevel = LogLevel::NOTICE;
        }
    }

    /**
     * @return void
     */
    protected function chooseConfigFile()
    {
        if ($this->queueConfigFile) {
            if (file_exists($this->queueConfigFile)) {
                return;
            }
            $this->logger->error("Chosen config file '{$this->queueConfigFile}' not found. Looking for others.");
        }
        $this->queueConfigFile = null;
        foreach ($this->configFiles as $file) {
            if (file_exists($file)) {
                $this->queueConfigFile = $file;
                break;
            }
        }
    }

    /**
     * @return void
     */
    protected function loadQueueConfig()
    {
        if ($this->queueConfigFile && file_exists($this->queueConfigFile)) {
            $this->logger->notice("Loading config file: {$this->queueConfigFile}");
            try {
                if (preg_match("/\.php/", $this->queueConfigFile)) {
                    ob_start();
                    include($this->queueConfigFile);
                    $queueConfig = (string)ob_get_clean();
                } else {
                    $queueConfig = (string)file_get_contents($this->queueConfigFile);
                }

                $this->queueConfig = Yaml::parse($queueConfig); // @phpstan-ignore-line
            } catch (ParseException $e) {
                $msg = "Invalid config file: ".$e->getMessage();
                $this->logger->error($msg);

                throw new \RuntimeException($msg, 0, $e);
            }
        }
        if (!$this->queueConfig) {
            $this->logger->error('No configuration loaded.');
            $this->queueConfig = array();
        }
    }
}
