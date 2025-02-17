<?php

namespace Resque\Pool;

use Psr\Log\LoggerInterface;
use Resque\Worker\ResqueWorker;


/**
 * Worker Pool for php-resque-pool
 *
 * @package   Resque-Pool
 * @author    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @author    Michael Kuan <michael34435@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class Pool
{
    /**
     * @var list<int>
     */
    private static $QUEUE_SIGS = array(SIGQUIT, SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGCONT, SIGHUP, SIGWINCH, SIGCHLD);

    /**
     * @var Configuration
     */
    private $config;

    /**
     * @var LoggerInterface
     */
    private $logger;
    /**
     * @var Platform
     */
    private $platform;

    /**
     * @var array<string,array<int,true>>
     */
    private $workers = array();

    public function __construct(Configuration $config)
    {
        $this->config = $config;
        $this->logger = $config->logger;
        $this->platform = $config->platform;
    }

    /**
     * @return void
     */
    public function start()
    {
        $this->config->initialize();
        $this->procline('(starting)');
        $this->platform->trapSignals(self::$QUEUE_SIGS);
        $this->maintainWorkerCount();

        $this->procline('(started)');
        $this->logger->notice("started manager", ['process' => 'manager']);
        $this->reportWorkerPoolPids();
    }

    /**
     * @return void
     */
    public function join()
    {
        while (true) {
            $this->reapAllWorkers();
            if ($this->handleSignalQueue()) {
                break;
            }

            if (0 === $this->platform->numSignalsPending()) {
                $this->maintainWorkerCount();
                $this->platform->sleep($this->config->sleepTime);
            }

            $this->procline(sprintf("managing [%s]", implode(' ', $this->allPids())));
        }
        $this->procline("(shutting down)");
        $this->logger->notice("manager finished", ['process' => 'manager']);
    }

    /**
     * @return bool When true the pool manager must shut down
     */
    protected function handleSignalQueue()
    {
        switch ($signal = $this->platform->nextSignal()) {
        case null:
            break;
        case SIGUSR1:
        case SIGUSR2:
        case SIGCONT:
            $this->logger->notice("{$signal}: sending to all workers", ['process' => 'manager']);
            $this->signalAllWorkers($signal);
            break;
        case SIGHUP:
            $this->logger->notice("HUP: reload config file", ['process' => 'manager']);
            $this->config->resetQueues();
            $this->config->initialize();
            $this->logger->notice('HUP: gracefully shutdown old children (which have old logfiles open)', ['process' => 'manager']);
            $this->signalAllWorkers(SIGQUIT);
            $this->logger->notice('HUP: new children will inherit new logfiles', ['process' => 'manager']);
            $this->maintainWorkerCount();
            break;
        case SIGWINCH:
            if ($this->config->handleWinch) {
                $this->logger->notice('WINCH: gracefully stopping all workers', ['process' => 'manager']);
                $this->config->resetQueues();
                $this->maintainWorkerCount();
            }
            break;
        case SIGQUIT:
            $this->platform->setQuitOnExitSignal(true);
            $this->gracefulWorkerShutdownAndWait($signal);

            return true;
        case SIGINT:
            $this->gracefulWorkerShutdown($signal);

            return true;
        case SIGTERM:
            switch ($this->config->termBehavior) {
            case "graceful_worker_shutdown_and_wait":
                $this->gracefulWorkerShutdownAndWait($signal);
                break;
            case "graceful_worker_shutdown":
                $this->gracefulWorkerShutdown($signal);
                break;
            default:
                $this->shutdownEverythingNow($signal);
                break;
            }

            return true;
        }

        return false;
    }

    /**
     * @return void
     */
    public function reportWorkerPoolPids()
    {
        if (count($this->workers) === 0) {
            $this->logger->notice('Pool is empty', ['process' => 'manager']);
        } else {
            $pids = $this->allPids();
            $this->logger->notice("Pool contains worker PIDs: ".implode(', ', $pids), ['process' => 'manager']);
        }
    }

    /**
     * Creates or shuts down workers to match the configured worker counts.
     * @return void
     */
    public function maintainWorkerCount()
    {
        foreach ($this->allKnownQueues() as $queues) {
            $delta = $this->workerDeltaFor($queues);
            if ($delta > 0) {
                while ($delta-- > 0) {
                    $this->spawnWorker($queues);
                }
            } elseif ($delta < 0) {
                $pids = array_slice($this->pidsFor($queues), 0, -$delta);
                $this->platform->signalPids($pids, SIGQUIT);
            }
        }
    }

    /**
     * Finds and unsets dead workers.
     *
     * @param boolean $wait When true waits for all children to shutdown.
     * @return void
     */
    public function reapAllWorkers($wait = false)
    {
        while ($exited = $this->platform->nextDeadChild($wait)) {
            list($wpid, $exit) = $exited;
            $this->logger->notice("Reaped resque worker {$wpid} (status: {$exit}) queues: ". $this->workerQueues($wpid), ['process' => 'manager']);
            $this->deleteWorker($wpid);
        }
    }

    /**
     * @param int $pid
     * @return string|null The queues $pid was created to work on
     */
    public function workerQueues($pid)
    {
        foreach ($this->workers as $queues => $workers) {
            if (isset($workers[$pid])) {
                return $queues;
            }
        }

        return null;
    }

    /**
     * @return list<int> The pids of all living worker daemons
     */
    public function allPids()
    {
        if (!$this->workers) {
            return array();
        }

        $result = array();
        foreach ($this->workers as $queues) {
            $result[] = array_keys($queues);
        }

        return call_user_func_array('array_merge', $result); // @phpstan-ignore-line
    }

    /**
     * @return list<string>
     */
    public function allKnownQueues()
    {
        return array_unique(array_merge($this->config->knownQueues(), array_keys($this->workers)));
    }

    /**
     * @param int $signal
     * @return void
     */
    public function signalAllWorkers($signal)
    {
        $this->platform->signalPids($this->allPids(), $signal);
    }

    /**
     * @param int $signal
     * @return void
     */
    public function gracefulWorkerShutdownAndWait($signal)
    {
        $this->logger->notice("{$signal}: graceful shutdown, waiting for children", ['process' => 'manager']);
        $this->signalAllWorkers(SIGQUIT);
        $this->reapAllWorkers(true); // will hang until all workers are shutdown
    }

    /**
     * @param int $signal
     * @return void
     */
    public function gracefulWorkerShutdown($signal)
    {
        $this->logger->notice("{$signal}: immediate shutdown (graceful worker shutdown)", ['process' => 'manager']);
        $this->signalAllWorkers(SIGQUIT);
    }

    /**
     * @param int $signal
     * @return void
     */
    public function shutdownEverythingNow($signal)
    {
        $this->logger->notice("{$signal}: immediate shutdown (and immediate worker shutdown)", ['process' => 'manager']);
        $this->signalAllWorkers(SIGTERM);
    }

    /**
     * @param string $queues
     * @return int
     */
    protected function workerDeltaFor($queues)
    {
        $max = $this->config->workerCount($queues);
        $active = isset($this->workers[$queues]) ? count($this->workers[$queues]) : 0;

        return $max - $active;
    }

    /**
     * @param string $queues
     * @return int[]
     */
    protected function pidsFor($queues)
    {
        return isset($this->workers[$queues]) ? array_keys($this->workers[$queues]) : array();
    }

    /**
     * @param int $pid
     * @return void
     */
    protected function deleteWorker($pid)
    {
        foreach (array_keys($this->workers) as $queues) {
            if (isset($this->workers[$queues][$pid])) {
                unset($this->workers[$queues][$pid]);

                return ;
            }
        }
    }

    /**
     * NOTE: the only time resque code is ever loaded is *after* this fork.
     *       this way resque(and application) code is loaded per fork and
     *       will pick up changed files.
     * TODO: the other possibility here is to load all the resque(and possibly application)
     *       code pre-fork so that the copy-on-write functionality of the linux memory model
     *       can share the compiled code between workers.  Some investigation into the facts
     *       would be usefull
     * @param string $queues
     * @return void
     */
    protected function spawnWorker($queues)
    {
        $pid = $this->platform->pcntl_fork();
        if ($pid === -1) {
            $this->logger->notice('pcntl_fork failed', ['process' => 'manager']);
            $this->platform->_exit(1);
        } elseif ($pid === 0) {
            $this->platform->releaseSignals();
            /** @var ResqueWorker */
            $worker = $this->createWorker($queues);
            $this->logger->info("Starting worker {$worker}", ['process' => 'worker']);
            $this->procline("Starting worker {$worker}");
            $this->callAfterPrefork($worker);
            $worker->work($this->config->workerInterval);
            $this->platform->_exit(0);
        } else {
            $this->workers[$queues][$pid] = true;
        }
    }

    /**
     * @param object $worker
     * @return void
     */
    protected function callAfterPrefork($worker)
    {
        if ($callable = $this->config->afterPreFork) {
            call_user_func($callable, $this, $worker);
        }
    }

    /**
     * @param string $queues
     * @return object
     */
    protected function createWorker($queues)
    {
        $queues = explode(',', $queues);
        $class = $this->config->workerClass;
        $worker = new $class($queues);
        $worker->setLogger($this->config->workerLogger); // @phpstan-ignore-line

        return $worker;
    }

    /**
     * @param string $string
     * @return void
     */
    protected function procline($string)
    {
        $appName = $this->config->appName ? "[{$this->config->appName}]" : '';

        if (function_exists('setproctitle')) {
            setproctitle("resque-pool-manager{$appName}: {$string}");
        } elseif (function_exists('cli_set_process_title') && PHP_OS !== 'Darwin') {
            cli_set_process_title("resque-pool-manager{$appName}: {$string}");
        }
    }
}
