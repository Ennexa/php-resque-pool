<?php

namespace Resque\Pool;

use Psr\Log\AbstractLogger;
use Psr\Log\LoggerTrait;
use Psr\Log\LogLevel;


/**
 * Logger for php-resque-pool
 *
 * @package   Resque-Pool
 * @author    Erik Bernhardson <bernhardsonerik@gmail.com>
 * @author    Michael Kuan <michael34435@gmail.com>
 * @copyright (c) 2012 Erik Bernhardson
 * @license   http://www.opensource.org/licenses/mit-license.php
 */
class Logger extends AbstractLogger
{
    use LoggerTrait;

    /** @var array<string, int> */
    protected static $logLevels = [
        LogLevel::EMERGENCY => 0,
        LogLevel::ALERT     => 1,
        LogLevel::CRITICAL  => 2,
        LogLevel::ERROR     => 3,
        LogLevel::WARNING   => 4,
        LogLevel::NOTICE    => 5,
        LogLevel::INFO      => 6,
        LogLevel::DEBUG     => 7,
    ];

    /** @var LogLevel::* */
    protected $logLevel;

    /** @var string */
    private $appName;

    /**
     * @param null|string $appName
     * @param LogLevel::* $logLevel
     */
    public function __construct($appName = null, $logLevel = LogLevel::WARNING)
    {
        $this->appName = $appName ? "[{$appName}]" : "";
        $this->logLevel = $logLevel;
    }

    /**
     * @param LogLevel::* $level
     * @param string $message
     * @param mixed[] $context
     * @return void
     */
    public function log($level, $message, array $context = array())
    {
        if (!$this->shouldLog($level)) {
            return;
        }

        $formattedMessage = $this->formatMessage($level, $message, $context);

        fwrite(STDOUT, $formattedMessage . PHP_EOL);
    }

    /**
     * This function closes and re-opens the output log
     * @return void
     */
    public function rotate()
    {
        // not possible in php?
    }

    /**
     * @param LogLevel::* $level
     * @param string $message
     * @param mixed[] $context
     * @return string
     */
    protected function formatMessage($level, $message, array $context)
    {
        $pid = getmypid();
        $process = "resque-pool-{process}{$this->appName}[{$pid}]";
        $context += ['process' => 'worker'];

        // Interpolate context values into the message placeholders
        $replace = [];
        foreach ($context as $key => $val) {
            $replace['{' . $key . '}'] = $val;
        }

        return strtr($process . ' ' . $message, $replace);
    }

    /**
     * @param LogLevel::* $level
     * @return bool
     */
    protected function shouldLog($level)
    {
        return self::$logLevels[$level] <= self::$logLevels[$this->logLevel];
    }
}
