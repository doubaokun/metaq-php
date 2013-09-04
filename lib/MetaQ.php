<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

namespace MetaQ;


require_once 'Consumer.php';
require_once 'Codec.php';
require_once 'Producer.php';
require_once 'Socket.php';
require_once 'MetaZookeeper.php';
require_once 'MetaQ_Exception.php';
require_once 'KLogger.php';

class MetaQ
{

    static $log;
    static $config;
    private $producer;
    private $consumer;
    private $topics;
    private $writeList;
    private $readList;

    public function __construct()
    {
        date_default_timezone_set("PRC");
        self::$log = \KLogger::instance(dirname(__FILE__). "/../log", \KLogger::DEBUG);
        require_once '../config/Config.php';
        $this->config = $METAQ_CONFIG;
        $this->initPartitionList();
    }

    private function initPartitionList()
    {
        $readList = $writeList = array();
        foreach ($this->config['brokers'] as $id => $broker) {
            if ($broker['role'] === 'master') {
                foreach ($broker['topics'] as $topic => $parts) {
                    if (!isset($writeList[$topic])) {
                        $writeList[$topic] = array();
                    }
                    foreach ($parts['partitions'] as $part) {
                        $writeList[$topic][] = $broker['host'] . '-' . $broker['port'] . '-' . $part;
                    }
                }
                foreach ($broker['topics'] as $topic => $parts) {
                    if (!isset($readList[$topic])) {
                        $readList[$topic] = array();
                    }
                    foreach ($parts['partitions'] as $part) {
                        $readList[$topic][] = $broker['host'] . '-' . $broker['port'] .
                            '-' . $part . '-' . $broker['host'] . '-' . $broker['port'] . '-' . $part;
                    }
                }
            } else {
                foreach ($broker['topics'] as $topic => $parts) {
                    if (!isset($readList[$topic])) {
                        $readList[$topic] = array();
                    }
                    foreach ($parts['partitions'] as $part) {
                        $master = $this->config['brokers'][(int)$part['master']];
                        $readList[$topic][] = $broker['host'] . '-' . $broker['port'] .
                            '-' . $part . '-' . $master['host'] . '-' . $master['port'] . '-' . $part;
                    }
                }
            }

        }

        $this->writeList = $writeList;
        $this->readList = $readList;
    }

    public function put($topic, $msg, $async = false)
    {
        if (!$this->producer) {
            $this->producer = new Producer($this->writeList);
        }
        $result = -1;
        try {
            $result = $this->producer->put($topic, $msg, $async);
        } catch (\Exception $e) {
            echo $e;
        }
        return $result;
    }

    public function subscribe($topic, $group)
    {
        if (!$this->consumer) {
            $this->consumer = new Consumer($this->config['zkHosts']);
        }
        try {
            $this->consumer->subscribe($topic, $group);
        } catch (\Exception $e) {
            echo $e;
        }
    }

    public function subscribePartition($topic, $group, $host, $port, $partition)
    {
        if (!$this->consumer) {
            $this->consumer = new Consumer($this->config['zkHosts']);
        }
        try {
            $this->consumer->subscribePartition($topic, $group, $host, $port, $partition);
        } catch (\Exception $e) {
            echo $e;
        }
    }

    public function getNext()
    {
        return $this->consumer->getNext();
    }
}