<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

namespace MetaQ;

class MetaZookeeper
{

    private $conn;
    private $zk;
    private $brokerMetaData = null;

    public function __construct($conn = '127.0.0.1:2181')
    {
        $this->conn = $conn;
        $this->connect();
    }

    private function connect()
    {
        if (!$this->zk) {
            $this->zk = new \Zookeeper($this->conn);
            $this->brokerMetaData = null;
        }
    }

    public function getTopicMetadata()
    {
        $this->connect();
        $topicMetadata = array();
        foreach ($this->zk->getChildren("/meta/brokers/topics") as $topic) {
            foreach ($this->zk->getChildren("/meta/brokers/topics/$topic") as $brokerId) {
                $brokerInfo = $this->getBrokerInfo($brokerId);
                if(!$brokerInfo) continue;
                $arr = explode("-", $brokerId);
                $masterInfo = $this->getBrokerInfo($arr[0] . '-' . 'm');
                $partitionCount = (int)$this->zk->get(
                    "/meta/brokers/topics/$topic/$brokerId"
                );
                for ($p = 0; $p < $partitionCount; $p++) {
                    $topicMetadata[$topic][] = array(
                        "id" => "{$brokerId}-{$p}",
                        "broker" => $brokerId,
                        "host" => $brokerInfo['host'],
                        "port" => $brokerInfo['port'],
                        "part" => $p,
                        "master" => $masterInfo['host'] . '-' . $masterInfo['port'] . '-' . $p,
                        "gid" => $arr[0],
                        "type" => (strpos($brokerId, 'm') > -1) ? 'master' : 'slave'
                        // "brokerInfo" => $brokerInfo,
                        // "masterInfo" => $masterInfo
                    );
                }
            }
        }

        return $topicMetadata;
    }

    public function getCversion($path) {
        $a = array();
        $this->zk->get($path, null, $a);
        return $a['cversion'];
    }

    public function getBrokerInfo($brokerId)
    {
        $this->getBrokerMetadata();
        if (!isset($this->brokerMetadata[$brokerId])) {
            //throw new \MetaQ_Exception("Unknown brokerId `$brokerId`");
            return false;
        }
        return $this->brokerMetadata[$brokerId];
    }

    public function getBrokerMetadata()
    {
        if ($this->brokerMetaData === null) {
            $this->connect();
            $this->brokerMetadata = array();
            $brokers = $this->zk->getChildren("/meta/brokers/ids", array($this, 'brokerWatcher'));
            foreach ($brokers as $brokerId) {
                $group = $this->zk->getChildren("/meta/brokers/ids/$brokerId");
                foreach ($group as $server) {
                    $brokerIdentifier = $this->zk->get("/meta/brokers/ids/$brokerId/$server");
                    if ($server === "master_config_checksum") continue;
                    $server = str_replace('master', 'm', $server);
                    $server = str_replace('slave', 's', $server);

                    $brokerIdentifier = str_replace('meta://', '', $brokerIdentifier);
                    $parts = explode(":", $brokerIdentifier);
                    $this->brokerMetadata[$brokerId . '-' . $server] = array(
                        'host' => $parts[0],
                        'port' => $parts[1],
                    );
                }
            }
        }
        return $this->brokerMetadata;
    }

    public function brokerWatcher($type, $state, $path)
    {
        if ($path == "/meta/brokers/ids") {
            $this->brokerMetadata = null;
        }
    }

    public function needsRefereshing()
    {
        return $this->brokerMetadata === null;
    }

    public function registerConsumerProcess($groupId, $processId)
    {
        $this->connect();
        if (!$this->zk->exists("/meta/consumers")) $this->createPermaNode("/meta/consumers");
        if (!$this->zk->exists("/meta/consumers/{$groupId}")) $this->createPermaNode("/meta/consumers/{$groupId}");
        if (!$this->zk->exists("/meta/consumers/{$groupId}/owners")) $this->createPermaNode("/meta/consumers/{$groupId}/owners");
        if (!$this->zk->exists("/meta/consumers/{$groupId}/offsets")) $this->createPermaNode("/meta/consumers/{$groupId}/offsets");
        if (!$this->zk->exists("/meta/consumers/{$groupId}/ids")) $this->createPermaNode("/meta/consumers/{$groupId}/ids");

        if (!$this->zk->exists("/meta/consumers/{$groupId}/ids/$processId")) {
            $this->createEphemeralNode("/meta/consumers/{$groupId}/ids/$processId", "");
        }
    }

    private function createPermaNode($path, $value = null)
    {
        $this->zk->create(
            $path,
            $value,
            $params = array(array(
                'perms' => \Zookeeper::PERM_ALL,
                'scheme' => 'world',
                'id' => 'anyone',
            ))
        );
    }

    private function createEphemeralNode($path, $value)
    {
        $result = $this->zk->create(
            $path,
            $value,
            $params = array(array(
                'perms' => \Zookeeper::PERM_ALL,
                'scheme' => 'world',
                'id' => 'anyone',
            )),
            \Zookeeper::EPHEMERAL
        );
        return $result;
    }

    public function removeOldOwnPartitions($groupId, $topic, $partition)
    {
        $pid = $partition[3];
        $path = "/meta/consumers/{$groupId}/owners/{$topic}/{$pid}";
        if ($this->zk->exists($path)) {
            $this->zk->delete($path);
        }
    }

    public function setPartitionOwner($groupId, $topic, $partition, $consumerId)
    {
        $pid = $partition['gid'] . '-' . $partition['part'];
        $path = "/meta/consumers/{$groupId}/owners/{$topic}";
        if (!$this->zk->exists($path)) {
            $this->createPermaNode("{$path}");
        }
        if (!$this->zk->exists("{$path}/{$pid}")) {
            $this->createEphemeralNode("{$path}/{$pid}", $consumerId);
            $result = true;
        } else {
            $result = false;
        }
        return $result;
    }

    public function getTopicConsumers($groupId, $topic)
    {
        $consumers = array();
        $this->connect();
        if (!$this->zk->exists("/meta/consumers/{$groupId}/ids")) {
            $this->createPermaNode("/meta/consumers/{$groupId}/ids");
        }
        $path = "/meta/consumers/{$groupId}/ids";
        if ($this->zk->exists($path)) {
            foreach ($this->zk->getChildren($path) as $id) {
                $consumers[] = array($id, $this->zk->get("{$path}/{$id}"));
            }
        }
        return $consumers;
    }

    public function getTopicOffset($groupId, $topic, $brokerId, $partition)
    {
        $offset = 0;
        $this->connect();
        if (!$this->zk->exists("/meta/consumers/{$groupId}/offsets/$topic/{$brokerId}-{$partition}")) {
            $this->createPermaNode("/meta/consumers/{$groupId}/offsets/$topic/{$brokerId}-{$partition}");
        }
        $path = "/meta/consumers/{$groupId}/offsets/$topic/{$brokerId}-{$partition}";
        $offset = $this->zk->get("{$path}");
        return $offset;
    }

    public function commitOffset($groupId, $topic, $brokerId, $partition, $offset)
    {
        $this->connect();
        $path = "/meta/consumers/{$groupId}/offsets/{$topic}";
        if (!$this->zk->exists($path)) {
            $this->createPermaNode($path);
        }
        if (!$this->zk->exists("{$path}/{$brokerId}-{$partition}")) {
            $this->createPermaNode("{$path}/{$brokerId}-{$partition}", $offset);
        } else {
            $this->zk->set("{$path}/{$brokerId}-{$partition}", $offset);
        }
    }
}