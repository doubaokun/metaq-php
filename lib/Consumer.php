<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

namespace MetaQ;


class Consumer extends MetaQ
{

    public $processId;
    public $id;
    public $topic;
    public $group;
    private $offset = array();
    private $sockets;
    private $partitionList = array();
    private $zkHosts;
    private $zk;
    private $topicMetas;
    private $pTime;
    private $cversion;

    public function __construct($zkHosts = '127.0.0.1:2181')
    {
        $this->zkHosts = $zkHosts;
        $this->processId = $this->group . gethostname() . "-" . uniqid();
    }

    public function subscribe($topic, $group)
    {
        $this->topic = $topic;
        $this->group = $group;

        if (!$this->zk) {
            $this->zk = new MetaZookeeper($this->zkHosts);
        }
        $this->zk->registerConsumerProcess($this->group, $this->getSelfId());
        $this->balance();

    }

    private function getSelfId()
    {
        $host = str_replace("\n", "",
            shell_exec("ifconfig eth0 | grep 'inet addr' | awk -F':' {'print $2'} | awk -F' ' {'print $1'}"));
        if (!$host) {
            $host = gethostname();
        }
        $this->id = $this->group . '_' . $host . "-" . getmypid();
        return $this->id;
    }

    private function checkBalance() {
        $newCversion = $this->zk->getCversion("/meta/consumers/{$this->group}/ids");
        if($newCversion !== $this->cversion) {
            $retry = 0;
            while($retry++ < 30) {
                if($this->balance()) break;
            }

            if($retry > 10) {
                throw new \Exception('balanced too many times');
            }

            $newCversion = $this->zk->getCversion("/meta/consumers/{$this->group}/ids");
            $this->cversion = $newCversion;
        }
    }

    public function balance()
    {
        MetaQ::$log->logDebug("begin balancing.");
        // begin balance
        // unsubscribe
        foreach ($this->partitionList as $partition) {
            $this->zk->removeOldOwnPartitions($this->group, $this->topic, $partition);
        }
        $this->partitionList = array();

        // wait other consumers
        usleep(2000000);
        $this->topicMetas = $this->zk->getTopicMetadata();
        $partitions = $this->topicMetas[$this->topic];

        $ids = $this->zk->getTopicConsumers($this->group, $this->topic);
        // Only master partitions
        $newPartitions = $this->getNewPartitions($partitions, $ids, $this->id);

        foreach ($newPartitions as $partition) {
            $own = $this->zk->setPartitionOwner($this->group, $this->topic, $partition, $this->id);
            if (!$own) {
                //echo "can not own ". $partition['id']. ", try later\n";
                // Needs further balance.
                usleep(1000000);
                return false;
            }
            $this->subscribePartition($this->topic, $this->group, $partition);
            $slaves = $this->getSlavePartitions($partition, $partitions);
            foreach ($slaves as $slave) {
                $this->subscribePartition($this->topic, $this->group, $slave);
            }
        }
        // echo "finished balance.\n";
        MetaQ::$log->logDebug("end balancing.");
        // end balance
        return true;
    }

    public function getNewPartitions($partitions, $ids, $id)
    {
        $masterPartitions = array();
        foreach ($partitions as $partition) {
            if ($partition['type'] === 'slave') continue;
            $masterPartitions[] = $partition;
        }
        $newPartitions = array();
        $consumers = array();
        foreach ($ids as $value) {
            $consumers[] = $value[0];
        }
        $nPartsPerConsumer = (int)(sizeof($masterPartitions) / sizeof($consumers));
        $nConsumerswithExtPart = sizeof($masterPartitions) % sizeof($consumers);

        $myConsumerPosition = array_search($id, $consumers);

        if ($myConsumerPosition < 0) {
            return array();
        }

        $startPart = $nPartsPerConsumer * $myConsumerPosition + min($myConsumerPosition, $nConsumerswithExtPart);
        $nParts = $nPartsPerConsumer + ($myConsumerPosition + 1 > $nConsumerswithExtPart ? 0 : 1);

        if ($nParts <= 0) {
            return array();
        }

        for ($i = $startPart; $i < $startPart + $nParts; $i++) {
            $newPartitions[] = $masterPartitions[$i];
        }

        return $newPartitions;
    }

    public function subscribePartition($topic, $group, $partition)
    {
        $this->topic = $topic;
        $this->group = $group;
        $host = $partition['host'];
        $port = $partition['port'];
        $part = $partition['part'];
        $master = $partition['master'];
        $this->partitionList[] = array(
            //name
            $host . '-' . $port . '-' . $part,
            //sleep
            1,
            //master
            $master,
            //partId
            $partition['gid'] . '-' . $partition['part']
        );
    }

    private function getSlavePartitions($partition, $partitions)
    {
        $slaves = array();
        $master = $partition['master'];
        foreach ($partitions as $value) {
            if ($value['master'] == $master && $value['type'] != 'master') {
                $slaves[] = $value;
            }
        }
        return $slaves;
    }

    public function getNext()
    {
        if((time() - $this->pTime) > 1) {
            $this->pTime = time();
//            MetaQ::$log->logDebug("current status:", array(
//                'id' => $this->id,
//                'group' => $this->group,
//                'topic' => $this->topic,
//                'offset' => $this->offset,
//                'sockets' => $this->sockets,
//                'partitionList' => $this->partitionList
//            ));
            $this->checkBalance();
        }

        if (sizeof($this->partitionList) == 0) {
            return array();
        }

        $arr = $this->getCurrentPartition();
        list($partition, $sleep, $master, $partId) = $arr;
        list($host, $port, $pid) = explode('-', $partition);

        $offset = $this->getOffset($master);
        if (!isset($this->sockets[$host . ':' . $port])) {
            $this->sockets[$host . ':' . $port] = new Socket($host, $port);
            try {
                $this->sockets[$host . ':' . $port]->connect();
            } catch (\Exception $e) {
                echo $e->getMessage();
            }
        }
        $socket = $this->sockets[$host . ':' . $port];
        $data = Codec::getEncode($this->topic, $this->group, $pid, $offset);
        $socket->write($data);
        $data = $socket->read(Codec::MAX_MSG_LENGTH + 14);
        list($msgs, $offset, $_offset) = Codec::getResultDecode($data);
        // 301, We can turn off this feature at server side
        if ($_offset) {
            $sleep += $sleep * 20;
            $this->partitionList[] = array(
                $partition,
                $sleep,
                $master,
                $partId);
            $this->offset[$master] = $_offset;
            $this->commitOffset($this->group, $master, $_offset, 0);
            return array();
        } else if ($offset) {
            $this->partitionList[] = array(
                $partition,
                $sleep,
                $master,
                $partId);
            $this->offset[$master] += $offset;
            $lastMsg = array_pop(array_values($msgs));
            $lastId = $lastMsg['id'];
            $this->commitOffset($this->group, $master, $this->offset[$master], $lastId);
            if ($msgs) {
                return $msgs;
            }
        } else {
            $sleep += $sleep * 2;
            $this->partitionList[] = array(
                $partition,
                $sleep,
                $master,
                $partId);
            // Reorder the partition list, sort and get the smallest sleep value
            $parts = array_values($this->partitionList);
            uasort($parts, array($this, 'cmp'));
            $smallest = array_shift($parts);
            if ($smallest[1] >= 2000000) {
                //echo "sleep 2s\n";
                //echo "offset ". $this->offset[$master]. "\n";
                usleep(2000000);
                foreach ($this->partitionList as $key => $value) {
                    $this->partitionList[$key][1] -= 2000000;
                }
            }
            return array();
        }
    }

    public function getCurrentPartition()
    {

        uasort($this->partitionList, array($this, 'cmp'));
        return array_shift($this->partitionList);
    }

    private function getOffset($partition)
    {
        if (!isset($this->offset[$partition])) {
            $offset = $this->fetchOffset($this->group, $partition);
            $this->offset[$partition] = $offset;
        }
        return $this->offset[$partition];
    }

    private function fetchOffset($group, $partition)
    {

        // master broker id
        $gid = $this->getGid($partition);
        $pid = array_pop(explode('-', $partition));
        $id_offset = $this->zk->getTopicOffset($this->group, $this->topic, $gid, $pid);
        $result = explode('-', $id_offset);
        $offset = $result[sizeof($result) - 1];
        $id = str_replace('-' . $offset, '', $id_offset);
        if (!$offset) {
            $offset = 0;
        }
        return $offset;
    }

    private function getGid($master)
    {
        foreach ($this->topicMetas[$this->topic] as $meta) {
            if ($meta['master'] == $master) {
                return $meta['gid'];
            }
        }
    }

    private function commitOffset($group, $partition, $_offset, $id)
    {

        $pid = array_pop(explode('-', $partition));
        // master broker id
        $gid = $this->getGid($partition);
        $this->zk->commitOffset($this->group, $this->topic, $gid, $pid, $id . '-' . $_offset);
    }

    private function cmp($a, $b)
    {
        if ($a[1] == $b[1]) {
            return 0;
        }
        return ($a[1] < $b[1]) ? -1 : 1;
    }
}