<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

namespace MetaQ;

class Producer extends MetaQ
{

    const RETRY = 3;
    private $sockets;
    private $writeList;
    private $inactive;

    public function __construct($writeList)
    {
        $this->writeList = $writeList;
    }

    public function put($topic, $msg, $async)
    {
        if (strlen($msg) > Codec::MAX_MSG_LENGTH) {
            throw new \Exception('Can not put message which length > ' . Codec::MAX_MSG_LENGTH);
        }
        list($host, $port, $part) = $this->selectPart($topic);
        if (!isset($this->sockets[$host . ':' . $port])) {
            $this->sockets[$host . ':' . $port] = new Socket($host, $port);
            try {
                $this->sockets[$host . ':' . $port]->connect();
            } catch (\Exception $e) {
                echo $e->getMessage();
            }

            if (!$this->sockets[$host . ':' . $port]->active) {
                $this->inactive = $host . '-' . $port;
                $this->removeInactive($topic);
                if (sizeof($this->writeList[$topic]) > 0) {
                    $this->put($topic, $msg);
                } else {
                    throw new \Exception('all brokers seems down');
                }
            }
        }
        $socket = $this->sockets[$host . ':' . $port];

        $data = Codec::putEncode($topic, $part, $msg);
        $reTry = 0;
        $success = false;
        while (1) {
            $writeSuccess = $socket->write($data);
            if ($writeSuccess && $async) {
                $success = true;
                $result = array(
                    'id' => -1,
                    'code' => 0,
                    'offset' => -1
                );
                break;
            }
            $buf = $socket->read0();
            list($success, $result, $errorMsg) = Codec::putResultDecode($buf);
            if ($errorMsg) {
                throw new MetaQ_Exception($errorMsg);
            }
            if ($success || $reTry >= self::RETRY) {
                break;
            }
            usleep(500);
            $reTry++;
        }

        if (!$success) {
            throw new MetaQ_Exception('put command not succeed to ' . $host . ':' . $port);
            $this->inactive = $host . '-' . $port;
            $this->removeInactive($topic);
            if (sizeof($this->writeList) > 0) {
                $this->put($topic, $msg);
            } else {
                throw new \Exception('all brokers seems down');
            }
        }
        return $result;
    }

    private function selectPart($topic)
    {
        if (!isset($this->writeList[$topic])) {
            throw new \Exception('no broker accept topic ' . $topic);
        }
        $partId = rand(0, sizeof($this->writeList[$topic]) - 1);
        return explode('-', $this->writeList[$topic][$partId]);
    }

    private function removeInactive($topic)
    {
        $writeList = $this->writeList[$topic];
        $newWriteList = array();
        foreach ($writeList as $part) {
            if (strpos($part, $this->inactive) === false) {
                $newWriteList[] = $part;
            }
        }
        $this->writeList[$topic] = $newWriteList;
    }

}