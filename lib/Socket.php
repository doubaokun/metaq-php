<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

namespace MetaQ;

use MetaQ\MetaQ_Exception;

class Socket
{

    public $active = false;
    private $host;
    private $port;
    private $timeout;
    private $stream;

    public function __construct(
        $host = 'localhost',
        $port = 8023,
        $timeout = 3
    )
    {
        $this->host = $host;
        $this->port = $port;
        $this->timeout = $timeout;
    }

    public function connect()
    {
        $this->stream = @fsockopen(
            $this->host,
            $this->port,
            $errNo,
            $errStr,
            $this->timeout
        );
        @stream_set_blocking($this->stream, 0);
        if (!is_resource($this->stream)) {
            throw new MetaQ_Exception('Can not connect to remote port ' . $this->host . ':' . $this->port);
            return;
        }
        //echo '[DEBUG]: '. 'connect to remote port '. $this->host. ':'. $this->port. "\n";
        register_shutdown_function(array($this, 'close'));
        $this->active = true;
    }

    public function close()
    {
        if (is_resource($this->stream)) {
            fclose($this->stream);
            //echo '[DEBUG]: '. 'disconnect to remote port '. $this->host. ':'. $this->port. "\n";
        }
    }

    public function read0()
    {
        $null = null;
        $selective = array($this->stream);
        $readable = @stream_select($selective, $null, $null, $this->timeout);
        if ($readable) {
            $data = $chunk = '';
            // while(1) {
            //     $chunk = fread($this->stream, 8192);
            //     if($chunk === false) {
            //         $this->close();
            //         throw new MetaQ_Exception('Can not read from remote server, connection closed');
            //     }
            //     if(feof($this->stream)) {
            //         $this->close();
            //         throw new MetaQ_Exception('Unexpected EOF while reading, connection closed');
            //     }
            //     $data .= $chunk;
            //     if($chunk === '') {
            //         break;
            //     }
            // }
            $data .= fgets($this->stream);
            $data .= fgets($this->stream);
            //echo $data."\n";
            return $data;
        }
        if ($readable !== false && $this->stream) {
            $res = stream_get_meta_data($this->stream);
            if (!empty($res['timed_out'])) {
                throw new MetaQ_Exception('Can not read from remote server, timeout');
            }
        }
        return false;
    }

    public function read($maxLen)
    {
        $null = null;
        $selective = array($this->stream);
        $readable = @stream_select($selective, $null, $null, $this->timeout);
        if ($readable) {
            $data = $chunk = '';

            $head = fgets($this->stream);
            $data .= $head;
            list($first, $second, $third) = explode(" ", $head);
            if ($first == 'value') {
                while (1) {
                    $chunk = fread($this->stream, 8192);
                    $data .= $chunk;
                    if (strlen($data) >= $second + strlen($head)) {
                        break;
                    }
                }
            } else if ($first == 'result') {
                $data .= fgets($this->stream);
            }

            // $retry = 0;
            // while(1) {

            //     $chunk = fread($this->stream, 8192);
            //     if($chunk === false) {
            //         $this->close();
            //         throw new MetaQ_Exception('Can not read from remote server, connection closed');
            //     }
            //     if(feof($this->stream)) {
            //         $this->close();
            //         //throw new MetaQ_Exception('Unexpected EOF while reading, connection closed');
            //         $this->connect();
            //     }
            //     $data .= $chunk;
            //     if(strlen($data) >= $maxLen) {
            //         break;
            //     }
            //     if(++$retry > 400000) {
            //         //echo "Can not read enough data.\n";
            //         //$this->close();
            //         //$this->connect();
            //         break;
            //     }
            // }
            //echo $data;
            return $data;
        }
        if ($readable !== false && $this->stream) {
            $res = stream_get_meta_data($this->stream);
            if (!empty($res['timed_out'])) {
                throw new MetaQ_Exception('Can not read from remote server, timeout');
            }
        }
        return false;
    }

    public function write($buf)
    {
        //echo '[DEBUG]: write '. $buf. "\n";
        $null = null;
        $selective = array($this->stream);
        $writable = @stream_select($null, $selective, $null, $this->timeout);
        if ($writable) {
            $written = fwrite($this->stream, $buf);
            if ($written === -1 || $written === false) {
                throw new MetaQ_Exception('Can not write to remote server');
            }
            if ($written === strlen($buf)) {
                return true;
            }
        }
        if ($writable !== false && $this->stream) {
            $res = stream_get_meta_data($this->stream);
            if (!empty($res['timed_out'])) {
                throw new MetaQ_Exception('Can not write to remote server, timeout');
            }
        }
        return false;
    }
}