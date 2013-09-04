<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

namespace MetaQ;


class MetaQ_Exception extends \Exception
{
    public function __construct($msg)
    {
        echo 'MetaQ exception: ' . $msg . "\n";
        return $msg;
    }
}