<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

require dirname(__FILE__) . "/../lib/MetaQ.php";

$metaq = new MetaQ\MetaQ();

$topic = 't1';
$group = 0;
$metaq->subscribe($topic, $group);

while (1) {
    $msgs = $metaq->getNext();
    foreach ($msgs as $msg) {
        print_r($msg);
    }
}