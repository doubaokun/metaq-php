<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

require dirname(__FILE__) . "/../lib/MetaQ.php";

$metaq = new MetaQ\MetaQ();

$topic = 't1';
$group = 1;
$metaq->subscribe($topic, $group);

$i = $j = 0;
$start = time();
while (1) {
    $msgs = $metaq->getNext();
    foreach ($msgs as $msg) {
        print_r($msg);
        ++$i;
        if (++$j % 1000 == 0 && (time() - $start) > 0) {
            $qps = $i / (time() - $start);
            echo round($qps) . " req/s\n";
            print_r($msg);

        }
    }
}