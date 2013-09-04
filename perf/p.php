<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

require dirname(__FILE__) . "/../lib/MetaQ.php";

$metaq = new MetaQ\MetaQ();

$i = $j = 0;
$start = time();
while (++$i < 100000000000) {
    $metaq->put('t1', 'hello' . $i . generate(1000));
    if (++$j % 1000 == 0 && (time() - $start) > 0) {
        $qps = $i / (time() - $start);
        echo posix_getpid(). " ". round($qps) . " req/s\n";
    }
}

function generate($len)
{
    return substr(str_shuffle(str_repeat('0123456789abcdefghijklmnopqrstuvwxyz', $len)), 0, $len);
}
