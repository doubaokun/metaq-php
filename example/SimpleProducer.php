<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

require dirname(__FILE__) . "/../lib/MetaQ.php";

$metaq = new MetaQ\MetaQ();

$i = 0;
while (++$i < 100000) {
    $result = $metaq->put('t1', 'hello' . $i);
    print_r($result);
    sleep(1);
}
