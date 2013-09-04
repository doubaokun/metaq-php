<?php

$METAQ_CONFIG = array(
    'zkHosts' => '127.0.0.1:2181',
    'brokers' => array(
        0 => array(
            'role' => 'master',
            'host' => '192.168.1.103',
            'port' => 8123,
            'topics' => array(
                'test' => array(
                    'partitions' => array(
                        0, 1, 2, 3, 4
                    ),
                ),
            ),
        ),
        1 => array(
            'role' => 'master',
            'host' => '192.168.1.104',
            'port' => 8123,
            'topics' => array(
                't1' => array(
                    'partitions' => array(
                        0, 1, 2, 3, 4
                    ),
                ),
            ),
        ),
    ),
);



