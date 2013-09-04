<?php

/**
 * @author  Bruce Dou <doubaokun@gmail.com>
 * @link    https://github.com/doubaokun/metaq-php
 * @version 0.1.0
 */

namespace MetaQ;


class Codec
{

    const PUT_COMMAND = 'put';
    const GET_COMMAND = 'get';
    const RESULT_COMMAND = 'result';
    const VALUE_COMMAND = 'value';
    const OFFSET_COMMAND = 'offset';
    const HTTP_BadRequest = '400';
    const HTTP_NotFound = '404';
    const HTTP_Forbidden = '403';
    const HTTP_Unauthorized = '401';
    const HTTP_InternalServerError = '500';
    const HTTP_ServiceUnavailable = '503';
    const HTTP_GatewayTimeout = '504';
    const HTTP_Success = '200';
    const HTTP_Moved = '301';
    const MAX_MSG_LENGTH = 102400;

    public static function putEncode($topic, $partition, $msg)
    {
        $data = sprintf(
            "put %s %d %d %d %d\r\n%s",
            $topic,
            $partition,
            strlen($msg),
            0,
            1,
            $msg
        );
        return $data;
    }

    public static function putResultDecode($data)
    {
        list($head, $payload) = explode("\r\n", $data);
        if(!$head || !$payload) {
            throw new MetaQ_Exception('can not receive complete response: '. $data);
        }
        list($type, $status, $len, $code) = explode(" ", $head);

        $success = false;
        $result = array();
        $errorMsg = '';
        if ($type == self::RESULT_COMMAND) {
            switch ($status) {
                case self::HTTP_Success:
                    $success = true;
                    list($id, $code, $offset) = explode(" ", $payload);
                    $result = array(
                        'id' => $id,
                        'code' => $code,
                        'offset' => $offset
                    );

                    break;
                case self::HTTP_NotFound:
                case self::HTTP_InternalServerError:
                    $success = true;
                    $errorMsg = $payload;
                    break;
            }

        }
        return array($success, $result, $errorMsg);
    }

    public static function getEncode($topic, $group, $partition, $offset)
    {
        $data = sprintf("get %s %s %d %d %d 0\r\n",
            $topic,
            $group,
            $partition,
            $offset,
            self::MAX_MSG_LENGTH
        );
        return $data;
    }

    public static function getResultDecode($data)
    {
        $msgs = array();
        $offset = $_offset = 0;

        $arr = explode("\r\n", $data, 2);

        if (sizeof($arr) !== 2) {
            echo "can not parse data " . sizeof($arr) . "\n";
            print_r($data);
            return array(array(), 0, 0);
        }
        $head = $arr[0];
        $body = $arr[1];

        list($first, $second, $third) = self::_decode_head($head);

        if ($first == 'result') {
            $status = $second;
            if ($second == self::HTTP_Moved) {
                $_offset = $body;
            }
        } else if ($first == 'value') {
            list($msgs, $offset) = self::_decode_body($body);
        }
        return array($msgs, $offset, $_offset);
    }

    private static function _decode_head($head)
    {
        list($first, $second, $third) = explode(" ", $head);
        return array($first, $second, $third);
    }

    private static function _decode_body($body)
    {
        $offset = 0;
        $msgs = array();
        while (1) {
            if (strlen($body) < 20) {
                break;
            }

            $len = array_shift(unpack('N', mb_substr($body, 0, 4)));
            $crc = array_shift(unpack('N', mb_substr($body, 4, 4)));
            $id = self::_64id(mb_substr($body, 8, 8));
            $flag = array_shift(unpack('N', mb_substr($body, 16, 4)));
            $msg = mb_substr($body, 20, $len);

            if (strlen($msg) < $len) {
                break;
            }

            if ((int)(crc32($msg) & 0x7FFFFFFF) !== $crc) {
                echo "crc error.\n";
                break;
            }

            $body = mb_substr($body, 20 + $len);
            $offset += 20 + $len;
            $msgs[] = array('id' => $id, 'msg' => $msg);
        }
        return array($msgs, $offset);
    }

    private static function _64id($id)
    {
        $return = unpack('Na/Nb', $id);
        return ($return['a'] << 32) + ($return['b']);
    }

    public static function offsetEncode($topic, $group, $partition, $offset)
    {
        $data = sprintf("offset %s %s %d %d\r\n",
            $topic,
            $group,
            $partition,
            $offset
        );
        return $data;
    }

}