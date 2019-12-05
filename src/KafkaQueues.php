<?php
/**
 * ��Ŀ���ƣ�ת����CleverPHP 3.x
 * �����ƣ�
 * ��������Kafka��Ϣ��������
 * �����ˣ�ceiba
 * ����ʱ�䣺2019��9��15��
 * @version
 */
namespace ClevePHP\Drives\Queues\kafka;

class Kafkaqueues
{

    static $driveObject;

    static $config = [];

    public static function driveObject($configs)
    {
        self::$config = $configs;
        return (new self());
    }
    public static function push($data, $toppic = "list")
    {
        $conf = [
            "toppic" => $toppic,
            "group_id" => isset(self::$config['group_id']) ? self::$config['group_id'] : "T-001"
        ];
        if (self::$config) {
            $conf = array_merge($conf, self::$config);
        }
        if (is_array($data)) {
            $data = json_endoce($data);
        }
        $config = (\ClevePHP\Drives\Queues\kafka\Config::getInstance())->loadConfig($conf);
        return (\ClevePHP\Drives\Queues\kafka\Producers::getInstance())->config($config)->produce($data);
    }
    public static function pop(callable $callback, $toppic = "list")
    {
        $conf = [
            "toppic" => $toppic,
            "group_id" => isset(self::$config['group_id']) ? self::$config['group_id'] : "T-001"
        ];
        if (self::$config) {
            $conf = array_merge($conf, self::$config);
        }
        $config = (\ClevePHP\Drives\Queues\kafka\Config::getInstance())->loadConfig($conf);
        (\ClevePHP\Drives\Queues\kafka\Consumer::getInstance())->config($config)->consumer(function ($message) use ($callback) {
            ($callback instanceof \Closure) && call_user_func($callback, $message->payload);
        });
    }
}
