<?php
/**
 * 项目名称：转自于CleverPHP 3.x
 * 类名称：
 * 类描述：Kafka消息队列驱动
 * 创建人：ceiba
 * 创建时间：2019年9月15日
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
