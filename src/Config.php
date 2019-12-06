<?php
namespace ClevePHP\Drives\Queues\kafka;

class Config
{

    //Topic的元信息刷新的间隔 **
    public $metadataRefreshIntervalMs = 10000;

    //设置broker的地址 **
    public $metadataBrokerList = "127.0.0.1:9092";

    // 设置broker的代理版本 **
    public $brokerVersion = "1.0.0";
    //只需要leader确认消息 **
    public $requiredAck = "1";
    //选择异步 **
    public $isAsyn = false;

    //每500毫秒发送消息 **
    public $produceInterval = 50;
    //toppic
    public $toppic = "";
    //groupid
    public $gropuId="";
    //保存offset的方式，可以选择broker或者file
    public $offsetStoreMethod="file";
    //如果没有检测到有保存的offset，就从最小开始
    public $autoOffsetReset="smallest";

    private static $instance;

    private function __construct()
    {}

    private function __clone()
    {}

    static public function getInstance()
    {
        if (! self::$instance instanceof self) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    // 由配置文件加载
    public function loadConfig($config)
    {
        if ($config) {
            if (isset($config["refreshInterval_ms"])) {
                $this->metadataRefreshIntervalMs = $config['refreshInterval_ms'];
            }
            if (isset($config["broker_list"])) {
                $this->metadataBrokerList = $config['broker_list'];
            }
            if (isset($config["broker_version"])) {
                $this->requiredAck = $config['broker_version'];
            }
            if (isset($config["required_ack"])) {
                $this->requiredAck = $config['required_ack'];
            }
            if (isset($config["is_asyn"])) {
                $this->isAsyn = $config['is_asyn'];
            }
            if (isset($config["produce_interval"])) {
                $this->produceInterval = $config['produce_interval'];
            }
            if (isset($config["toppic"])) {
                $this->toppic = $config['toppic'];
            }
            if (isset($config["group_id"])) {
                $this->gropuId = $config['group_id'];
            }
            if (isset($config['offset_store_method'])) {
                $this->offsetStoreMethod=$config['offset_store_method'];
            }
            if (isset($config['auto_offset_reset'])) {
                $this->autoOffsetReset=$config['auto_offset_reset'];
            }
        }
        return $this;
    }
}