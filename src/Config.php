<?php
namespace ClevePHP\Drives\Queues\kafka;

class Config
{

    public $metadataRefreshIntervalMs = 10000;
    public $metadataBrokerList = "127.0.0.1:9092";
    public $brokerVersion = "1.0.0";
    public $requiredAck = "0";
    public $isAsyn = false;
    public $produceInterval = 50;
    public $toppic = "";
    public $gropuId="";
    public $offsetStoreMethod="file";
    public $autoOffsetReset="smallest";
    public $autoCommitEnable=0;
    public $autoCommitIntervalMs=100;
    public $messageTimeoutMs=3000;
    public $debugLogLevel=0;

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
            if(isset($config["auto_commit_enable"])){
                $this->autoCommitEnable=$config["auto_commit_enable"];
            }
            if (isset($config["auto_commit_interval_ms"])) {
                $this->autoCommitIntervalMs=$config["auto_commit_interval_ms"];
            }
            if (isset($config['debug_log_level'])) {
                $this->debugLogLevel=$config['debug_log_level'];
            }
            if (isset($config['message_timeout_ms'])) {
                $this->messageTimeoutMs=$config['message_timeout_ms'];
            }
        }
        return $this;
    }
}