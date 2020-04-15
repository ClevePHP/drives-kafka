<?php
namespace ClevePHP\Drives\Queues\kafka;

class Config
{

    public $metadataRefreshIntervalMs = 10000;

    public $metadataBrokerList = "127.0.0.1:9092";

    public $brokerVersion = "1.0.0";

    public $requiredAck = "1";

    public $isAsyn = false;

    public $produceInterval = 50;

    public $toppic = "";

    public $gropuId = "";

    public $offsetStoreMethod = "file";

    public $autoOffsetReset = "smallest";

    public $autoCommitEnable = 0;

    public $autoCommitIntervalMs = 100;

    public $messageTimeoutMs = 3000;

    public $debugLogLevel = 1;

    public $consumerModel = 0;

    public $isStrong = true;

    public $errorSavePath = null;

    public $isInfoError = true;

    public $certification = [];

    private $extConfig = [];

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
                unset($config['refreshInterval_ms']);
            }
            if (isset($config["broker_list"])) {
                $this->metadataBrokerList = $config['broker_list'];
                unset($config['broker_list']);
            }
            if (isset($config["broker_version"])) {
                $this->brokerVersion = $config['broker_version'];
                unset($config['broker_version']);
            }
            if (isset($config["required_ack"])) {
                $this->requiredAck = $config['required_ack'];
                unset($config['required_ack']);
            }
            if (isset($config["is_asyn"])) {
                $this->isAsyn = $config['is_asyn'];
                unset($config['is_asyn']);
            }
            if (isset($config["produce_interval"])) {
                $this->produceInterval = $config['produce_interval'];
                unset($config['produce_interval']);
            }
            if (isset($config["toppic"])) {
                $this->toppic = $config['toppic'];
                unset($config['toppic']);
            }
            if (isset($config["group_id"])) {
                $this->gropuId = $config['group_id'];
                unset($config['group_id']);
            }
            if (isset($config['offset_store_method'])) {
                $this->offsetStoreMethod = $config['offset_store_method'];
                unset($config['offset_store_method']);
            }
            if (isset($config['auto_offset_reset'])) {
                $this->autoOffsetReset = $config['auto_offset_reset'];
                unset($config['auto_offset_reset']);
            }
            if (isset($config["auto_commit_enable"])) {
                $this->autoCommitEnable = $config["auto_commit_enable"];
                unset($config["auto_commit_enable"]);
            }
            if (isset($config["auto_commit_interval_ms"])) {
                $this->autoCommitIntervalMs = $config["auto_commit_interval_ms"];
                unset($config["auto_commit_interval_ms"]);
            }
            if (isset($config['debug_log_level'])) {
                $this->debugLogLevel = $config['debug_log_level'];
                unset($config['debug_log_level']);
            }
            if (isset($config['message_timeout_ms'])) {
                $this->messageTimeoutMs = $config['message_timeout_ms'];
                unset($config['message_timeout_ms']);
            }
            if (isset($config["consumer_model"])) {
                $this->consumerModel = $config["consumer_model"];
                unset($config["consumer_model"]);
            }
            if (isset($config['is_strong'])) {
                $this->isStrong = $config["is_strong"];
                unset($config["is_strong"]);
            }
            if (isset($config['error_save_path'])) {
                $this->errorSavePath = $config["error_save_path"];
                unset($config["error_save_path"]);
            }
            if (isset($config['is_info_error'])) {
                $this->isInfoError = $config["is_info_error"];
                unset($config["is_info_error"]);
            }
            if (isset($config["username"]) && isset($config["password"])) {
                $this->certification = [
                    "username" => $config['username'],
                    "password" => $config['password'],
                    "protocol" => $config['protocol'] ?? "",
                    "ca_location" => $config["ca_location"] ?? ""
                ];
                unset($config['username']);
                unset($config['password']);
                unset($config['protocol']);
                unset($config["ca_location"]);
            }
            
            $this->extConfig = $config;
        }
        return $this;
    }

    public function getExtConfig($modelType = null)
    {
        if (isset($this->extConfig[$modelType])) {
            return $this->extConfig[$modelType];
        }
        return [];
    }
}