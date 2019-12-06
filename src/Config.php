<?php
namespace ClevePHP\Drives\Queues\kafka;

class Config
{

    //Topic��Ԫ��Ϣˢ�µļ�� **
    public $metadataRefreshIntervalMs = 10000;

    //����broker�ĵ�ַ **
    public $metadataBrokerList = "127.0.0.1:9092";

    // ����broker�Ĵ���汾 **
    public $brokerVersion = "1.0.0";
    //ֻ��Ҫleaderȷ����Ϣ **
    public $requiredAck = "1";
    //ѡ���첽 **
    public $isAsyn = false;

    //ÿ500���뷢����Ϣ **
    public $produceInterval = 50;
    //toppic
    public $toppic = "";
    //groupid
    public $gropuId="";
    //����offset�ķ�ʽ������ѡ��broker����file
    public $offsetStoreMethod="file";
    //���û�м�⵽�б����offset���ʹ���С��ʼ
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

    // �������ļ�����
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