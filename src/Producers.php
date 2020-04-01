<?php
namespace ClevePHP\Drives\Queues\kafka;

class Producers
{

    protected $producer;

    protected $topic;

    protected $topicConf;

    protected $config;

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

    public function config(\ClevePHP\Drives\Queues\kafka\Config $config)
    {
        $this->config = $config;
        $this->topic = $config->toppic;
        unset($config);
        return $this;
    }
    public function produce($message)
    {
        $this->producer();
        if ($this->producer && $this->topic && $message) {
            $topic = $this->producer->newTopic($this->getConfig()->toppic, $this->topicConf);
            $result = $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
            if ($this->getConfig()->isStrong) {
                // 如果超时，是否重新选举
                $len = $this->producer->getOutQLen();
                while ($len > 0) {
                    $len = $this->producer->getOutQLen();
                    $this->producer->poll(50);
                }
            }
            return $result;
            //
        } else {
            throw new \Exception("toppic is null");
        }
    }

    protected function producer()
    {
        
        if (! $this->producer) {
            $rcf = new \RdKafka\Conf();
            $rcf->set('group.id', $this->getConfig()->gropuId);
            if ($this->config->certification){
                $certification=$this->config->certification;
                $rcf->set('sasl.mechanisms', $certification["mechanisms"]??'PLAIN');
                $rcf->set('api.version.request',$certification["version_request"] ?? 'true');
                $rcf->set("sasl.username",$certification["username"]??"");
                $rcf->set("sasl.password",$certification["password"]??"");
                $rcf->set("security.protocol",$certification["protocol"]??"SASL_SSL");
                $rcf->set("ssl.ca.location",$certification["ca_location"]??"");
            }
            if ($this->getConfig()->errorSavePath) {
                $rcf->setErrorCb(function ($kafka, $err, $reason) use ($cf) {
                    file_put_contents($this->getConfig()->errorSavePath, sprintf("Kafka error: %s (reason: %s)", rd_kafka_err2str($err), $reason) . PHP_EOL, FILE_APPEND);
                });
            }
            if ($this->getConfig()->isInfoError) {
                $rcf->setErrorCb(function ($kafka, $err, $reason){
                    if ($err){
                        throw new \Exception($err);
                    }
                });
            }
            
            if ($this->getConfig()->debugLogLevel) {
                $rcf->setLogCb(function ($kafka, $level, $facility, $message) {
                    printf("Kafka %s: %s (level: %d)\n", $facility, $message, $level);
                });
            }
            
            $cf = new \RdKafka\TopicConf();
            $cf->set('offset.store.method', $this->getConfig()->offsetStoreMethod);
            $cf->set('auto.offset.reset', $this->getConfig()->autoOffsetReset);
            $cf->set('request.required.acks', $this->getConfig()->requiredAck);
            $cf->set("message.timeout.ms", $this->getConfig()->messageTimeoutMs);
            
            $this->topicConf = $cf;
            $rk = new \RdKafka\Producer($rcf);
            
            
            
            $rk->addBrokers($this->getConfig()->metadataBrokerList);
            $this->producer = $rk;
        }
        return $this->producer;
    }
    protected function getConfig(): ?\ClevePHP\Drives\Queues\kafka\Config
    
    {
        return $this->config;
    }
}
