<?php

namespace ClevePHP\Drives\Queues\kafka;

class Consumer
{

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

    protected $producer;

    protected $topic;

    protected $config;

    public function config(\ClevePHP\Drives\Queues\kafka\Config $config)
    {
        $this->config = $config;
        $this->topic = $config->toppic;
        unset($config);
        return $this;
    }

    public function getPartitions()
    {
        $consumer = new \RdKafka\Consumer();
        $consumer->addBrokers($this->getConfig()->metadataBrokerList);
        $topic = $consumer->newTopic($this->topic);
        $allInfo = $consumer->getMetadata(false, $topic, 60e3);
        $topics = $allInfo->getTopics();

        foreach ($topics as $tp) {
            $partitions = $tp->getPartitions();
            break;
        }
        return $partitions;
    }

    public function consumerPop(callable $callback, $partionId = 0)
    {
        $conf = new \RdKafka\Conf();
        $conf->set('group.id', $this->getConfig()->gropuId);
        if ($this->config->certification){
            //认证相关
            $certification=$this->config->certification;
            $conf->set('sasl.mechanisms', $certification["mechanisms"]??'PLAIN');
            $conf->set('api.version.request',$certification["version_request"] ?? 'true');
            $conf->set("sasl.username",$certification["username"]??"");
            $conf->set("sasl.password",$certification["password"]??"");
            $conf->set("security.protocol",$certification["protocol"]??"SASL_SSL");
            $conf->set("ssl.ca.location",$certification["ca_location"]??"");
        }
        $rk = new \RdKafka\Consumer($conf);
        $topicConf = new \RdKafka\TopicConf();
        $rk->addBrokers($this->getConfig()->metadataBrokerList);
        $topicConf->set('request.required.acks', $this->getConfig()->requiredAck);
        $topicConf->set('offset.store.path', sys_get_temp_dir());
        $topicConf->set('auto.commit.enable', $this->getConfig()->autoCommitEnable);
        $topicConf->set('auto.commit.interval.ms', $this->getConfig()->autoCommitIntervalMs);
        $topicConf->set('offset.store.method', $this->getConfig()->offsetStoreMethod);
        $topicConf->set('auto.offset.reset', $this->getConfig()->autoOffsetReset);
        $topicConf->set("message.timeout.ms", $this->getConfig()->messageTimeoutMs);
        $topic = $rk->newTopic($this->getConfig()->toppic, $topicConf);
        // Start consuming partition 0
        $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
        $message = $topic->consume($partionId, 120 * 10000);
        if ($message) {
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    ($callback instanceof \Closure) && call_user_func($callback, $message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "receive..." . PHP_EOL;
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "timeout..." . PHP_EOL;
                    break;
                default:
                    break;
            }
        }
    }

    public function consumer(callable $callback)
    {
        $conf = new \RdKafka\Conf();
        $conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    // echo "Assign: ";
                    // var_dump($partitions);
                    $kafka->assign($partitions);
                    break;
                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    // echo "Revoke: ";
                    // var_dump($partitions);
                    $kafka->assign(NULL);
                    break;
                default:
                    throw new \Exception($err);
            }
        });
        $conf->set('group.id', $this->getConfig()->gropuId);
        $conf->set('metadata.broker.list', $this->getConfig()->metadataBrokerList);

        // $conf = new \RdKafka\TopicConf();
        $conf->set('request.required.acks', $this->getConfig()->requiredAck);
        $conf->set('auto.offset.reset', $this->getConfig()->autoOffsetReset);
        // $conf->setDefaultTopicConf($topicConf);
        $consumer = new \RdKafka\KafkaConsumer($conf);
        $consumer->subscribe([
            $this->getConfig()->toppic
        ]);
        while (true) {
            $message = $consumer->consume(120 * 1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    ($callback instanceof \Closure) && call_user_func($callback, $message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    sleep(2);
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "time-out:". $message->errstr() . PHP_EOL;
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }

    public function consumerLow(callable $callback, $partionId = 0)
    {
        $conf = new \RdKafka\Conf();
        $conf->set('group.id', $this->getConfig()->gropuId);
        $rk = new \RdKafka\Consumer($conf);
        $rk->addBrokers($this->getConfig()->metadataBrokerList);
        $topicConf = new \RdKafka\TopicConf();
        $topicConf->set('request.required.acks', $this->getConfig()->requiredAck);
        $topicConf->set('offset.store.path', sys_get_temp_dir());
        $topicConf->set('auto.commit.enable', $this->getConfig()->autoCommitEnable);
        $topicConf->set('auto.commit.interval.ms', $this->getConfig()->autoCommitIntervalMs);
        $topicConf->set('offset.store.method', $this->getConfig()->offsetStoreMethod);
        $topicConf->set('auto.offset.reset', $this->getConfig()->autoOffsetReset);
        $topic = $rk->newTopic($this->getConfig()->toppic, $topicConf);
        $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
        while (true) {
            $message = $topic->consume(0, 12 * 1000);
            if (is_null($message)) {
                sleep(1);
                continue;
            }
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    ($callback instanceof \Closure) && call_user_func($callback, $message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }

    protected function getConfig(): ?\ClevePHP\Drives\Queues\kafka\Config
    
{
	return $this->config;
}
}

