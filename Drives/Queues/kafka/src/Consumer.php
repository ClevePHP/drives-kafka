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

    /**
             * 获取topic有多少分区
     *
     * @return []
     */
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
    public function consumer(callable $callback, $partionId = 0)
    {
        $conf = new \RdKafka\Conf();
        $conf->set('group.id', $this->getConfig()->gropuId);
        $rk = new \RdKafka\Consumer($conf);
        $rk->addBrokers($this->getConfig()->metadataBrokerList);
        $topicConf = new \RdKafka\TopicConf();
        $topicConf->set('auto.commit.interval.ms', $this->getConfig()->metadataRefreshIntervalMs);
        $topicConf->set('offset.store.method', $this->getConfig()->offsetStoreMethod);
        $topicConf->set('offset.store.path', sys_get_temp_dir());
        $topicConf->set('auto.offset.reset', 'smallest');
        $topic = $rk->newTopic($this->getConfig()->toppic, $topicConf);
        // Start consuming partition 0
        $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
        while (true) {
            $message = $topic->consume($partionId, 120 * 10000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    ($callback instanceof \Closure) && call_user_func($callback, $message);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "receive...".PHP_EOL;
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "timeout...".PHP_EOL;
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

