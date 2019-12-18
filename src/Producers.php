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
        try {
            $this->producer();
            if ($this->producer && $this->topic && $message) {
                $topic = $this->producer->newTopic($this->getConfig()->toppic, $this->topicConf);
                $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
                $len = $this->producer->getOutQLen();
                while ($len > 0) {
                    $len = $this->producer->getOutQLen();
                    $this->producer->poll(50);
                }
            } else {
                throw new \Exception("toppic is null");
            }
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    protected function producer()
    {
        try {
            if (! $this->producer) {
                $rcf = new \RdKafka\Conf();
                $rcf->set('group.id', $this->getConfig()->gropuId);
                $cf = new \RdKafka\TopicConf();
                $cf->set('offset.store.method', $this->getConfig()->offsetStoreMethod);
                $cf->set('auto.offset.reset', $this->getConfig()->autoOffsetReset);
                $cf->set('request.required.acks', $this->getConfig()->requiredAck);
                $cf->set("message.timeout.ms", $this->getConfig()->messageTimeoutMs);
                $this->topicConf = $cf;
                $rk = new \RdKafka\Producer($rcf);
                if ($this->getConfig()->debugLogLevel && method_exists($rk, "setLogLevel")) {
                    $rk->setLogLevel($this->getConfig()->debugLogLevel);
                }
                $rk->addBrokers($this->getConfig()->metadataBrokerList);
                $this->producer = $rk;
            }
            return $this->producer;
        } catch (\Throwable $e) {
            echo "======producer======".PHP_EOL;
            print_r($e->__toString());
        }
    }

    protected function getConfig(): ?\ClevePHP\Drives\Queues\kafka\Config
    {
        return $this->config;
    }
}