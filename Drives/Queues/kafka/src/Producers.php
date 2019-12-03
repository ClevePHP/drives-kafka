<?php
namespace ClevePHP\Drives\Queues\kafka;

class Producers
{

    protected $producer;

    protected $topic;

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

    /**
                *   实例化
     *
     * @param \ClevePHP\Drives\Queues\kafka\Config $config
     */
    public function config(\ClevePHP\Drives\Queues\kafka\Config $config)
    {
        $this->config = $config;
        $this->topic = $config->toppic;
        unset($config);
        return $this;
    }

    /**
             * 发消息
     *
     * @return \RdKafka\Producer
     */
    public function produce($message)
    {
        try {
            $this->producer();
            if ($this->producer && $this->topic && $message) {
                $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
                $this->producer->poll(0);
                while ($this->producer->getOutQLen() > 0) {
                    $this->producer->poll($this->getConfig()->produceInterval);
                }
            } else {
                throw new \Exception("toppic is null");
            }
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
                * 生产者
     *
     * @return \RdKafka\Producer
     */
    protected function producer()
    {
        if (! $this->producer) {
            $producer = new \RdKafka\Producer();
            $producer->setLogLevel(LOG_DEBUG);
            $producer->addBrokers($this->getConfig()->metadataBrokerList);
            $this->topic = $producer->newTopic($this->getConfig()->toppic);
            $this->producer = $producer;
        }
        return $this->producer;
    }

    protected function getConfig(): ?\ClevePHP\Drives\Queues\kafka\Config
    {
        return $this->config;
    }
}