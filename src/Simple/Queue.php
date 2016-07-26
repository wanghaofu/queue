<?php namespace Common\Queue\Simple;

use Common\Queue\Interfaces\IJobFailPolicyRetry;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

abstract class Queue extends Base
{
    /**
     * @var AMQPChannel
     */
    protected $delayedChannel;

    protected static $instances = [];

    /**
     * @return static
     */
    public static function instance()
    {
        if (!isset(self::$instances[static::class])) {
            self::$instances[static::class] = new static();
        }

        return self::$instances[static::class];
    }

    protected function __construct()
    {
        parent::__construct();
        $exchangeName = $this->getExchangeName();
        $queueName = $this->getQueueName();

        $this->channel->exchange_declare($exchangeName, 'direct', false, true, false);
        $this->channel->queue_declare($queueName, false, true, false, false);
        $this->channel->queue_bind($queueName, $exchangeName);
        $this->channel->basic_qos(null, 1, null);
    }

    /**
     * @param string $message
     */
    protected function enqueue($message, array $property = [])
    {
        $exchangeName = $this->getExchangeName();
        $channel = $this->getChannel();

        $msg = $this->wrapMessage($message, $property);

        $channel->basic_publish($msg, $exchangeName);
    }

    protected function delayedEnqueue($message, $delaySec, array $property = [])
    {
        if ($delaySec >= 8640000) {//最多100天
            throw new \Exception(__METHOD__ . '/' . __LINE__);
        }

        $exchangeName = $this->getDelayedExchangeName();
        $channel = $this->getDelayedChannel();

        $msg = $this->wrapMessage($message, [
                self::PROP_EXPIRATION => max(1, $delaySec * 1000),
            ] + $property);

        $channel->basic_publish($msg, $exchangeName);
    }

    public function work($maxCount = 500, $timeout = 0)
    {
        $consumerTag = 'csm-' . $this->machineString();
        $class = get_class($this);

        $count = 0;
        $callback = function (AMQPMessage $msg) use ($class, $consumerTag, &$count, $maxCount) {
            /**
             * @var AMQPChannel $channel
             */

            $channel = $msg->delivery_info['channel'];
            $delivery_tag = $msg->delivery_info['delivery_tag'];

            try {
                $this->consume($msg->body);

                $channel->basic_ack($delivery_tag);
            } catch (\Exception $e) {
                $message = $e->getMessage();
                $message .= " {$class}";
                $this->logger->error(
                    $message,
                    [
                        'queue_msg' => $msg->body,
                        'exception' => $e,
                    ]
                );

                switch (true) {
                    case $e instanceof IJobFailPolicyRetry:
                        $channel->basic_reject($delivery_tag, true);
                        break;
//                    case $e instanceof IJobFailPolicyOtherQueue:
//                        $this->handlePushOtherQueueException($msg, $e);
//                        break;
                    default:
                        $channel->basic_ack($delivery_tag);
                }
            }

            if (++$count >= $maxCount) {
                $channel->basic_cancel($consumerTag);
            }
        };

        $this->basicConsume($callback, $consumerTag);

        try {
            $this->wait($timeout);
        } catch (AMQPTimeoutException $e) {
        }
    }

    /**
     * process one single message
     * @param string $message
     */
    abstract protected function consume($message);

    protected function basicConsume(callable $callback, $consumerTag)
    {
        $queueName = $this->getQueueName();
        $channel = $this->getChannel();

        $channel->basic_consume($queueName, $consumerTag, false, false, false, false, $callback);
    }

    protected function wait($timeout)
    {
        $channel = $this->getChannel();

        while (count($channel->callbacks)) {
            $channel->wait(null, false, $timeout);
        }
    }

    /**
     * @return string
     */
    protected function getExchangeName()
    {
        return "exchange:{$this->name}";
    }

    /**
     * @return string
     */
    protected function getQueueName()
    {
        return "queue:{$this->name}";
    }


    /**
     * @return \PhpAmqpLib\Channel\AMQPChannel
     */
    protected function getChannel()
    {
        return $this->channel;
    }

    /**
     * @param $message
     * @return AMQPMessage
     */
    protected function wrapMessage($message, $properties = [])
    {
        return new AMQPMessage($message, $properties + [
                self::PROP_CONTENT_TYPE => 'text/plain',
                self::PROP_DELIVERY_MODE => 2,
            ]);
    }

    /**
     * @return string
     */
    protected function getDelayedExchangeName()
    {
        $exchangeName = $this->getExchangeName();
        return "$exchangeName:pending";
    }

    protected function getDelayedQueueName()
    {
        $queueName = $this->getQueueName();

        return "$queueName:pending";
    }

    protected function getDelayedChannel()
    {
        if (!isset($this->delayedChannel)) {
            $exchangeName = $this->getExchangeName();
            $delayedExchangeName = $this->getDelayedExchangeName();
            $delayedQueueName = $this->getDelayedQueueName();

            $this->delayedChannel = $this->connection->channel();

            $this->delayedChannel->exchange_declare($delayedExchangeName, 'direct', false, true, false);
            $this->delayedChannel->queue_declare($delayedQueueName, false, true, false, false, false, new AMQPTable([
                self::ARG_X_DEAD_LETTER_EXCHANGE => $exchangeName,
            ]));
            $this->delayedChannel->queue_bind($delayedQueueName, $delayedExchangeName);
        }

        return $this->delayedChannel;
    }
}
