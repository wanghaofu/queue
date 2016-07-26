<?php namespace Common\Queue\Simple;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;


abstract class RpcQueue extends Base
{
    const MESSAGE_HEADER_FAILED = 'x';
    const MESSAGE_HEADER_OK = 'o';
    const DEFAULT_TIMEOUT = 5;
    protected $timeout = self::DEFAULT_TIMEOUT;

    /**
     * 积压超过此数值报警
     */
    const WARN_TASKCOUNT = 25;

    abstract protected function process($msg);

    protected function enqueue($msg, array $properties = [])
    {
        $start = microtime(true);
        $response = false;
        $correlationId = uniqid($this->machineString());

        list(, $messageCount) = $this->channel->queue_declare($this->getQueueName(), true);
        if ($messageCount > self::WARN_TASKCOUNT) {
            $this->logger->notice(__METHOD__, [
                'count' => $messageCount
            ]);
        }

        list($callbackQueueName) = $this->channel->queue_declare('', false, false, true, false);
        $this->channel->basic_consume($callbackQueueName, '', false, true, false, false,
            function (AMQPMessage $reply) use (&$response, $correlationId) {
                if ($reply->get('correlation_id') === $correlationId) {
                    $response = $reply->body;
                }
            });

        $msg = new AMQPMessage($msg, [
                self::PROP_CORRELATION_ID => $correlationId,
                self::PROP_REPLY_TO => $callbackQueueName,
            ] + $properties);

        $this->channel->basic_publish($msg, '', $this->getQueueName());

        while (!$response && microtime(true) - $start < $this->timeout) {
            $this->channel->wait(null, false, $this->timeout);
        }

        return $this->parseResponse($response);
    }

    protected function getQueueName()
    {
        return "rpc:{$this->name}";
    }

    public function work($maxCount = 500, $timeout = 0)
    {
        $this->channel->queue_declare($this->getQueueName(), false, false, false, false);
        $this->channel->basic_qos(null, 1, null);
        $class = get_class($this);
        $consumerTag = 'csm-' . $this->machineString();

        $callback = function (AMQPMessage $req) use ($class, $consumerTag, &$count, $maxCount) {
            /**
             * @var AMQPChannel $channel
             */
            $channel = $req->delivery_info['channel'];

            try {
//                echo "got req >>>", $req->body, PHP_EOL;
                $result = $this->process($req->body);
//                echo "send reqs <<<", $result, ' | ', $req->get('correlation_id'), '|', $req->get('reply_to'), PHP_EOL;

                $this->response($req, self::MESSAGE_HEADER_OK . (string)$result);
            } catch (\Exception $e) {
                $this->handleException($e, $req);
            }

            if (++$count >= $maxCount) {
                $channel->basic_cancel($consumerTag);
            }
        };

        $this->channel->basic_consume($this->getQueueName(), $consumerTag, false, false, false, false, $callback);

        try {
            while (count($this->channel->callbacks)) {
                $this->channel->wait(null, false, $timeout);
            }
        } catch (AMQPTimeoutException $e) {
            //noop
        }
    }

    protected function parseResponse($response)
    {
        if (!is_string($response) || $response[0] !== self::MESSAGE_HEADER_OK) {
            return false;
        } else {
            return substr($response, 1);
        }
    }

    protected function handleException(\Exception $e, AMQPMessage $req)
    {
        $this->logger->notice(__METHOD__, [
            'queue_msg' => $req->body,
            'exception' => $e,
        ]);

        $this->response($req, self::MESSAGE_HEADER_FAILED);
    }

    protected function response(AMQPMessage $req, $msg)
    {
        /**
         * @var AMQPChannel $channel
         */
        $channel = $req->delivery_info['channel'];
        $deliveryTag = $req->delivery_info['delivery_tag'];

        $res = new AMQPMessage(
            $msg,
            [
                self::PROP_CORRELATION_ID => $req->get(self::PROP_CORRELATION_ID),
            ]
        );
        $channel->basic_publish($res, '', $req->get(self::PROP_REPLY_TO));
        $channel->basic_ack($deliveryTag);
    }
}
