<?php namespace Common\Queue\Simple;

use Common\Logger\Logger;
use Common\Queue\QueueFactory;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;

class Base implements LoggerAwareInterface
{

    use LoggerAwareTrait;
    const NAME = null;
    const ARG_X_DEAD_LETTER_EXCHANGE = "x-dead-letter-exchange";
    
    const PROP_EXPIRATION = 'expiration';
    const PROP_REPLY_TO = 'reply_to';
    const PROP_CORRELATION_ID = 'correlation_id';
    const PROP_CONTENT_TYPE = 'content_type';
    const PROP_DELIVERY_MODE = 'delivery_mode';
    protected $name;
    /**
     * @var AMQPChannel
     */
    protected $channel;
    /**
     * @var AbstractConnection
     */
    protected $connection;

    /**
     * get name of the queue
     *
     * @return string
     */
    protected function getName()
    {
        $name = static::NAME;
        if (empty($name)) {
            $name = static::class;
        }

        return $name;
    }

    protected function __construct()
    {
        $this->name = $this->getName();
        $this->logger = Logger::instance();
        $this->connection = QueueFactory::instance()->rabbitConnection();
        $this->channel = $this->connection->channel();
    }


    protected function machineString()
    {
        $class = get_class($this);
        $pid = getmypid();

        //from \Rhumsaa\Uuid\Uuid::getNodeFromSystem
        ob_start();
        switch (strtoupper(substr(php_uname('a'), 0, 3))) {
            case 'WIN':
                passthru('ipconfig /all 2>&1');
                break;
            case 'DAR':
                passthru('ifconfig 2>&1');
                break;
            case 'LIN':
            default:
                passthru('netstat -ie 2>&1');
                break;
        }

        $node = '';
        $ifconfig = ob_get_clean();
        $pattern = '/[^:]([0-9A-Fa-f]{2}([:-])[0-9A-Fa-f]{2}(\2[0-9A-Fa-f]{2}){4})[^:]/';
        $matches = array();

        // Search the ifconfig output for all MAC addresses and return
        // the first one found
        if (preg_match_all($pattern, $ifconfig, $matches, PREG_PATTERN_ORDER)) {
            $node = $matches[1][0];
            $node = str_replace(':', '', $node);
            $node = str_replace('-', '', $node);
        }


        return "$class-$pid-$node";
    }
}
