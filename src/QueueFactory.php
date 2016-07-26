<?php namespace Common\Queue;

use Common\Dependency\Dependency;
use PhpAmqpLib\Connection\AbstractConnection;

class QueueFactory extends Dependency
{

    const RABBIT_CONNECTION = 'rabbit_connection';

    /**
     * @return AbstractConnection
     */
    public function rabbitConnection()
    {
        return $this->packageFetch(self::RABBIT_CONNECTION);
    }

}
