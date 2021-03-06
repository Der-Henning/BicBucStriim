<?php

namespace Solarium\Core\Query\Result;

use Solarium\Core\Query\ResponseParserInterface;
use Solarium\Exception\UnexpectedValueException;

/**
 * QueryType result.
 */
class QueryType extends Result
{
    /**
     * Lazy load parsing indicator.
     *
     * @var bool
     */
    protected $parsed = false;

    /**
     * Parse response into result objects.
     *
     * Only runs once
     *
     * @throws UnexpectedValueException
     */
    protected function parseResponse()
    {
        if (!$this->parsed) {
            $responseParser = $this->query->getResponseParser();
            if (!$responseParser || !($responseParser instanceof ResponseParserInterface)) {
                throw new UnexpectedValueException('No responseparser returned by querytype: '.$this->query->getType());
            }

            $this->mapData($responseParser->parse($this));

            $this->parsed = true;
        }
    }

    /**
     * Map parser data into properties.
     *
     * @param array $mapData
     */
    protected function mapData(array $mapData)
    {
        foreach ($mapData as $key => $data) {
            $this->{$key} = $data;
        }
    }
}
