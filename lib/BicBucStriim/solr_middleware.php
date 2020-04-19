<?php
/**
 * BicBucStriim -- SOLR Addon
 *
 * Copyright 2020 Henning Merklinger
 * Licensed under MIT License, see LICENSE
 *
 */

require 'vendor/autoload.php';

require_once 'lib/BicBucStriim/solr.php';

class SolrMiddleware extends \Slim\Middleware {



	/**
     * Initialize the configuration
     *
     * @param array $config
     */
    public function __construct() {

    }

	public function call() {
		global $globalSettings;
		$app = $this->app;

		$solrConfig = array(
			'endpoint' => array(
				'localhost' => array(
					'host'    => $globalSettings['solr_host'],
					'port'    => $globalSettings['solr_port'],
					'path'    => $globalSettings['solr_path'],
					'core'    => $globalSettings['solr_core'],
					'timeout' => $globalSettings['solr_timeout'],
				)
			)
		);

		$app->solr = new Solr($app, $solrConfig);

		$this->next->call();
	}
}
?>
