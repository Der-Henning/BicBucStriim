<?php
/**
 * BicBucStriim -- SOLR Addon
 *
 * Copyright 2020 Henning Merklinger
 * Licensed under MIT License, see LICENSE
 *
 */

require 'vendor/autoload.php';

class Solr
{
    protected $client = NULL;
    protected $app = NULL;

    function __construct($app, $solrConfig)
    {
        $this->client = new Solarium\Client($solrConfig);
        $ping = $this->client->createPing();
        $this->app = $app;

        try {
            $this->client->ping($ping);
            $app->getLog()->info("Solr connected successfully");
        } catch (Exception $e) {
            $app->getLog()->error($e->getMessage());
        }
    }

    function docsSlice($use_solr = true, $index = 0, $length = 100, $search = NULL)
    {
        global $app;
        $entries = array();
        $no_pages = 0;
        $no_entries = 0;
        if ($use_solr){
            try{
                $query = $this->client->createSelect();
                $edismax = $query->getEDisMax();
                $edismax->setQueryFields('title_deu title_eng title_ge authors tags_deu tags_eng tags_ge page_deu_* page_eng_* page_ge_*');

                $query->setQuery($search);
                $query->setStart($index * $length)->setRows($length);
                $query->setFields(array('id', 'score'));

                $resultset = $this->client->select($query);

                $no_entries = $resultset->getNumFound();
                $no_pages = ceil($no_entries / $length);
                
                foreach ($resultset as $document) {
                    $book = $this->app->calibre->title($document['id']);
                    if ($book != NULL) {
                        $book->score = $document["score"];
                        $entries[] = $book;
                    }
                }
            } catch (Exception $e) {
                $app->getLog()->error($e->getMessage());
            }
        }

        return array('page' => $index, 'pages' => $no_pages, 'entries' => $entries, 'total' => $no_entries);
    }
}

?>
