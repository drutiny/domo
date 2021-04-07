<?php

namespace Drutiny\Plugin\Domo;

use Drutiny\Http\Client;
use Drutiny\Plugin\Domo\Plugin\DomoPlugin;
use Symfony\Contracts\Cache\CacheInterface;
use Symfony\Contracts\Cache\ItemInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\Console\Helper\ProgressBar;
use Psr\Http\Message\MessageInterface;
use League\Csv\Writer;


/**
 * API client for CSKB.
 */
class Api {

  protected $client;
  protected $cache;
  protected $plugin;
  protected $progress;

  public function __construct(Client $client, CacheInterface $cache, ContainerInterface $container, DomoPlugin $plugin, ProgressBar $progress)
  {
      $this->progress = $progress;
      $this->plugin = $plugin->load();
      $this->cache = $cache;
      $this->client = $client->create([
        'base_uri' => $container->getParameter('domo.api.base_uri'),
        'headers' => [
          'User-Agent' => 'drutiny-cli/3.x',
        //  'Accept' => 'application/vnd.api+json',
          'Accept-Encoding' => 'gzip',
        ],
        'decode_content' => 'gzip',
        'allow_redirects' => FALSE,
        'connect_timeout' => 10,
        'verify' => FALSE,
        'timeout' => 300,
      ]);
  }

  /**
   * Get the OAuth token for API requests
   */
  protected function getToken() {
    return $this->cache->get('domo.api.oauth_token', function (ItemInterface $item) {
      $this->progress->setMessage("Retriving Domo OAuth Token");
      $response = $this->client->get('/oauth/token', [
        'auth' => [$this->plugin['client_id'], $this->plugin['secret']],
        'query' => [
          'grant_type' => 'client_credentials',
          'scope' => 'data',
        ]
      ]);
      $data = json_decode($response->getBody(), true);
      $item->expiresAfter($data['expires_in'] - 1);
      return $data;
    });
  }

  /**
   * Wrapper for the API client.
   */
  protected function call(string $method, string $path, array $parameters = []):MessageInterface
  {
    try {
      $token = $this->getToken();
      $parameters['headers']['Authorization'] = 'Bearer ' . $token['access_token'];
      $this->progress->setMessage("Making call to $method $path...");
      return $this->client->request($method, $path, $parameters);
    }
    catch (\GuzzleHttp\Exception\ClientException $e) {
      $response = $e->getResponse();
      $json = json_decode($response->getBody(), true);

      // Detect expired tokens and reattempt.
      if (isset($json['error']) && $json['error'] == 'invalid_token') {
        $this->cache->delete('domo.api.oauth_token');
        return $this->call($method, $path, $parameters);
      }

      throw $e;
    }
    throw new \Exception("Unexpected outcome to API call attempt to Domo.");
  }

  /**
   * Flush cache.
   */
  public function flushCache()
  {
    $this->cache->delete('domo.api.datasets');
    return $this;
  }

  /**
   * Get a list of datasets. This can take a while...
   */
  public function getDatasets()
  {
        return $this->cache->get('domo.api.datasets', function (ItemInterface $item) {
          $datasets = [];

          $offset=0;
          $limit = 50;

          do {
            $response = $this->call('GET', '/v1/datasets', ['query' => [
                'offset' => $offset,
                'limit' => $limit,
              ]]);
            $data = json_decode($response->getBody(), true);

            foreach ($data as $dataset) {
              $datasets[] = $dataset;
            }
            $offset = count($datasets);
          }
          while (count($data) == $limit);

          return $datasets;
      });
  }

  public function getDatasetByName($name)
  {
    foreach ($this->getDatasets() as $dataset) {
      if ($dataset['name'] == $name) return $this->getDataset($dataset['id']);
    }
    throw new \Exception("No such dataset found: $name.");
  }

  public function getDataset($dataset_id)
  {
    $cid = 'domo.dataset.'.$dataset_id;
    return $this->cache->get($cid, function  (ItemInterface $item) use ($dataset_id) {
      $response = $this->call('GET', '/v1/datasets/'.$dataset_id);
      return json_decode($response->getBody(), true);
    });
  }

  public function queryDataset($dataset_id, $sql_query)
  {
    $cid = 'domo.dataset.query.'.$dataset_id.hash('md5', $sql_query);
    $dataset = $this->cache->get($cid, function  (ItemInterface $item) use ($dataset_id, $sql_query) {
      $response = $this->call('POST', '/v1/datasets/query/execute/'.$dataset_id, [
        'json' => [
          'sql' => $sql_query
        ]
      ]);
      return json_decode($response->getBody(), true);
    });
    return $dataset['rows'];
  }

  /**
   * Dataset query with keyed result set.
   */
  public function queryDatasetKeyed($dataset_id, $sql_query)
  {
    $cid = 'domo.dataset.query.'.$dataset_id.hash('md5', $sql_query);
    $dataset = $this->cache->get($cid, function  (ItemInterface $item) use ($dataset_id, $sql_query) {
      $response = $this->call('POST', '/v1/datasets/query/execute/'.$dataset_id, [
        'json' => [
          'sql' => $sql_query
        ]
      ]);
      return json_decode($response->getBody(), true);
    });

    $rows = [];
    $headers = $dataset['columns'];
    foreach ($dataset['rows'] as $row) {
      $rows[] = array_combine($headers, $row);
    }
    return $rows;
  }

  /**
   * Create dataset.
   */
  public function createDataset(string $name, array $columns)
  {
    $this->progress->setMessage("Creating dataset in Domo '$name'");
    $response = $this->call('POST', '/v1/datasets', [
      'json' => [
        'name' => $name,
        'description' => 'Drutiny table for '.$name,
        'rows' => 0,
        'schema' => [
          'columns' => $columns
        ],
      ]
    ]);
    $this->flushCache();
    return json_decode($response->getBody(), true);
  }

  public function deleteDataset($dataset_id)
  {
    $this->progress->setMessage("Deleting dataset in Domo '$dataset_id'");
    $response = $this->call('DELETE', '/v1/datasets/'.$dataset_id);
    $this->flushCache();
    return json_decode($response->getBody(), true);
  }

  /**
   * Append dataset.
   */
  public function appendDataset($dataset_id, Writer $writer)
  {
    $response = $this->call('PUT', '/v1/datasets/'.$dataset_id.'/data', [
      'headers' => [
        'Content-Type' =>  'text/csv',
      ],
      'query' => ['updateMethod' => 'APPEND'],
      'body' => $writer->getContent(),
    ]);
    return json_decode($response->getBody(), true);
  }
}
