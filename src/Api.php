<?php

namespace Drutiny\Plugin\Domo;

use Drutiny\Http\Client;
use Drutiny\Plugin\Domo\Plugin\DomoPlugin;
use Symfony\Contracts\Cache\CacheInterface;
use Symfony\Contracts\Cache\ItemInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * API client for CSKB.
 */
class Api {

  protected $client;
  protected $cache;
  protected $plugin;

  public function __construct(Client $client, CacheInterface $cache, ContainerInterface $container, DomoPlugin $plugin)
  {
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

  protected function getToken() {
    return $this->cache->get('domo.api.oauth_token', function (ItemInterface $item) {
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

  public function getDatasets()
  {
    // $this->cache->delete('domo.api.datasets');
    return $this->cache->get('domo.api.datasets', function (ItemInterface $item) {
      $datasets = [];
      $token = $this->getToken();

      $offset=0;
      $limit = 50;

      do {
        $response = $this->client->get('/v1/datasets', [
          'headers' => [
            'Authorization' => 'Bearer ' . $token['access_token'],
          ],
          'query' => [
            'offset' => $offset,
            'limit' => $limit,
          ]
        ]);
        $data = json_decode($response->getBody(), true);

        foreach ($data as $dataset) {
          $datasets[$dataset['name']] = $dataset;
        }
        $offset = count($datasets);
      }
      while (count($data) == $limit);

      return $datasets;
    });
  }

  public function queryDataset($dataset_id, $sql_query)
  {
    $cid = 'domo.dataset.query.'.$dataset_id.hash('md5', $sql_query);
    $dataset = $this->cache->get($cid, function  (ItemInterface $item) use ($dataset_id, $sql_query) {
      $token = $this->getToken();
      $response = $this->client->request('POST', '/v1/datasets/query/execute/'.$dataset_id, [
        'headers' => [
          'Authorization' => 'Bearer ' . $token['access_token'],
        ],
        'json' => [
          'sql' => $sql_query
        ]
      ]);
      return json_decode($response->getBody(), true);
    });
    return $dataset['rows'];
  }

  public function queryDatasetKeyed($dataset_id, $sql_query)
  {
    $cid = 'domo.dataset.query.'.$dataset_id.hash('md5', $sql_query);
    $dataset = $this->cache->get($cid, function  (ItemInterface $item) use ($dataset_id, $sql_query) {
      $token = $this->getToken();
      $response = $this->client->request('POST', '/v1/datasets/query/execute/'.$dataset_id, [
        'headers' => [
          'Authorization' => 'Bearer ' . $token['access_token'],
        ],
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
}
