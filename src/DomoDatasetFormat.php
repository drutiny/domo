<?php

namespace Drutiny\Plugin\Domo;

use Drutiny\AssessmentInterface;
use Drutiny\AuditResponse\AuditResponse;
use Drutiny\Policy;
use Drutiny\Profile;
use Drutiny\Report\Format;
use Drutiny\Report\FormatInterface;
use Drutiny\Console\Verbosity;
use League\Csv\RFC4180Field;
use League\Csv\Writer;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Twig\Environment;

class DomoDatasetFormat extends Format
{
    protected string $name = 'domo';
    protected $content;
    protected $data;
    protected $client;

    public function __construct(Api $domo, ContainerInterface $container, Environment $twig, LoggerInterface $logger)
    {
        $this->client = $domo;
        parent::__construct($container, $logger);
    }

    public function render(Profile $profile, AssessmentInterface $assessment):FormatInterface
    {

        $uuid = $this->uuid();
        $date = date('c', REQUEST_TIME);
        $datasets = [];

        $target = $this->container->get('target');
        $target_class = str_replace('\\', '', get_class($target));


        // Remove aht.app_data.field_derived_key_salt
        $datasets['Drutiny_Target_'.$target_class.'_Data'] = [
          'assessment_uuid' => ['type' => 'STRING', 'name' => 'uuid', 'value' => $uuid],
          'target' => ['type' => 'STRING', 'name' => 'target', 'value' => $target->getId()],
          'date' => ['type' => 'DATETIME', 'name' => 'date', 'value' => $date],
        ];

        foreach ($target->getPropertyList() as $property_name) {
          $data = $target[$property_name];
          if (is_object($data)) {
            continue;
          }
          if (is_array($data) && isset($data['field_derived_key_salt'])) {
            unset($data['field_derived_key_salt']);
          }
          $datasets['Drutiny_Target_'.$target_class.'_Data'][$property_name] = [
            'type' => 'STRING',
            'name' => $property_name,
            'value' => json_encode($data),
          ];
        }

        $datasets['Drutiny_assessment_results'] = [
          'assessment_uuid' => ['type' => 'STRING', 'name' => 'assessment_uuid', 'value' => $uuid],
          'profile' => ['type' => 'STRING', 'name' => 'profile', 'value' => $profile->name],
          'target' => ['type' => 'STRING', 'name' => 'target', 'value' => $target->getId()],
          'start' => ['type' => 'DATETIME', 'name' => 'reporting_period_start', 'value' => $profile->getReportingPeriodStart()->format('c')],
          'end' => ['type' => 'DATETIME', 'name' => 'reporting_period_end',   'value' => $profile->getReportingPeriodEnd()->format('c')],
          'policy_name' => ['type' => 'STRING', 'name' => 'policy_name'],
          'policy_title' => ['type' => 'STRING', 'name' => 'policy_title'],
          'language' => ['type' => "STRING", 'name' => 'language'],
          'type' => ['type' => "STRING", 'name' => 'type'],
          'result_type' => ['type' => "STRING", 'name' => 'result_type'],
          'result_severity' => ['type' => "STRING", 'name' => 'result_severity'],
          'date' => ['type' => 'DATETIME', 'name' => 'date', 'value' => $date],
        ];

        $rows = [];
        $rows[] = [
          'dataset' => 'Drutiny_Target_'.$target_class.'_Data',
          'columns' =>   $datasets['Drutiny_Target_'.$target_class.'_Data'],
        ];

        //$datasets = $this->client->getDatasets();

        foreach ($assessment->getResults() as $response) {
          $policy = $response->getPolicy();

          $row = [
            'dataset' => 'Drutiny_assessment_results',
            'columns' => $datasets['Drutiny_assessment_results']
          ];
          $row['columns']['policy_name']['value'] = $policy->name;
          $row['columns']['policy_title']['value'] = $policy->title;
          $row['columns']['language']['value'] = $policy->language;
          $row['columns']['type']['value'] = $policy->type;
          $row['columns']['result_type']['value'] = $response->getType();
          $row['columns']['result_severity']['value'] = $response->getSeverity();
          $rows[] = $row;

          $data = $this->getPolicyDatasetColumns($policy, $response);
          $data[0]['value'] = $uuid;

          $datasets[$this->getPolicyDatasetName($policy)] = array_map(function ($r) {
              if(isset($r['value'])) unset($r['value']);
              return $r;
          }, $data);

          $rows[] = [
            'dataset' => $this->getPolicyDatasetName($policy),
            'columns' => $data,
          ];
        }

        $this->content = [
          'datasets' => $datasets,
          'rows' => $rows,
        ];
        return $this;
    }

    public function write():iterable
    {
        $logger = $this->container->get('logger');
        $this->client->flushCache();
        $datasets = $this->client->getDatasets();

        // Ensure all datasets are present and obtain their dataset id.
        foreach ($this->content['datasets'] as $name => $schema) {
            // Find the dataset if it exists.
            $exists = array_filter($datasets, function ($row) use ($name) {
                return $row['name'] == $name;
            });

            if (empty($exists)) {
                $logger->info("Creating new Dataset in Domo: $name.");
                $response = $this->client->createDataset($name, array_values($schema));
            }
            else {
                $response = reset($exists);
                $logger->info("Found Domo dataset $name: {$response['id']}");
            }
            $this->content['datasets'][$name]['id'] = $response['id'];
        }
        $this->client->flushCache();

        // Sort into datasets.
        foreach ($this->content['rows'] as $row) {
            $this->content['datasets'][$row['dataset']]['rows'][] = array_column($row['columns'], 'value');
        }

        // Append new rows.
        foreach ($this->content['datasets'] as $name => $dataset) {
            $writer = Writer::createFromString();
            $writer->setEscape('');
            $writer->insertAll($dataset['rows']);
            $writer->setNewline("\r\n");
            //RFC4180Field::addTo($writer);
            $logger->info("Appending rows into $name ({$dataset['id']}).");
            $this->client->appendDataset($dataset['id'], $writer);

            $this->logger->info("Send " . count($dataset['rows']) . " to dataset '$name'.");
            $this->logger->debug('======='.$name.'=======');
            $this->logger->debug($writer->getContent());

            yield $dataset['id'];
        }
    }

    public function getPolicyDatasetName(Policy $policy)
    {
        return 'Drutiny_Policy_'.strtr($policy->name, [
          ':' => '_',
          ]).'_results';
    }

    public function getPolicyDatasetColumns(Policy $policy, AuditResponse $response)
    {
        $columns = [
          ['type' => 'STRING', 'name' => 'uuid'],
          ['type' => "STRING", 'name' => 'target',   'value' => $this->container->get('target')['drush.alias']],
          ['type' => "STRING", 'name' => 'title',    'value' => $policy->title],
          ['type' => "STRING", 'name' => 'name',     'value' => $policy->name],
          ['type' => "STRING", 'name' => 'class',    'value' => $policy->class],
          ['type' => "STRING", 'name' => 'description', 'value' => $policy->description],
          ['type' => "STRING", 'name' => 'language', 'value' => $policy->language],
          ['type' => "STRING", 'name' => 'type',     'value' => $policy->type],
          ['type' => "STRING", 'name' => 'tags',     'value' => implode(',', $policy->tags)],
          ['type' => "STRING", 'name' => 'severity', 'value' => $policy->severity],

          ['type' => "STRING", 'name' => 'result_type',     'value' => $response->getType()],
          ['type' => "STRING", 'name' => 'result_severity', 'value' => $response->getSeverity()],
          ['type' => "DATETIME", 'name' => 'result_date',   'value' => date('c', REQUEST_TIME)],
        ];

        foreach ($policy->parameters as $key => $value) {
          $column = $this->prepareColumn($value);
          $column['name'] = 'parameters_' . $key;
          $columns[] = $column;
        }

        foreach ($response->getTokens() as $key => $value) {
          $column = $this->prepareColumn($value);
          $column['name'] = 'result_token_' . $key;
          $columns[] = $column;
        }
        return $columns;
    }

    protected function prepareColumn($value) {
      switch (gettype($value)) {
         case 'string':
             $column['type'] = "STRING";
             $column['value'] = $value;
             break;
         case 'integer':
             $column['type'] = "DECIMAL";
             $column['value'] = $value;
             break;
         case 'double':
             $column['type'] = "DOUBLE";
             $column['value'] = $value;
             break;
         case 'boolean':
             $column['type'] = "DECIMAL";
             $column['value'] = $value ? 1 : NULL;
             break;
         default:
             $column['type'] = "STRING";
             $column['value'] = json_encode($value);
             break;
      }
      return $column;
    }

    private function uuid()
    {
      $data = random_bytes(16);
      $data[6] = chr(ord($data[6]) & 0x0f | 0x40);
      $data[8] = chr(ord($data[8]) & 0x3f | 0x80);
      return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
