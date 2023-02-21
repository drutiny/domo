<?php

namespace Drutiny\Plugin\Domo;

use Drutiny\AssessmentInterface;
use Drutiny\Attribute\AsFormat;
use Drutiny\AuditResponse\AuditResponse;
use Drutiny\Policy;
use Drutiny\Profile;
use Drutiny\Report\Format;
use Drutiny\Report\FormatInterface;
use League\Csv\Writer;
use Psr\Log\LoggerInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use GuzzleHttp\Exception\ClientException;

#[AsFormat(name:'domo')]
class DomoDatasetFormat extends Format
{
    protected $content;
    protected $data;
    protected $client;

    public function __construct(Api $domo, ContainerInterface $container, LoggerInterface $logger)
    {
        $this->client = $domo;
        parent::__construct($container, $logger);
    }

    public function render(Profile $profile, AssessmentInterface $assessment):FormatInterface
    {

        $uuid = $this->uuid();
        $date = date('c', REQUEST_TIME);
        $schemas = [];
        $insert = [];

        $target = $this->container->get('target');
        $target_class = str_replace('\\', '', get_class($target));

        $schemas['Drutiny_Target_'.$target_class.'_Data'] = [
          'assessment_uuid' => [
            'type' => 'STRING', 'name' => 'assessment_uuid'
          ],
          'target' => [
            'type' => 'STRING', 'name' => 'target'
          ],
          'date' => [
            'type' => 'DATETIME', 'name' => 'date'
          ],
        ];

        $insert['Drutiny_Target_'.$target_class.'_Data'][0] = [
          'assessment_uuid' => $uuid,
          'target' => $target->getId(),
          'date' => $date,
        ];

        foreach ($target->getPropertyList() as $property_name) {
          $data = $target[$property_name];
          // Can't store objects.
          if (is_object($data)) {
            continue;
          }
          if (is_array($data) && isset($data['field_derived_key_salt'])) {
            unset($data['field_derived_key_salt']);
          }
          $schemas['Drutiny_Target_'.$target_class.'_Data'][$property_name] = [
            'type' => 'STRING', 'name' => $property_name
          ];
          $insert['Drutiny_Target_'.$target_class.'_Data'][0][$property_name] = json_encode($data);
        }

        $schemas['Drutiny_assessment_results'] = [
          'assessment_uuid' => [
            'type' => 'STRING', 'name' => 'assessment_uuid'
          ],
          'profile' => [
            'type' => 'STRING', 'name' => 'profile'
          ],
          'target' => [
            'type' => 'STRING', 'name' => 'target'
          ],
          'start' => [
            'type' => 'DATETIME', 'name' => 'reporting_period_start'
          ],
          'end' => [
            'type' => 'DATETIME', 'name' => 'reporting_period_end'
          ],
          'policy_name' => [
            'type' => 'STRING', 'name' => 'policy_name'
          ],
          'policy_title' => [
            'type' => 'STRING', 'name' => 'policy_title'
          ],
          'language' => [
            'type' => "STRING", 'name' => 'language'
          ],
          'type' => [
            'type' => "STRING", 'name' => 'type'
          ],
          'result_type' => [
            'type' => "STRING", 'name' => 'result_type'
          ],
          'result_severity' => [
            'type' => "STRING", 'name' => 'result_severity'
          ],
          'date' => [
            'type' => 'DATETIME', 'name' => 'date'
          ],
        ];

        $defaults = [
          'assessment_uuid' => $uuid,
          'profile' => $profile->name,
          'target' => $target->getId(),
          'start' => $profile->getReportingPeriodStart()->format('c'),
          'end' => $profile->getReportingPeriodEnd()->format('c'),
          'policy_name' => NULL,
          'policy_title' => NULL,
          'language' => NULL,
          'type' => NULL,
          'result_type' => NULL,
          'result_severity' => NULL,
          'date' => $date,
        ];

        foreach ($assessment->getResults() as $response) {
          $policy = $response->getPolicy();
          $dataset_name = $this->getPolicyDatasetName($policy);
          $insert_row = [];

          $dataset_columns = $this->getPolicyDatasetColumns($policy, $response);
          $dataset_columns[0]['value'] = $uuid;
          foreach ($dataset_columns as $column) {
              $column_name = $column['name'];
              $schemas[$dataset_name][$column_name] = [
                'name' => $column_name,
                'type' => $column['type'],
              ];

              $insert_row[$column_name] = $column['value'];
          }
          $insert[$dataset_name][] = $insert_row;

          $assessment_row = $defaults;
          $assessment_row['policy_name'] = $policy->name;
          $assessment_row['policy_title'] = $policy->title;
          $assessment_row['language'] = $policy->language;
          $assessment_row['type'] = $policy->type;
          $assessment_row['result_type'] = $response->getType();
          $assessment_row['result_severity'] = $response->getSeverity();
          $insert['Drutiny_assessment_results'][] = $assessment_row;
        }

        $this->content = [
          'schemas' => $schemas,
          'insert' => $insert,
        ];
        return $this;
    }

    public function write():iterable
    {
        $logger = $this->container->get('logger');
        $this->client->flushCache();
        $existing_datasets = [];
        foreach ($this->client->getDatasets() as $dataset) {
          $existing_datasets[$dataset['name']] = $dataset['id'];
        }

        foreach ($this->content['schemas'] as $dataset => $schema) {
            if (!isset($existing_datasets[$dataset])) {
              $logger->info("Creating new Dataset in Domo: $dataset.");
              $response = $this->client->createDataset($dataset, array_values($schema));
              $existing_datasets[$dataset] = $response['id'];
            }
            // Reset the schema to what is available in Domo already.
            else {
              $logger->info("Found Domo dataset $dataset: {$existing_datasets[$dataset]}");
              $info = $this->client->getDataset($existing_datasets[$dataset]);
              $this->content['schemas'][$dataset] = $info['schema']['columns'];
            }
        }

        // Append new rows.
        foreach ($this->content['insert'] as $dataset_name => $rows) {
            $schema = $this->content['schemas'][$dataset_name];
            $id = $existing_datasets[$dataset_name];
            // Ensure the cell order matches the schema.
            $data = [];
            foreach ($rows as $row) {
              $dataset_row = [];
              foreach ($schema as $column) {
                $dataset_row[] = $row[$column['name']] ?? NULL;
              }
              $data[] = $dataset_row;
            }
            $writer = Writer::createFromString();
            $writer->setEscape('');
            $writer->insertAll($data);
            $writer->setNewline("\r\n");
            //RFC4180Field::addTo($writer);
            $logger->info("Appending rows into $dataset_name ($id).");
            try {
              $this->client->appendDataset($id, $writer);
              $this->logger->info("Sent " . count($data) . " rows to dataset '$dataset_name'.");
            }
            catch (ClientException $e) {
              $this->logger->error("Failed to sent data to $dataset_name: " . $e->getMessage());
              continue;
            }

            $this->logger->debug('======='.$dataset_name.'=======');
            $this->logger->debug($writer->getContent());

            yield $dataset_name;
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
          ['type' => 'STRING', 'name' => 'assessment_uuid'],
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
