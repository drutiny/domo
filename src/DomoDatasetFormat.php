<?php

namespace Drutiny\Plugin\Domo;

use Drutiny\Attribute\AsFormat;
use Drutiny\Attribute\UseService;
use Drutiny\Report\Format\CSV;
use Fiasco\TabularOpenapi\Table;
use League\Csv\Writer;
use GuzzleHttp\Exception\ClientException;

#[AsFormat(name:'domo')]
#[UseService(Api::class, 'setDomoApi')]
class DomoDatasetFormat extends CSV
{
    protected readonly Api $client;
    protected array $existingDatasets = [];

    public function setDomoApi(Api $domo)
    {
        $this->client = $domo;
    }

    protected function writeTable(Table $table):string
    {
        $dataset = 'Drutiny_' . $table->name;
        if (!isset($this->existingDatasets[$dataset])) {
            $this->logger->info("Creating new Dataset in Domo: $dataset.");
            $row = $table->fetch(0)->current();
            $row['_table'] = $table->uuid;
            $response = $this->client->createDataset($dataset, $this->buildDatasetSchemaFromRow($row));
            $this->existingDatasets[$dataset] = $response['id'];
        }
        $info = $this->client->getDataset($this->existingDatasets[$dataset]);
        $headers = [];
        foreach ($info['schema']['columns'] as $column) {
          $headers[] = $column['name'];
        }

        $writer = Writer::createFromString();
        $writer->setEscape('');
        $writer->setNewline("\r\n");
        $writer->insertOne($headers);

        $rows = 0;
        foreach ($table->fetchAll() as $values) {
          $values['_table'] = $table->uuid;
          $row = [];
          // Ensure the table values come out the right way.
          foreach ($headers as $header) {
            $row[$header] = $values[$header] ?? null;
          }
          $writer->insertOne($row);
          $rows++;
        }

        $this->logger->info("Appending rows into $dataset ({$this->existingDatasets[$dataset]}).");
        try {
          $this->client->appendDataset($this->existingDatasets[$dataset], $writer);
          $this->logger->info("Sent $rows rows to dataset '$dataset'.");
        }
        catch (ClientException $e) {
          $this->logger->error("Failed to sent data to $dataset: " . $e->getMessage());
        }
        return $table->name . ' to ' . $this->client->getDatasetUrl($this->existingDatasets[$dataset]);
    }

    protected function buildDatasetSchemaFromRow(array $row):array {
      $schema = [];
      foreach ($row as $header => $value) {
        $schema[] = [
          'name' => $header,
          'type' => $this->mapDataType($value)
        ];
      }
      return $schema;
    }

    protected function mapDataType($value):string {
      return match(gettype($value)) {
        'string' => "STRING",
        'integer' => "DECIMAL",
        'double' => "DOUBLE",
        'boolean' => "DECIMAL",
         default => "STRING",
      };
    }
}
