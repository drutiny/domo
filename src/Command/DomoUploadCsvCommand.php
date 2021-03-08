<?php

namespace Drutiny\Plugin\Domo\Command;

use Drutiny\Plugin;


use Drutiny\Config\Config;
use Drutiny\Plugin\Domo\Plugin\DomoPlugin;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Drutiny\Console\Command\DrutinyBaseCommand;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\Finder\Finder;
use League\Csv\Reader;
use League\Csv\Statement;
use League\Csv\RFC4180Field;
use League\Csv\Writer;
use Drutiny\Plugin\Domo\Api;

/**
 *
 */
class DomoUploadCsvCommand extends DrutinyBaseCommand
{
    protected ContainerInterface $container;
    protected DomoPlugin $plugin;
    protected array $datasets;
    protected array $schemas;
    protected array $domoDatasets;
    protected array $domoSchemas;
    protected Api $client;
    protected $logger;

    public function __construct(ContainerInterface $container, DomoPlugin $plugin, Api $client)
    {
        $this->container = $container;
        $this->plugin = $plugin;
        $this->client = $client;
        $this->logger = $container->get('logger');
        parent::__construct();
    }

    /**
     * @inheritdoc
     */
    protected function configure()
    {
        $this
        ->setName('domo:upload:csv')
        ->setDescription('Upload csv files into domo.')
        ->addOption(
            'report-dir',
            'o',
            InputOption::VALUE_OPTIONAL,
            'Where to look for csv files to upload',
            getenv('PWD')
        )
        ->addOption(
            'remove-csv',
            null,
            InputOption::VALUE_NONE,
            'Remove the csv files when uploaded.'
        );
    }

    /**
     * @inheritdoc
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $io = new SymfonyStyle($input, $output);
        $finder = new Finder();
        $finder->files()
               ->in($input->getOption('report-dir'))
               ->name('*.csv');

        $this->datasets = [];

        $files = [];
        foreach ($finder as $file) {
          $dataset_name = $this->getDatasetName($file);

          $reader = Reader::createFromFileObject($file->openFile('r'));
          $reader->setHeaderOffset(0);

          $records = Statement::create()->process($reader);

          if (!isset($this->schemas[$dataset_name])) {
            $this->schemas[$dataset_name] = $records->getHeader();
          }

          foreach ($records as $record) {
            $this->datasets[$dataset_name][] = $record;
          }
          $files[] = $file->getRealPath();
        }

        if (empty($this->datasets)) {
          $io->success("No CSV files found. No upload required.");
          return 0;
        }

        $progress = $this->getProgressBar(count($this->datasets));
        $progress->start();
        $progress->setMessage("Sending CSV files to Domo...");

        $this->syncDatasetSchemas();

        if ($input->getOption('remove-csv')) {
          foreach ($files as $file) {
            $this->logger->notice("Removing " . $file);
            unlink($file);
          }
        }

        $progress->finish();
        $io->success("Upload complete.");

        return 0;
    }

    /**
     * Get the name of the CSV dataset.
     */
    protected function getDatasetName(\SplFileInfo $file):string
    {
      $name = $file->getFilename();
      list($dataset_name, $target_metadata) = explode('__', $name);
      return $dataset_name;
    }

    protected function syncDatasetSchemas()
    {
      foreach ($this->client->getDatasets() as $dataset) {
        $this->domoDatasets[$dataset['name']] = $dataset['id'];
      }
      foreach ($this->datasets as $dataset_name => $rows) {
          if (!isset($this->domoDatasets[$dataset_name])) {
            $this->logger->notice("Creating new Dataset in Domo: $dataset_name.");
            $this->domoSchemas[$dataset_name] = $this->buildDomoSchema($dataset_name);
            $response = $this->client->createDataset($dataset_name, $this->domoSchemas[$dataset_name]);
            $this->domoDatasets[$dataset_name] = $response['id'];
          }
          // Set the schema to what is available in Domo already.
          else {
            $this->logger->notice("Found Domo dataset $dataset_name: {$this->domoDatasets[$dataset_name]}");
            $info = $this->client->getDataset($this->domoDatasets[$dataset_name]);
            $this->domoSchemas[$dataset_name] = $info['schema']['columns'];
            $this->domoDatasets[$dataset_name] = $info['id'];
          }

          $data = [];
          foreach ($this->datasets[$dataset_name] as $row) {
            $dataset_row = [];
            foreach ($this->domoSchemas[$dataset_name] as $column) {
              $dataset_row[] = $row[$column['name']] ?? NULL;
            }
            $data[] = $dataset_row;
          }

          $writer = Writer::createFromString();
          $writer->setEscape('');
          $writer->insertAll($data);
          $writer->setNewline("\r\n");
          //RFC4180Field::addTo($writer);
          $this->logger->notice("Appending rows into $dataset_name.");
          try {
            $this->client->appendDataset($this->domoDatasets[$dataset_name], $writer);
            $this->logger->notice("Sent " . count($data) . " rows to dataset '$dataset_name'.");
          }
          catch (ClientException $e) {
            $this->logger->error("Failed to sent data to $dataset_name: " . $e->getMessage());
            continue;
          }
          finally {
            $this->getProgressBar()->advance();
          }
      }


    }

    protected function buildDomoSchema(string $dataset_name):array
    {
      $schema = [];
      $sample = $this->datasets[$dataset_name][0];
      foreach ($this->schemas[$dataset_name] as $header) {
        $schema[$header] = $this->prepareColumn($sample[$header]);
        $schema[$header]['name'] = $header;

        switch ($header) {
          case 'result_date':
          case 'reporting_period_start':
          case 'reporting_period_end':
            $schema[$header]['type'] = 'DATETIME';
            break;
        }
      }
      return $schema;
    }

    protected function prepareColumn($value):array
    {
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
}
