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
use League\Csv\ResultSet;
use Drutiny\Plugin\Domo\Api;
use GuzzleHttp\Exception\ClientException;

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

        $datasets = [];

        $files = [];

        // Reporting directory may contain multiple files that belong to the
        // same dataset so we read these files all into memory so we only
        // send one upload call per dataset.
        foreach ($this->findCsvFiles($input) as $file) {
          $data = $this->readCsvData($file);

          if (empty($data)) continue;

          $name = $this->getDatasetName($file);
          foreach ($data as $row) {
            $datasets[$name][] = $row;
          }
          $files[$name][] = $file->getRealPath();
        }

        // Mo files, mo problems.
        if (empty($files)) {
          $io->success("No CSV files found. No upload required.");
          return 0;
        }

        $progress = $this->getProgressBar();
        $progress->start(count($datasets));
        $progress->setMessage("Sending CSV files to Domo...");

        // Upload datasets. If this fails, then remove the files from the
        // $files array to prevent them from being deleted. This allows the
        // data to be inspected and corrected so it can be uploaded.
        $failure = 0;
        foreach ($datasets as $name => $records) {
          try {
            $this->uploadDataset($name, $records);
          }
          catch (\Exception $e) {
            $failure++;
            $this->logger->error("Failed to send data to $name: " . $e->getMessage());
            unset($files[$name]);
          }
          $this->getProgressBar()->advance();
        }

        if ($input->getOption('remove-csv')) {
          foreach ($files as $dataset => $filenames) {
            foreach ($filenames as $file) {
              $this->logger->notice("Removing " . $file);
              unlink($file);
            }
          }
        }

        $progress->finish();
        $io->writeln("Upload complete.");

        return $failure;
    }

    protected function findCsvFiles(InputInterface $input):Finder
    {
      $finder = new Finder();
      $finder->files()
             ->in($input->getOption('report-dir'))
             ->name('*.csv');
      return $finder;
    }

    protected function readCsvData(\SplFileInfo $file):ResultSet
    {
      $reader = Reader::createFromFileObject($file->openFile('r'));
      $reader->setHeaderOffset(0);

      return Statement::create()->process($reader);
    }

    protected function buildSchemaFromData(array $data):array
    {
      $schema = [];

      foreach (reset($data) as $k => $v) {
        $schema[] = $this->prepareColumn($k, $v);
      }

      return $schema;
    }

    /**
     * Get the name of the CSV dataset.
     */
    protected function getDatasetName(\SplFileInfo $file):string
    {
      list($name, ) = explode('__', $file->getFilename(), 2);
      return $name;
    }

    /**
     * Upload data to Domo.
     */
    protected function uploadDataset(string $name, array $records):void
    {
      try {
        $dataset = $this->client->getDatasetByName($name);
        $method = 'retrieving';
      }
      catch (\Exception $e) {
        $this->logger->warning($e->getMessage());
        $dataset = $this->client->createDataset($name, $this->buildSchemaFromData($records));
        $method = 'creating';
      }

      $rows = [];

      if (!isset($dataset['schema'])) {
        $this->logger->error("Found dataset '$name' ({$dataset['id']}) with no schema while $method dataset. Deleting...");
        $this->client->deleteDataset($dataset['id']);
        throw new \Exception("Found dataset '$name' ({$dataset['id']}) with no schema while $method dataset.");
      }

      $headers = array_map(fn ($h) => $h['name'], $dataset['schema']['columns']);

      foreach ($records as $r) {
        $rows[] = array_map(fn ($h) => $r[$h] ?? NULL, $headers);

        $missing_headers = array_diff(array_keys($r), $headers);
        if (!empty($missing_headers)) {
          $this->logger->warning("Dataset '$name' is missing schema columns for " . implode(', ', $missing_headers));
        }
      }

      $writer = Writer::createFromString();
      $writer->setEscape('');
      $writer->insertAll($rows);
      $writer->setNewline("\r\n");
      //RFC4180Field::addTo($writer);

      $this->logger->notice("Appending " . count($rows) . " rows into $name.");
      $this->client->appendDataset($dataset['id'], $writer);
    }

    protected function prepareColumn($name, $value):array
    {
      $column = $this->prepareColumnValue($value);
      unset($column['value']);
      $column['name'] = $name;

      switch ($name) {
        case 'result_date':
        case 'reporting_period_start':
        case 'reporting_period_end':
          $column['type'] = 'DATETIME';
          break;
      }
      return $column;
    }

    protected function prepareColumnValue($value):array
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
