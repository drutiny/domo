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
class DomoDatasetListCommand extends DrutinyBaseCommand
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
        ->setName('domo:dataset:list')
        ->setDescription('Upload csv files into domo.');
    }

    /**
     * @inheritdoc
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $io = new SymfonyStyle($input, $output);

        foreach ($this->client->getDatasets() as $dataset) {
          $rows[] = [$dataset['name'], $dataset['id'], $dataset['rows'], $dataset['createdAt'], $dataset['updatedAt'], $dataset['owner']['name']];
        }

        $io->table(['Name', 'ID', 'Rows', 'Created', 'Updated', 'Owner'], $rows);
        return 0;
    }

}
