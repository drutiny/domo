<?php

namespace Drutiny\Plugin\Domo\Command;

use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Drutiny\Console\Command\DrutinyBaseCommand;
use Drutiny\Plugin\Domo\Api;

/**
 *
 */
class DomoDatasetListCommand extends DrutinyBaseCommand
{
    protected array $domoSchemas;

    public function __construct(protected Api $client)
    {
        $this->client = $client;
        parent::__construct();
    }

    /**
     * @inheritdoc
     */
    protected function configure()
    {
        $this
        ->setName('domo:dataset:list')
        ->setDescription('List datasets from Domo.');
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
