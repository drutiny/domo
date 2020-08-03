<?php

namespace Drutiny\Plugin\Domo\Audit;

use Drutiny\Sandbox\Sandbox;
use Drutiny\Audit\AbstractAnalysis;
use Drutiny\Plugin\Domo\Api;

class DatasetQuery extends AbstractAnalysis {

    public function configure()
    {
        $this->addParameter(
          'dataset',
          AbstractAnalysis::PARAMETER_REQUIRED,
          'The name of the dataset to query.',
        );
        $this->addParameter(
          'query',
          AbstractAnalysis::PARAMETER_REQUIRED,
          'The SQL query to run against the dataset.',
        );
        parent::configure();
    }


    public function gather(Sandbox $sandbox)
    {
        $api = $this->container->get('domo.api');
        $datasets = $api->getDatasets();
        $dataset = $datasets[$this->getParameter('dataset')];

        $sql = $this->interpolate($this->getParameter('query'), [
          'reportingPeriod.start' => $sandbox->getReportingPeriodStart()->format('c'),
          'reportingPeriod.end' => $sandbox->getReportingPeriodEnd()->format('c'),
        ]);
        $this->logger->info($sql);
        $sql = str_replace("\n", " ", $sql);

        $rows = $api->queryDatasetKeyed($dataset['id'], $sql);
        $this->set('tickets', $rows);
    }
}
