<?php

namespace Drutiny\Plugin\Domo\Audit;

use Drutiny\Sandbox\Sandbox;
use Drutiny\Audit\AbstractAnalysis;
use Drutiny\Plugin\Domo\Api;

class DatasetQuery extends AbstractAnalysis {

    public function configure():void
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
        $name = $this->getParameter('dataset');

        foreach ($datasets as $set) {
          if ($set['name'] == $name) {
            $dataset = $set;
            break;
          }
        }

        if (!isset($dataset)) {
          throw new \InvalidArgumentException("Cannot find dataset: ".$this->getParameter('dataset'));
        }

        $sql = $this->interpolate($this->getParameter('query'), [
          'reportingPeriod.start' => $sandbox->getReportingPeriodStart()->format('c'),
          'reportingPeriod.end' => $sandbox->getReportingPeriodEnd()->format('c'),
        ]);
        $this->logger->info($sql);
        $sql = str_replace("\n", " ", $sql);

        $rows = $api->queryDatasetKeyed($dataset['id'], $sql);
        $this->set('rows', $rows);
    }
}
