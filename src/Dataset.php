<?php

namespace Drutiny\Plugin\Domo;

class Dataset {
    public function __construct(
        public readonly string $name,
        public readonly Schema $schema,
        public readonly string $description = '',
    ) {}

    public function forJson():array {
        $vars = get_object_vars($this);
        $vars['schema'] = $this->schema->forJson();
        return $vars;
    }
}