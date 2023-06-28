<?php

namespace Drutiny\Plugin\Domo;

class Schema {
    public readonly array $columns;
    public function __construct(Column ...$columns)
    {
        $this->columns = $columns;
    }

    static public function fromColumns(array $columns):static {
        return new static(...array_map(fn ($c) => new Column(...$c), $columns));
    }

    public function forJson():array {
        $vars = get_object_vars($this);
        $vars['columns'] = array_map(fn($c) => get_object_vars($c), $this->columns);
        return $vars;
    }
}