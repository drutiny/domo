<?php

namespace Drutiny\Plugin\Domo;

class Column {
    public function __construct(
        public readonly string $type,
        public readonly string $name
    ) {}
}