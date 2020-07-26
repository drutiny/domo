<?php

namespace Drutiny\Plugin\Domo\Plugin;

use Drutiny\Plugin;

class DomoPlugin extends Plugin {

    /**
     * {@inheritdoc}
     */
    public function getName()
    {
        return 'domo:api';
    }

    public function configure()
    {
        $this->addField(
            'client_id',
            "Your Domo OAuth Client ID:",
            static::FIELD_TYPE_CREDENTIAL
            )
          ->addField(
            'secret',
            'Your Domo OAuth Secret:',
            static::FIELD_TYPE_CREDENTIAL
          );
    }
}

 ?>
