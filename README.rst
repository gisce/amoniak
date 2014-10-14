amoniak
=======

AMON Library and utils to convert GISCE-ERP objects to AMON data.

The objects that can be converted to AMON format are: Profiles, Devices, Contracts, Metering points.
Also there is a command line executable to send data to the Empowering Insight Engine, and can be
used to initialize the data from the utility and/or sending data to the Insight Engine in batch mode.

-------
Install
-------

.. code-block:: shell

  $ pip install amoniak


---------------
Running amoniak
---------------

.. code-block:: shell

  $ amoniak --help
  
------------------
Available commands
------------------

All the commands work with environment variables, see the next section for available options.

By default all commands work with RQ Queues, but it can be run in sync mode using environment variables
(See working with RQ section) or with the parametter ``--no-async`` in the amoniak command.


* **Enqueue all AMON measures**: Used to initialize Empowering Insight Engine Service.
  
  .. code-block:: shell
  
    $ amoniak enqueue_all_amon_measures
    
* **Enqueue measures**: Enqueue new measures for the contracts which have and etag searching the last measure
  created in the Insight Engine
  
  .. code-block:: shell
  
    $ amoniak enqueue_measures

* **Enqueue contracts**: Used to upload contracts. This command will upload firstly the updated contracts:
  the ones which have an etag and have been updated after the ``_updated``. Then will upload new contracts:
  searching for the ones which have a smart metter and no etag in the contract and the contracte was created
  after the las ``_updated`` in the Insight Engine.
  
  .. code-block:: shell
  
    $ amoniak enqueue_contracts


---------------
Running workers
---------------

Workers are the default RQ Workers but you must setup the necessary environment variables to work properly

To execute contracts's tasks

.. code-block:: shell

  $ rqworker contracts
  
  
To execute measures's tasks

.. code-block:: shell

  $ rqworker measures


----------------------------------
Working with environment variables
----------------------------------


Empowering services
-------------------

To work with **Empowering services** you must define the following environment variables:

* EMPOWERING_COMPANY_ID
* EMPOWERING_KEY_FILE
* EMPOWERING_CERT_FILE (This can be the same as EMPOWERING_KEY_FILE)

If you want to work with empowering debug server you have to define EMPOWERING_DEBUG


Working with ERPPeek
--------------------

* PEEK_SERVER
* PEEK_DB
* PEEK_USER
* PEEK_PASSWORD


Working with MongoDB
--------------------

* MONGODB_HOST
* MONGODB_DATABASE


Working with Sentry
-------------------

* SENTRY_DSN

Be sure to use a synchronous protocol


Working with RQ
---------------

* RQ_ASYNC 

