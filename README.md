# ADSReferencePipeline
reference resolver processing pipeline

[![Build Status](https://travis-ci.org/adsabs/ADSReferencePipeline.svg)](https://travis-ci.org/adsabs/ADSReferencePipeline)
[![Coverage Status](https://coveralls.io/repos/adsabs/ADSReferencePipeline/badge.svg)](https://coveralls.io/r/adsabs/ADSReferencePipeline)


## Short summary

This pipeline is to process source reference files, if xml to parse them first and then send to reference resolver to get matched with solr record. If reference source file is of type raw, it is sent to reference resolver to get parsed and matched there.


## Required software

    - RabbitMQ and PostgreSQL
    
    
## Setup (recommended)

    $ virtualenv python
    $ source python/bin/activate
    $ pip install -r requirements.txt
    $ pip install -r dev-requirements.txt
    $ vim local_config.py # edit, edit
    $ ./start-celery.sh


## Queues
    - task_process_reference: from input filename one reference at a time is queued for processing

## Command lines:

### To run diagnostics:
- Either supply list of bibcodes or list of source files
    ```
    python run.py DIAGNOSTICS -b <list of bibcodes separated by spaces>
    python run.py DIAGNOSTICS -s <list of source filenames separated by spaces>
    python run.py DIAGNOSTICS -b <list of bibcodes separated by spaces> -s <list of source filenames separated by spaces>
    ```

- To check if a source files can be processed by the pipeline (parser is included), use the command
    ```
    python run.py DIAGNOSTICS -p <source filename>
    ```
    
    If diagnostics is run without any parameters, count of records in each of the four tables, Reference, History, Resolved, and Compare are displayed.

### To resolve references:

- There are six options:

    1. Specify source files to be processed, regardless of format (ie, raw, any flavor xml), use the command
        ```
        python run.py RESOLVE -s <list of source filenames separated by spaces>
        ```

    2. Specify a directory, and file extension (i.e. -e *.raw), to recursively search all sub directories for this type of reference file, and queue them all for processing, use the command
        ```
        python run.py RESOLVE -p <source files path> -e <source files extension>
        ```

    3. To reprocess existing references based on confidence cutoff value, use the command
        ```
        python run.py RESOLVE -c <confidence cutoff>
        ```
        where all the references having score value lower than cutoff shall be queued for reprocessing.
        
    4. To reprocess existing references based on resolved bibcode's bibstem, use the command
        ```
        python run.py RESOLVE -b <resolved reference bibstem>
        ```
        where all the references having this bibstem shall be queued for reprocessing.

    5. To reprocess existing references based on resolved bibcode's year, use the command
        ```
        python run.py RESOLVE -y <resolved reference year>
        ```
        where all the references having this year shall be queued for reprocessing.
        
    6. To reprocess existing references that were queued but were not resolved due to reference service issue, use the command
        ```
        python run.py RESOLVE -f
        ```
        where any reference that were queued but not resolved shall be reprocessed.

    Note that there is an optional parameter that can be combined with cases *ii* - *v*, to filter results on time. Include the parameter
    
        -d <days>
    to filter on time. For the case *ii*, this parameter is applied to source file, if timestamp of the file is later than past *days*, the file shall be queued for processing. For the cases *iii* - *v* the time is applied to resolved references run, if they were processed in the past *days*, they shall be queue for reprocessing. 

### To query database:

- To get a list of source files processed from a specified publisher, use the command 
    ```
    python run.py STATS -p <publisher>
    ```

- To see the result of resolved records for specific source bibcode/filename, use the command
    ```
    python run.py STATS -b <source bibcode>
    python run.py STATS -s <source filename>
    ```

- To see number of rows in the four main tables, use the command
    ```
    python run.py STATS -c
    ```


## Maintainers

Golnaz
