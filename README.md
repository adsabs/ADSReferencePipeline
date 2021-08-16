# ADSReferencePipeline
reference resolver processing pipeline

[![Build Status](https://travis-ci.org/adsabs/ADSReferencePipeline.svg)](https://travis-ci.org/adsabs/ADSReferencePipeline)
[![Coverage Status](https://coveralls.io/repos/adsabs/ADSReferencePipeline/badge.svg)](https://coveralls.io/r/adsabs/ADSReferencePipeline)


## Short summary

This pipeline is to process source reference files, if xml to parse them first and then send to reference resolver to get matched with solr record. If reference source file is of type raw, it is sent to reference resolver to get parsed and matched there.


## Setup (recommended)

    $ virtualenv python
    $ source python/bin/activate
    $ pip install -r requirements.txt
    $ pip install -r dev-requirements.txt
    $ vim local_config.py # edit, edit


### Config options for users
* 


## Queues
* 


### Command lines:

#####To run diagnostics:
* Either supply list of bibcodes or list of source files
    ```
    python run.py DIAGNOSTICS -b <list of bibcodes separated by spaces>
    python run.py DIAGNOSTICS -s <list of source filenames separated by spaces>
    python run.py DIAGNOSTICS -b <list of bibcodes separated by spaces> -s <list of source filenames separated by spaces>
    ```

* To check if a source files can be processed by the pipeline (parser is included), use the command
    ```
    python run.py DIAGNOSTICS -p <source filename>
    ```
    
If diagnostics is run without any parameters, count of records in each of the four tables, Reference, History, Resolved, and Compare are displayed.

#####To resolve references:

* There are six options:

    1. Specify source files to be processed, regardless of source file format (ie, raw, any flavor xml), use the command
        ```
        python run.py RESOLVE -s <list of source filenames separated by spaces>
        ```

    2. Specify a directory, and file extension, to recursively search all sub directories for this type of reference file, and queue them all for processing, use the command
        ```
        python run.py RESOLVE -p <source files path> -e <source files extension>
        ```

    3. To reprocess existing references based on confidence cutoff value, use the command
        ```
        python run.py RESOLVE -c <confidence cutoff>
        ```
        where all the references with this score lower than this shall be queued for reprocessing.
        
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
        
    6. To reprocess existing references that were queued but for some reason they were not able to get resolved (ie, service issue), use the command
        ```
        python run.py RESOLVE -f
        ```
        where any reference that were queued but not resolved shall be reprocessed.

    Note that there is an optional parameter that can be combined with cases numbers 2 - 5, to filter references on time. Include the parameter
    
        -d <days>
    to filter on time. For the case of command #2, this parameter is applied to source file, if timestamp of the file is later than specified past many days, the file shall be queued for processing. For the case of commands numbers 3 - 5 the time is applied to when the references where run last, if they were processed in the past specified days, they shall be queue for reprocessing. 

#####To query database:

* To get a list of source files processed from a specified publisher, use the command 
    ```
    python run.py STATS -p <publisher>
    ```

* To see the result of resolved records for specific file/bibcode, use the command
    ```
    python run.py STATS -s <source filename>
    python run.py STATS -b <bibcode>
    ```


## Maintainers

Golnaz
