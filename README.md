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


    python run.py DIAGNOSTICS -b <list of bibcodes separated by spaces>
    python run.py DIAGNOSTICS -s <list of source filenames separated by spaces>
    python run.py DIAGNOSTICS -b <list of bibcodes separated by spaces> -s <list of source filenames separated by spaces>
    
If diagnostics is run without any parameters, count of records in each of the four tables, Reference, History, Resolved, and Compare are displayed.

#####To resolve text source files:

* List source files to be processed using the command

    python run.py RAW -s <list of source filenames separated by spaces>

#####To resolve xml source files:
* It can be verified if a publisher xml source file can be parsed so that the references can be resolved. To check this


    python run.py XML -p <parse filename>
     
#####To resolve source files in a directory:
* Specify the directory, the file extension to locate in that directory, and an optional cutoff time parameter, to consider only source files with modified date after the cutoff date


    python run.py DIR -p <source files path> -e <source files extension> <cutoff date (optional)>

#####To query database for xml parser specifically:

* Specify source files to see if pipeline has parser for it


    python run.py PARSER -p <source filename>


* Specific publisher type to get a list of source files processed

    python run.py PARSER -p <publisher>


## Maintainers

Golnaz
