# Data pipeline

Data pipeline was created is part of checkpoint 1 of semester project for OWS. The original idea was to use Kafka message broker, but it would require additional installation of that software and there is no easy to use docker immage which could take its place. So, I rather went for 'new' for me python framework [Luigi](https://github.com/spotify/luigi). 
Luigi has many outputs also so semi-steps of  a pipeline, but to make it simple, only local files are used.

## Pipeline structure

(all path are relative to git root)
* data_pipeline/ - python source code
* config/ - data pipeline configuration files
* log/ - logging output of data pipeline
* META/ - files for states of pipeline pieces which need that
* tmp/ - outputs of pipeline pieces 
* results/ - folder with merged rdf files

## Information sources
 At this moment just public transportation irregularities of Prague and Brno are collected because they are vita for other checkpoint of that semester work.
 
* Prague public transportation irregularity:
    * http://www.dpp.cz/rss/doprava/
    * http://www.dpp.cz/rss/mimoradne-udalosti/
* Brno public transportation irregularity:
    * http://dpmb.cz/cs/vsechna-omezeni-dopravy
    
## How to run it

* Option 1: run python script with python scheduler

```python run_data_pipeline.py```

* Option 2: add follow command to crontab with your scheduling options, for special identification is recommended to use timestamp

```luigi --module data_pipeline.pipeline DataPipeline --unique-param <speial identification>```

