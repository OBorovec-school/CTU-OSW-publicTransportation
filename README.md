# Ontologies and Semantic Web - Semestral work

## Requirements

### Cp00

Subtopic selection and its short description, review of relevant public data sources (existing data sets, existing ontologies, books, web pages). Expected subtopics are (but not limited to) construction (car typology/components, aircraft typology/components, …), operation (time-tables, flight plans), safety (occurrence/accident reports), infrastructure (roads, flight routes, cycling routes). Some resources that might help you find useful data are below. We expect you to find also other data sources.

The subtopic you select (e.g. Aviation Safety in the Czech Republic) must involve at least two data sources provided by two different parties/organizations (e.g. “Safety Accident Database by Air Investigation Institute” and “Aircraft register by Civil Aviation Authority”).

* Deliverable
    * data1-2 pages PDF describing subtopic selection, your motivation for the topic, data source selection and short description (how complex the data schema is, how much data the data source contains, what kind of data it contains, etc).

### Cp01
Create semi-automated data pipeline, based on the data sources from checkpoint 0.

* Deliverable
    * data pipeline (Scrapy, SPARQL, etc.) for reconstructing the data set;
    * the data set;
    * UML diagrams of the data set schema
* Details
    * the data pipeline should transform data-sources into RDF (i.e. target data set)
    * choose any tools you like (e.g. any programming language you are familiar with) to create the data pipeline. However, for the most of the cases the following two alternatives should be sufficient to use:
        * GraphDB (OpenRefine+SPARQL) for processing CSV files, triplifying them, and manipulating resulting RDF
        * Scrapy + GraphDB (OpenRefine+SPARQL) for scraping web pages, triplifying them, and manipulating the resulting RDF
    * the resulting data set should contain all relevant data for the integration task unified in format (RDF)
    
### Cp02

Align your datasets with the integration ontology, extend the integration ontology with classes/properties specific to your subtopic. Integrate data set with other well-known data sets.

* Details
    * at least 100 mutually interlinked resources, compliant with the OSW ontology, as well as well-known external ontologies, published in your GraphDB repository
    * design of at least 3 non-trivial interesting SPARQL queries over the datasets you create, leveraging the integrated datasets,
    * (Optional) possibly equipped with an application processing the data (R, Java, Python, etc.).    
    
## Checkpoint solutions
* Cp00 - 'doc/projectDescription'
* Cp01 - Python code and README.md in 'data_pipeline' and parsed data in 'sample_data' folder
* Cp02 - TBD    

## Checkpoint 0

I decided to focus at public transportation in Prague and Brno, combining theirs irregularities with reasoning from other sources. To see all report, pls follow this [link](docs/projectDesc/projectDesc.pdf)

## Checkpoint 1

Data pipeline was created is part of checkpoint 1 of semester project for OWS. The original idea was to use Kafka message broker, but it would require additional installation of that software and there is no easy to use docker immage which could take its place. So, I rather went for 'new' for me python framework [Luigi](https://github.com/spotify/luigi). 
Luigi has many outputs also so semi-steps of  a pipeline, but to make it simple, only local files are used.

### Pipeline structure

(all path are relative to git root)
* data_pipeline/ - python source code
* config/ - data pipeline configuration files
* log/ - logging output of data pipeline
* META/ - files for states of pipeline pieces which need that
* tmp/ - outputs of pipeline pieces 
* results/ - folder with merged rdf files

### Information sources
 At this moment just public transportation irregularities of Prague and Brno are collected because they are vita for other checkpoint of that semeser work.
 
* Prague public transportation irregularity:
    * http://www.dpp.cz/rss/doprava/
    * http://www.dpp.cz/rss/mimoradne-udalosti/
* Brno public transportation irregularity:
    * http://dpmb.cz/cs/vsechna-omezeni-dopravy