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
 
* Public transportation irregularity:
    * Prague 
        * http://www.dpp.cz/rss/doprava/
        * http://www.dpp.cz/rss/mimoradne-udalosti/
    * Brno
        * http://dpmb.cz/cs/vsechna-omezeni-dopravy
    * Pislen
        * http://www.pmdp.cz/zmeny-v-doprave.xml
* Traffic info:
    * Dopravni informace (more [here](http://portal.dopravniinfo.cz/rss-informace#))
        * http://www.dopravniinfo.cz/DataServices/RSSpraha.ashx
        * http://www.dopravniinfo.cz/DataServices/RSSbrno.ashx
    
## How to run it

* Option 1: run python script with python scheduler

```python run_data_pipeline.py```

* Option 2: add follow command to crontab with your scheduling options, for special identification is recommended to use timestamp

```luigi --module data_pipeline.pipeline DataPipeline --unique-param <speial identification>```

## Processing examples

### Public transportation Prague

```
<item>
  <title><![CDATA[Chýně, Pivovarský dvůr - Zličín (oba směry) (26.01. 20:20 - 26.01. 22:05)]]></title>
  <description><![CDATA[]]></description>
<content_encoded><![CDATA[
	<section></section>
<emergency_types>Odklon, Neobsloužení zastávek</emergency_types>
<time_start>2017-01-26 20:20:00</time_start>
<time_stop>2017-01-26 22:05:00</time_stop>
<time_final_stop>2017-01-26 22:05:00</time_final_stop>
	<integrated_rescue_system>0</integrated_rescue_system>
<aff_line_types>
															   <line>BUS</line>                              	    </aff_line_types>
	<aff_lines>347</aff_lines>
]]></content_encoded>      
<link>http://www.dpp.cz/mimoradne-udalosti/1_1086/</link>
  <pubDate>Thu, 26 Jan 2017 20:55:11 +0100</pubDate>
  <guid isPermaLink="false">261325-1_1086</guid>
</item>
```

transformed to 

```
<http://publictransportation.cz/prague/irregularity/8c4333667ceb6160ee6f3fdac71d00fb> a ns1:TrafficChange ;
    pragueChanges:affectedLines [ a rdf:Bag ;
            rdf:li "347"^^xsd:string ] ;
    pragueChanges:affectedTypes [ a rdf:Bag ;
            rdf:li "BUS "^^xsd:string ] ;
    pragueChanges:classification [ a rdf:Bag ;
            rdf:li " Neobsloužení zastávek"^^xsd:string,
                "Odklon"^^xsd:string ] ;
    pragueChanges:ends "2017-01-26T22:05:00+01:00"^^xsd:datetime ;
    pragueChanges:link <http://www.dpp.cz/mimoradne-udalosti/1_1086/> ;
    pragueChanges:published "2017-01-26T20:55:11+01:00"^^xsd:datetime ;
    pragueChanges:starts "2017-01-26T20:20:00+01:00"^^xsd:datetime ;
    pragueChanges:title "Chýně, Pivovarský dvůr - Zličín (oba směry) (26.01. 20:20 - 26.01. 22:05)"^^xsd:string .
```

### Public transportation Brno


```
<div class="selInfo" style="float: left; height: auto; padding: 0px 0px 0px 0px; display: inline-block; vertical-align:top; min-width: 320px;">
   <div class="boxTyp" style="vertical-align:top;"><img src="/data/ImgImage/udalost.png" style="width: 40px; height: 40px;" alt="Mimořádná událost v MHD"></div>
   <div class="boxGen" style="vertical-align:top;"><img src="/data/ImgImage/vykricnik.png" style="width: 10px; height: 40px; visibility: hidden;" alt="Mimořádná událost v MHD"></div>
   <div class="boxFaze" style="vertical-align:top;">
      <div style="float: left; width: 5px; min-height: 60px; height: auto; padding: 0px 0px 0px 0px; display: inline-block; vertical-align:top; background-color: #CDCED0;" title="Ukončená"></div>
   </div>
   <div class="boxOdDo truncDate" style="vertical-align:top;">Od: 10.01.2018 09:15<br>Do: 10.01.2018 09:20</div>
   <div class="boxLink truncLnk" style="vertical-align:top;">Linky: 6, 8</div>
   <div class="boxTxt truncNps" style="vertical-align:top;">Porucha vozidla - Zastávka Starý Lískovec-smyčka </div>
</div>
<div class="selInfo1" style="display: block; float: left; width: calc(100% - 110px); height: auto; padding: 0px 0px 10px 15px; margin-left: 75px; border-width: 5px; border-left-style: solid; border-color: rgb(205, 206, 208);" id="div_3753">
    Zastávka: Starý Lískovec-smyčka<br>Směr: do centra<br>Důvod: Porucha vozidla<br>Zdržení: 5 min
</div>
```

transformed to 

```
<http://publictransportation.cz/brno/changes/1601a8fb372be7cf062af3909293c34e> a ns1:TrafficChange ;
    brnoChanges:affecedStop "Starý Lískovec-smyčka"^^xsd:string ;
    brnoChanges:affectedLines [ a rdf:Bag ;
            rdf:li "8"^^xsd:string ] ;
    brnoChanges:classification [ a rdf:Bag ;
            rdf:li "Mimořádná událost v MHD"^^xsd:string,
                "Porucha vozidla"^^xsd:string ] ;
    brnoChanges:delay_amount "5 min"^^xsd:string ;
    brnoChanges:dirrection "do centra"^^xsd:string ;
    brnoChanges:ends "2018-04-01T04:45:00"^^xsd:datetime ;
    brnoChanges:possibleSolution "Cestující převezme záložní tramvaj se zpožděním 7 minut."^^xsd:string ;
    brnoChanges:reason "Porucha vozidla"^^xsd:string ;
    brnoChanges:starts "2018-04-01T04:40:00"^^xsd:datetime ;
    brnoChanges:title "Porucha vozidla - Zastávka Starý Lískovec-smyčka "^^xsd:string .
```

### Public transportation Pilsen


```
<item>
<title><![CDATA[Změny na trolejbusových a autobusových linkách od 2. 1. 2018, zahájení odbavování v nové zastávce "Belánka"]]></title>
<description><![CDATA[ Trolejbusy 

 S platností od 2. 1. 2018 pojedou trolejbusové linky dle nových grafikonů. Na základě došlých požadavků dochází k úpravám odjezdů vybraných spojů a intervalů. 

 Současně bude od 2. 1. 2018 zahájeno odbavování v nové zastávce „Belánka“ (směr Jižní Předměstí) umístěné v Borské ulici za]]></description>
<link>http://www.pmdp.cz/informace-o-preprave/zmeny-v-doprave/doc/zmeny-na-trolejbusovych-a-autobusovych-linkach-od-2-1-2018-zahajeni-odbavovani-v-nove-zastavce-belanka-2251/trafficitem.htm</link>
<pubDate>Tue, 19 Dec 2017 11:53:00 GMT</pubDate>
</item>
<item>
```

transformed to 

```
<http://publictransportation.cz/pilsen/changes/573f05194b185e93be1378bb6de2c905> pilsenChanges:link <http://www.pmdp.cz/informace-o-preprave/zmeny-v-doprave/doc/zmeny-na-trolejbusovych-a-autobusovych-linkach-od-2-1-2018-zahajeni-odbavovani-v-nove-zastavce-belanka-2251/trafficitem.htm> ;
    pilsenChanges:published "2017-12-19T11:53:00+00:00"^^xsd:datetime ;
    pilsenChanges:title "Změny na trolejbusových a autobusových linkách od 2. 1. 2018, zahájení odbavování v nové zastávce \"Belánka\""^^xsd:string .

```

### Dopravni informace

```
<item>
<title>
ulice Na vápence, mezi křižovatkami ulic Jeseniova a Jilmová, Praha 3,: Práce na silnici
</title>
<link>
http://www.dopravniinfo.cz/default.aspx?rssdetail=1&e=-738991,-738917,-1043316,-1043426
</link>
<description>práce na vodovodním potrubí ,</description>
<pubDate>Wed, 10 Jan 2018 12:02:21 GMT</pubDate>
<guid isPermaLink="false">5459538</guid>
</item>
```

transformed to 

```
<http://publictransportation.cz/prague/ti_di/3a467eec2289a670b4379659fdf07b24> a pragueDi:TrafficInfo ;
    pragueDi:classification "Práce na silnici"^^xsd:string ;
    pragueDi:description "práce na vodovodním potrubí  ,"^^xsd:string ;
    pragueDi:internalId "5459538"^^xsd:string ;
    pragueDi:link <http://www.dopravniinfo.cz/default.aspx?rssdetail=1&e=-738991,-738917,-1043316,-1043426> ;
    pragueDi:published "2018-01-10T12:02:21+00:00"^^xsd:datetime ;
    pragueDi:title "ulice Na vápence, mezi křižovatkami ulic Jeseniova a Jilmová, Praha 3,: Práce na silnici"^^xsd:string .
```