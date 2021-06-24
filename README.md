# Suche im Linkkontext zum Ausbau von Webarchivsammlungen
Der Ausbau von Webarchivsammlungen soll unterstützt werden, indem aus den gesammelten
Webarchiven zunächst die ausgehenden Links extrahiert und anschließend ihre thematische Relevanz für die Sammlung bewertet. 
Neben der Ziel-URL und -domain werden für jeden Link auch der Ankertext und Linkkontext extrahiert und für die Suche aufbereitet. 
Linkkontext bezeichnet hier den Textinhalt des Elternelements, in das der Linktag eingebettet ist.
Die extrahierten Linkdaten werden zusätzlich mit Metadaten aus dem OPAC angereichert, mit denen Bibliothekar\*innen
Nach der Indexierung der Linkkontexte können Bibliothekar*innen gezielt nach Links auf Webinhalte zu bestimmten Themen suchen
oder explorativ die Links durchstöbern, die von einer Website zu einem bestimmten Thema ausgehen.
Zur Auswertung der Linkkontexte wurden zwei verschiedene Ansätze getestet: Die Volltextsuche mit
[Apache Solr](https://lucene.apache.org/solr/) sowie die Textähnlichkeitssuche mit
[Sentence-BERT](https://github.com/UKPLab/sentence-transformers/).

## Extraktion des Linkkontexts aus Webarchiven
Die Linkinformationen werden mithilfe des [Archives Unleashed Toolkit](https://github.com/archivesunleashed/aut/tree/aut-0.80.0)
aus den WARC-Dateien extrahiert. Die Beispielskripte zur Extraktion und Filterung der Linkinformationen
liegen im Ordner `extraction` dieses Repositories.
Ergebnis der Extraktion ist eine Datei im JSON Lines Format, welche die extrahierten Links
als JSON-Elemente mit den Feldern `src_host` (Host, von dem der Link ausgeht), `dest` (Ziel-URL des Links), `dest_host` (Ziel-Host), `anchor` (Ankertexte),
`context` (Linkkontexte) `inv_path_depth` (inverse Pfadtiefe) enthält.

## Anreicherung mit Metadaten aus dem OPAC
Die archivierten Websites, aus denen die Links stammen, wurden bereits von Bibliothekar*innen mit thematischen Schlagworten versehen.
Diese Schlagworte können aus dem Bibliothekskatalog über die MarcXML Schnittstelle abgefragt und zu den Linkextrakten hinzugefügt werden (Feld `src_annotations`).
Auf diese Weise können die Nutzer\*innen später die Links zusätzlich anhand der Schlagworte filtern.

## Volltextsuche mit Apache Solr
Die Konfiguration des Solr Servers befindet sich im Ordner `solr/conf`. Es wird empfohlen, das deutsche Wörterbuch für die Zerlegung zusammengesetzter Wörter
(`dictionary_de.txt`) zu erweitern. Ein gutes Beispielwörterbuch findet sich im Repository [german-decompounder](https://github.com/uschindler/german-decompounder).
Starte den Docker-Container mit der Solr-Instanz:
```shell
cd solr
docker-compose up
```
Das Admin-Interface ist unter [http://localhost:8988](http://localhost:8988) zugänglich.  
Nun können die Linkdaten aus der JSON-Datei indexiert werden:
```sh
cd ..
curl 'http://localhost:8988/solr/link-index/update/json/docs?f=src_domain:/src_host&f=/src_annotations&f=url:/dest&f=domain:/dest_host&f=/anchor&f=/context&f=/inv_path_depth&commit=true' --data-binary @data/sample_bavarica.json -H 'Content-type:application/json'
```
Als Interface für die Volltextsuche mit dem neu angelegten Index kann das Jupyter Notebook
`full_text_search.ipynb` im Ordner `notebooks` in diesem Repository genutzt werden.

## Textähnlichkeitssuche mit Sentence-Transformers
[Sentence-Transformers](https://sbert.net) ist ein Python Framework für neuronale Netzwerke, die als Eingabe natürlichsprachige Sätze erhalten und
diese so auf einen Vektorraum abbildet, dass Sätze mit ähnlichem semantischem Gehalt nahe
beieinander liegen. Diese Darstellung kann für die Textähnlichkeitssuche genutzt werden, indem
der Kosinus des Winkels zwischen den Vektoren berechnet und als Maß für die semantische
Ähnlichkeit genutzt wird. Mehr zum theoretischen Hintergrund findet sich in der Publikation:
[Sentence-BERT: Sentence Embeddings using Siamese BERT-Networks](https://arxiv.org/abs/1908.10084).
In unserem Experiment kommt ein vortrainiertes Modell zum Einsatz, das mit mehreren Sprachen umgehen kann („distiluse-base-multilingual-cased“).  
Für die Vektorsuche nutzen wir die Suchmaschine [vespa](https://vespa.ai/). Die Konfiguration der Suchmaschine für unser
Beispiel befindet sich im Ordner `vespa/application` in diesem Repository.

### Satzembeddings berechnen
Als erstes wird der Linkkontext in eine Vektordarstellung übersetzt und im vespa-Format abgelegt.
Dieser Schritt kann einige Zeit in Anspruch nehmen.
```shell
python vespa/create_embeddings.py  data/sample_bavarica.json data/sample_bavarica_embedded.json
```

### Indexierung in vespa
Starte den vespa Docker Container:
```shell
docker run -m 8G --detach --name vespa-linkcontext --hostname vespa-linkcontext \
    --publish 8080:8080 --publish 19112:19112 --publish 19071:19071 \
    vespaengine/vespa
```
Installiere die vespa Anwendung, die für die Linkkontextsuche konfiguriert wurde:
```shell
cd vespa
tar -C application -cf - . | gzip | \
curl --header Content-Type:application/x-gzip --data-binary @- \
localhost:19071/application/v2/tenant/default/prepareandactivate
```
Lade das Indexierungswerkzeug herunter, das von vespa bereit gestellt wird:
```shell
curl -L -o vespa-http-client-jar-with-dependencies.jar \
https://search.maven.org/classic/remotecontent?filepath=com/yahoo/vespa/vespa-http-client/7.391.28/vespa-http-client-7.391.28-jar-with-dependencies.jar
```
Indexiere die Daten
```shell
java -jar vespa-http-client-jar-with-dependencies.jar \
    --verbose --file ../data/sample_bavarica_embedded.json --endpoint http://localhost:8080
```
Als Interface für die Textähnlichkeitssuche mit dem neu angelegten Index kann das Jupyter Notebook
`semantic_search.ipynb` im Ordner `notebooks` in diesem Repository genutzt werden.