# Extraktion des Linkkontexts
`links_in_context` enthält den Quellcode für die Extraktion sowie die CSV-Dateien `Collection_Seeds.csv` und `Exclude_Domains.csv`.
`Collection_Seeds.csv` enthält eine Liste der Startdomains der Sammlung, aus denen die Links extrahiert werden.
Da die Domains bereits Teil der Sammlung sind, werden Links auf sie bei der Extraktion verworfen.
In `Exclude_Domains.csv` können weitere Domains hinzugefügt werden, die für die Archivierung nicht in Frage kommen und deshalb
ebenfalls bei der Extraktion nicht berücksichtigt werden sollen.

## Nutzung mit Docker
Das Skript setzt voraus, dass die auszuwertenden Daten in der folgenden Struktur abgelegt sind:
```shell
Eingabeverzeichnis
|- Zeitschnitt1
    |- arcs
        |- XY-1.warc.gz
        |- XY-2.warc.gz
        |- ...
|- Zeitschnitt2
    |- arcs
        |- XY-1.warc.gz
        |- XY-2.warc.gz
        |- ...
    
```
Passe Ein- und Ausgabeverzeichnisse in `docker-compose.yml` an:
```
    volumes:
      - /path/to/warcs:/in:ro
      - /path/to/store/results:/out:rw
```
Starte Extraktion im Docker Container mit `docker-compose up`.

## Entwicklung
Lade aktuellstes AUT-Python Paket von Github herunter und entpacke es:
```shell
curl https://github.com/archivesunleashed/aut/releases/download/aut-0.90.2/aut-0.90.2.zip
unzip aut-0.90.2.zip
```
Installiere sonstige Python Abhängigkeiten:
```shell
pip install -r requirements.txt
```
Setze `JAVA_HOME`.  
Führe Tests aus:
```shell
python -m pytest test
```