# Musik-Nutzungsdaten-Pipeline-Architektur

Please see English below

Dies ist die Lösung für eine Coding-Challenge. Die Aufgabe bestand darin, eine Pipeline zu erstellen, die CSV-Dateien einliest, die Daten transformiert, mittels einer externen API anreichert und in einer Datenbank speichert.

## Architektur

Dies ist eine Kommandozeilenanwendung. Sie verwendet eine Variante einer geschichteten Softwarearchitektur, wobei besonderer Wert auf Skalierbarkeit gelegt wurde.

![alt text](https://github.com/DoomFungus/Musik-Nutzungsdaten-Pipeline/blob/main/Gema.png?raw=true)

Die Verantwortlichkeiten sind wie folgt aufgeteilt: Das cli-Modul übernimmt die Interaktion mit der Schnittstelle, das pipeline-Modul die Geschäftslogik einschließlich Dateneingabe und -transformation, das db-Modul die Persistierung, und das enrich-Modul die Kommunikation mit einer externen API. In einem größeren Projekt könnten Transformation und Ingestion getrennt werden, hier war das jedoch nicht nötig.

Die genannte externe API liefert Informationen zu einem Musikwerk anhand der ISRC. Es gibt kostenlose APIs, die diese Informationen bereitstellen, jedoch keine, die dem für diese Aufgabe geforderten Umfang gewachsen ist. Daher gehe ich davon aus, dass eine interne API existiert. Aus diesem Grund habe ich einen Mock verwendet.

## Skalierung der Lösung

Die Lösung basiert auf Polars - einem modernen Python-Framework für die Arbeit mit Dataframes. Es bietet Streaming- und Lazy-Loading-Funktionen, die in diesem Projekt genutzt werden.

Die größte in diesem Projekt gelöste Herausforderung ist die Fähigkeit, dieselben Daten mehrfach verarbeiten zu können. Da das System teilweise erfolgreiche Antworten der Work-Catalog-API akzeptiert, dieselben Daten möglicherweise in unterschiedlichen Dateien enthalten sein können und menschliche Fehler auftreten können, muss das System eine Deduplizierung ermöglichen, ohne die Fähigkeit zu verlieren, Millionen von Zeilen zu verarbeiten. Dies wird durch eine Kombination aus einem Unique-Constraint auf drei Feldern, die einen Playback-Log eindeutig identifizieren (Datum, Sender, ISRC), und einem mehrstufigen Ladeprozess realisiert. Zunächst werden die Daten mittels einer effizienten COPY-Operation in eine eindeutig benannte Staging-Tabelle geladen, anschließend wird diese Tabelle unter Verwerfung von Duplikaten in die Haupttabelle gemergt, und schließlich wird die Staging-Tabelle gelöscht. Alle Operationen laufen innerhalb derselben Transaktion.

Die drei eindeutigen Felder wurden nicht als Primärschlüssel verwendet, da ein separater fortlaufender Primärschlüssel unter manchen Umständen von Vorteil ist.

## Fehlerbehandlung und Tests

Es gibt mehrere klar definierte Fehlerfälle, die die Pipeline explizit erkennt, jeweils auf einer bestimmten Stufe geloggt und konsistent behandelt:

| Bedingung | Log-Level | Ergebnis |
|---|---|---|
| CSV-Datei nicht gefunden | ERROR | Pipeline schlägt fehl |
| CSV strukturell fehlerhaft (leere Datei, unregelmäßige/fehlerhafte Zeilen) | ERROR | Pipeline schlägt fehl |
| Zeile besteht Validierung nicht (leere `isrc`/`station_id`, nicht-positive `duration_seconds`/`listener_count`, leeres `timestamp`) | WARNING | Ungültige Zeile wird übersprungen, Pipeline läuft weiter |
| Anreicherungs-API liefert 404 | ERROR | Pipeline schlägt fehl |
| Anreicherungs-API liefert 500 oder einen Verbindungsfehler | ERROR | Dieser Batch wird übersprungen, Pipeline läuft weiter |
| Antwort der Anreicherungs-API fehlerhaft (keine Liste, oder ein einzelner Datensatz besteht Validierung nicht) | ERROR | Dieser Batch wird übersprungen, Pipeline läuft weiter |
| DB-Schreibvorgang schlägt fehl | ERROR | Pipeline schlägt fehl |

Diese Fehlerzustände sowie die „Sunny-Day“-Szenarien werden durch Unit- und Integrationstests abgedeckt.

---

# Music usage pipeline

This is a coding challenge solution. The task was to create a pipeline that would ingest csv files, transform the data, enrich it using external API and save the data in the database.

# Architecture

This is a command-line application. It uses a version of a layered software architecture, with effort placed on scalability.

![alt text](https://github.com/DoomFungus/Musik-Nutzungsdaten-Pipeline/blob/main/Gema.png?raw=true)

The responsibilities are split as follows: cli module handles interface interaction, pipeline module handles business logic, including data ingestion and transformation, db module handles persistence, and enrich module handles interaction with an external API. In a bigger project, transformation and ingestion may be split off, but here there was no need.

External API mentioned provides information about the music work by ISRC. There exist free-to-use APIs that provide this information, but none do it at the scale required for the task, so I assume there exists an internal API. For this reason, I used a mock.

## Scaling the solution

The solution is built using Polars - a modern Python framework for working with dataframes. It offers streaming and lazy loading capabilities that are used in this project.

The biggest issue tackled in this project is the ability to handle the same data multiple times. Due to the fact that the system accepts partial successes from the work catalog API, as well as possible inclusion of the same data in different files, as well as human error, the system needs to be able to provide deduplication capability without losing the ability to ingest millions of rows of data. This is handled by a combination of a unique constraint on three fields that uniquely identify a playback log (date, station, ISRC) and a multi-step loading process. First, the data is loaded into a uniquely-named staging table using an efficient COPY operation, then that table is merged into the main one, discarding duplicates, then the staging table is destroyed. All operations are a part of the same transaction.

The three unique fields were not used as a primary key because in some circumstances, having a separate continuous primary key is beneficial.

## Error handling and testing

There are several distinct error conditions the pipeline recognizes explicitly, each logged at a specific level and handled consistently:


| Condition | Log level | Result |
|---|---|---|
| CSV file not found | ERROR | Pipeline fails |
| CSV structurally malformed (empty file, ragged/malformed rows) | ERROR | Pipeline fails |
| Row fails validation (blank `isrc`/`station_id`, non-positive `duration_seconds`/`listener_count`, null `timestamp`) | WARNING | Invalid row is skipped, pipeline continues |
| Enrichment API returns 404 | ERROR | Pipeline fails |
| Enrichment API returns 500 or a connection error | ERROR | That record/batch is skipped, pipeline continues |
| Enrichment API response malformed (not a list, or an individual record fails validation) | ERROR | That record/batch is skipped, pipeline continues |
| DB write fails| ERROR | Pipeline fails |

Those error states, as well as "sunny day" scenarios are covered by unit and inteegration tests.