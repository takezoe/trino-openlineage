trino-openlineage
====
An experimental OpenLineage integration for Trino

## Requirements

- Java 11
- Maven
- Trino 362

## Installation

Build and copy trino-openlineage plugin:

```sh
mvn clean install -DskipTests
unzip ./target/trino-openlineage-362.zip -d $TRINO_HOME/plugin
```

Add the following line to `$TRINO_HOME/etc/event-listener.properties`:

```properties
event-listener.name=openlineage
```
