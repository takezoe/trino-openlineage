trino-openlineage
====
An experimental [OpenLineage](https://github.com/OpenLineage/OpenLineage) integration for [Trino](https://github.com/trinodb/trino)

![Marquez](marquez.png)

## Requirements

- Java 21
- Maven
- Trino 422
- [Marquez](https://github.com/MarquezProject/marquez)

## Installation

Build and copy trino-openlineage plugin:

```sh
mvn clean install -DskipTests
unzip ./target/trino-openlineage-422.zip -d $TRINO_HOME/plugin
```

Add the following line to `$TRINO_HOME/etc/event-listener.properties`:

```properties
event-listener.name=openlineage
openlineage.url=http://localhost:5000
```
