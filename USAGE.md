# Discovering Scala 3 ZIO Ecosystem with MLB

## Caliban: Creating a GraphQL API

### Resources

See documentation links:
* [ZIO Community Libraries: Caliban](https://zio.dev/ecosystem/community/caliban/)
* [ZIO Http](https://zio.dev/zio-http/)
* [Caliban: A simple example](https://ghostdogpr.github.io/caliban/docs/#a-simple-example)
* [Caliban Deriving](https://zio.dev/caliban-deriving/)

### Usage

Compile, then run project with auto reload thanks to sbt-revolver:
``` bash
$ sbt
[...]
sbt:scala3-mlb> project graphql
sbt:graphql> compile
sbt:graphql> ~reStart
```
Test a query with:

```bash
$ curl -v 'http://localhost:8088/api/graphql' --data-binary '{"query":"{playoffs(playoffRound: 1){date}}"}'
$ curl -v 'http://localhost:8088/api/graphql' --data-binary '{"query":"{season(year: 2023){date}}"}'
```

## ZIO Http: Building HTTP Applications

### Resources

See documentation links:
* [ZIO Http](https://zio.dev/zio-http/)

### Usage

Compile, then run project with auto reload thanks to sbt-revolver:
``` bash
$ sbt
[...]
sbt:scala3-mlb> project rest
sbt:graphql> compile
sbt:graphql> ~reStart
```
Test a query with:

```bash
$ curl -s -D - -o /dev/null "http://localhost:8080/text"
```

## ZIO JDBC: Idiomatic ZIO 2.0 Interface to JDBC

### Resources

See documentation links:
* [ZIO JDBC](https://zio.dev/zio-jdbc/)
* [ZIO Worksheet](https://github.com/dacr/zio-worksheet)

### Usage

Compile, then run project with auto reload thanks to sbt-revolver:
``` bash
$ sbt
[...]
sbt:scala3-mlb-db> project db
sbt:scala3-mlb-db> compile
sbt:scala3-mlb-db> run
Game(2023-04-01,2023,Some(1))
Game(2023-04-02,2023,None)
```