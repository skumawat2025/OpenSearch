# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add fingerprint ingest processor ([#13724](https://github.com/opensearch-project/OpenSearch/pull/13724))
- [Remote Store] Rate limiter for remote store low priority uploads ([#14374](https://github.com/opensearch-project/OpenSearch/pull/14374/))
- Apply the date histogram rewrite optimization to range aggregation ([#13865](https://github.com/opensearch-project/OpenSearch/pull/13865))
- [Writable Warm] Add composite directory implementation and integrate it with FileCache ([12782](https://github.com/opensearch-project/OpenSearch/pull/12782))
- Add batching supported processor base type AbstractBatchingProcessor ([#14554](https://github.com/opensearch-project/OpenSearch/pull/14554))
- Fix race condition while parsing derived fields from search definition ([14445](https://github.com/opensearch-project/OpenSearch/pull/14445))
- Add allowlist setting for ingest-common and search-pipeline-common processors ([#14439](https://github.com/opensearch-project/OpenSearch/issues/14439))
- [Writable Warm] Changes for shard recovery and relocation in case of composite directory ([#14670](https://github.com/opensearch-project/OpenSearch/pull/14670))

### Dependencies
- Bump `org.gradle.test-retry` from 1.5.8 to 1.5.9 ([#13442](https://github.com/opensearch-project/OpenSearch/pull/13442))
- Update to Apache Lucene 9.11.0 ([#14042](https://github.com/opensearch-project/OpenSearch/pull/14042))
- Bump `netty` from 4.1.110.Final to 4.1.111.Final ([#14356](https://github.com/opensearch-project/OpenSearch/pull/14356))
- Bump `org.wiremock:wiremock-standalone` from 3.3.1 to 3.6.0 ([#14361](https://github.com/opensearch-project/OpenSearch/pull/14361))
- Bump `reactor` from 3.5.17 to 3.5.18 ([#14395](https://github.com/opensearch-project/OpenSearch/pull/14395))
- Bump `reactor-netty` from 1.1.19 to 1.1.20 ([#14395](https://github.com/opensearch-project/OpenSearch/pull/14395))
- Bump `commons-net:commons-net` from 3.10.0 to 3.11.1 ([#14396](https://github.com/opensearch-project/OpenSearch/pull/14396))
- Bump `com.nimbusds:nimbus-jose-jwt` from 9.37.3 to 9.40 ([#14398](https://github.com/opensearch-project/OpenSearch/pull/14398))
- Bump `org.apache.commons:commons-configuration2` from 2.10.1 to 2.11.0 ([#14399](https://github.com/opensearch-project/OpenSearch/pull/14399))
- Bump `com.gradle.develocity` from 3.17.4 to 3.17.5 ([#14397](https://github.com/opensearch-project/OpenSearch/pull/14397))
- Bump `opentelemetry` from 1.36.0 to 1.39.0 ([#14457](https://github.com/opensearch-project/OpenSearch/pull/14457))
- Bump `azure-identity` from 1.11.4 to 1.13.0, Bump `msal4j` from 1.14.3 to 1.15.1, Bump `msal4j-persistence-extension` from 1.2.0 to 1.3.0 ([#14506](https://github.com/opensearch-project/OpenSearch/pull/14506))
- Bump `com.azure:azure-storage-common` from 12.21.2 to 12.25.1 ([#14517](https://github.com/opensearch-project/OpenSearch/pull/14517))
- Bump `com.microsoft.azure:msal4j` from 1.15.1 to 1.16.0 ([#14610](https://github.com/opensearch-project/OpenSearch/pull/14610))

### Changed
- [Tiered Caching] Move query recomputation logic outside write lock ([#14187](https://github.com/opensearch-project/OpenSearch/pull/14187))
- unsignedLongRangeQuery now returns MatchNoDocsQuery if the lower bounds are greater than the upper bounds ([#14416](https://github.com/opensearch-project/OpenSearch/pull/14416))
- Updated the `indices.query.bool.max_clause_count` setting from being static to dynamically updateable ([#13568](https://github.com/opensearch-project/OpenSearch/pull/13568))
- Make the class CommunityIdProcessor final ([#14448](https://github.com/opensearch-project/OpenSearch/pull/14448))
- Allow @InternalApi annotation on classes not meant to be constructed outside of the OpenSearch core ([#14575](https://github.com/opensearch-project/OpenSearch/pull/14575))
- Add @InternalApi annotation to japicmp exclusions ([#14597](https://github.com/opensearch-project/OpenSearch/pull/14597))
- Allow system index warning in OpenSearchRestTestCase.refreshAllIndices ([#14635](https://github.com/opensearch-project/OpenSearch/pull/14635))

### Deprecated

### Removed

### Fixed
- Fix bug in SBP cancellation logic ([#13259](https://github.com/opensearch-project/OpenSearch/pull/13474))
- Fix handling of Short and Byte data types in ScriptProcessor ingest pipeline ([#14379](https://github.com/opensearch-project/OpenSearch/issues/14379))
- Switch to iterative version of WKT format parser ([#14086](https://github.com/opensearch-project/OpenSearch/pull/14086))
- Fix the computed max shards of cluster to avoid int overflow ([#14155](https://github.com/opensearch-project/OpenSearch/pull/14155))
- Fixed rest-high-level client searchTemplate & mtermVectors endpoints to have a leading slash ([#14465](https://github.com/opensearch-project/OpenSearch/pull/14465))
- Write shard level metadata blob when snapshotting searchable snapshot indexes ([#13190](https://github.com/opensearch-project/OpenSearch/pull/13190))
- Fix aggs result of NestedAggregator with sub NestedAggregator ([#13324](https://github.com/opensearch-project/OpenSearch/pull/13324))
- Fix fs info reporting negative available size ([#11573](https://github.com/opensearch-project/OpenSearch/pull/11573))
- Add ListPitInfo::getKeepAlive() getter ([#14495](https://github.com/opensearch-project/OpenSearch/pull/14495))
- Fix FuzzyQuery in keyword field will use IndexOrDocValuesQuery when both of index and doc_value are true ([#14378](https://github.com/opensearch-project/OpenSearch/pull/14378))
- Fix file cache initialization ([#14004](https://github.com/opensearch-project/OpenSearch/pull/14004))
- Handle NPE in GetResult if "found" field is missing ([#14552](https://github.com/opensearch-project/OpenSearch/pull/14552))
- Refactoring FilterPath.parse by using an iterative approach ([#14200](https://github.com/opensearch-project/OpenSearch/pull/14200))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.15...2.x
