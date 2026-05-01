# Changelog

## [Unreleased]

### Added

- Initial CloudKit implementation.

## [0.1.0] - 2026-05-01

### Added

- SwiftData inheritance support for `PersistentModel` types.
  - Inherited models can be inserted, updated, deleted, and fetched from the data store using class table inheritance.
  - Support for polymorphic type check and type cast, including `is` checks and `as` casts in runtime Swift code and `#Predicate` expressions.
- Support for filtering `PersistentModel` objects in `#Predicate`.
- Support for evaluating properties with `Schema.Attribute.Option.ephemeral` in `#Predicate` when snapshot caching is enabled.
- Configuration options for `SQLPredicateTranslator`.

### Changed

- SQL implementations are more SwiftData-aware to preserve its semantics needed for persistent history tracking and CloudKit synchronization.
- Reduced the package's Swift tools version and dependency version requirements so it can be added as a dependency in Swift Playground.

### Fixed

- Addressed an issue that prevented the library from compiling in release mode.

## [0.0.1] - 2026-03-11

### Added

- Initial release.
