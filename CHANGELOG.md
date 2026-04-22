# Changelog

## [Unreleased]

### Added

- Initial CloudKit implementation.
- Properties with `Schema.Attribute.Option.ephemeral` can be evaluated in `#Predicate` when caching is enabled.
- Filter `PersistentModel` objects in `#Predicate`.
- Added configurations for `SQLPredicateTranslator`.

### Changed

- Improved inheritance support for `PersistentModel` types.
  - Added support for update and delete operations.
  - Updated predicate expressions for `#Predicate` macro.
    - `ConditionalCast` uses the `as` keyword to conditionally cast a value to a specific model type.
    - `TypeCheck` uses the `is` keyword to check whether a value is a specific model type.
- Reduced the package's Swift tools version and dependency version requirements so it can be added as a dependency in Swift Playground.

## [0.0.1] - 2026-03-11

### Added

- Initial release.
