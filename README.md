# DataStoreKit

A SwiftData custom data store implementation that supports SQLite as its primary persistence layer.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Preview](#preview)
- [Documentation](#documentation)
- [Compatibility](#compatibility)
- [Limitations](#limitations)
- [Known Issues](#known-issues)
- [Roadmap](#roadmap)
- [FAQ](#faq)
- [Changelog](#changelog)
- [License](#license)

---

## Features

- SwiftData integration.
- Provides `DatabaseStore` as SwiftData's SQLite storage backend, configured through `DatabaseConfiguration` in `ModelContainer`.
- View all supported predicate expressions in `/Sources/DataStoreRuntime/SQLQuery/PredicateExpressions+SQLPredicateExpression.swift`.
- Extended SwiftData features and conveniences:
  - Use `#Predicate` to query attributes that are Swift collection types, such as `Dictionary`, `Set`, and `Array`.
  - Automatic persistence handling for custom value types that conform to `RawRepresentable` and `OptionSet`.
    - Conforming types will be stored as raw values.
    - Allows you to use typed cases and constants in `#Predicate` (you are still required to capture their value as expected by the macro).
- Caches references, snapshots, and queries:
  - References between entities are managed by the `ReferenceGraph` to reduce fetching overhead from the database.
  - Snapshots for the model's backing data are cached by one or more associated `ModelContext` instances.
  - Queries cache and rebuild results based on matching hash keys.
  - Implicit prefetching for relationships (e.g., preferring to include already cached snapshots in the fetch result).
- Allows SwiftData and custom fetch/save request/result types:
  - `PreloadFetchRequest` warms up an upcoming fetch by offloading the work asynchronously.
    - `ModelContext.preload(_:for:)` is used to manually fetch and process the result ahead of time on a background actor using the `async`/`await` syntax. Follow up by switching to the desired actor to fetch the prepared results.
    - `ModelContext.preloadedFetch(_:isolation:)` is an instance method that conveniently wraps the actor switching for you using the `@concurrent` attribute.
    - `@Fetch` is a new property wrapper that builds on preloaded fetching and behaves similarly to SwiftData’s built-in `@Query`, but moves the expensive work onto a background actor. The `@MainActor` then only applies the prepared results, which significantly reduces UI stutters on large databases.
- Persistent history tracking:
  - History is stored inline for the current year.
  - Supports archiving older history into external databases.
  - Archived history can be attached separately when fetching history.
- Combine ORM and SQL workflow/patterns.
  - Represent your models as snapshots or rows (array or dictionary).
    - Continue using your SwiftData `PersistentModel` types as observable reference models with `ModelContext`.
    - Use `DatabaseSnapshot` as a DTO or as a value-type representation of your model. This object conforms to `Codable` and `Sendable`.
  - Create a snapshot from a model using `DatabaseSnapshot(_:)`.
  - Create a model from a snapshot using `PersistentModel(snapshot:modelContext:)`.
- Use `[any Sendable]` and `[String: any Sendable]` when fetching row data.
- Provides access to `DatabaseStore` to manually make requests for fetching and saving.
- Provides shared resource access to `DatabaseQueue`, where you can lease a noncopyable `DatabaseConnection` instance.

## Installation

### Swift Package Manager

Add to a Swift package in `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/asymbas/datastorekit.git", from: "0.0.1")
],
targets: [
    .target(
        name: "Target",
        dependencies: [.product(name: "DataStoreKit", package: "datastorekit")]
    )
]
```

## Quick Start

The SwiftData experience is preserved, and in many cases adopting DataStoreKit can be as simple as replacing `ModelConfiguration` with `DatabaseConfiguration` in an existing schema setup.

```swift
import DataStoreKit

let schema = Schema(versionedSchema: DatabaseSchema.self)
let configuration = DatabaseConfiguration(name: "custom", schema: schema)
let modelContainer = try ModelContainer(for: schema, configurations: configuration)
```

## Usage

Request a noncopyable `DatabaseConnection` from the `DatabaseQueue`.

```swift
let rows = try store.queue.withConnection { connection in
    try connection.fetch("SELECT * FROM Entity")
}
```

Specify a connection type explicitly, or use the convenience methods.

By default, `withConnection(_:for:_:)` uses `nil` for the connection type, which prefers a reader and may fall back to a writer if no reader is available. Be careful with this behavior in code paths that may already depend on writer access, since an implicit writer fallback can introduce deadlock risk.

```swift
try store.queue.withConnection(nil) { connection in ... }
try store.queue.withConnection(.reader) { connection in ... }
try store.queue.withConnection(.writer) { connection in ... }

try store.queue.reader { connection in ... }
try store.queue.writer { connection in ... }
```

You can also work with conventional SQL-style rows by fetching them as Swift collections, such as `[any Sendable]` or `[String: any Sendable]`.

See the documentation for details on mapping row data back to SwiftData models using the schema.

## Preview

Explore the [Editor](https://github.com/asymbas/editor) repository. It is a companion Xcode project used to develop and demonstrate DataStoreKit.

It showcases SwiftData and DataStoreKit features and is intended to become a dedicated tool for the library as development continues.

## Documentation

The documentation is currently being revised and is hosted separately from this repository.

Read the latest version here: [DataStoreKit Documentation](https://www.asymbas.com/datastorekit/documentation/datastorekit/)

For questions, feedback, or suggestions, please use [GitHub Discussions](https://github.com/asymbas/datastorekit/discussions).

## Compatibility

- OS 26.1 and OS 26.2 have an issue with `Schema`, where Swift collections can be unintentionally defined as transformable attributes when their elements contain simple types. This causes ModelCoders to incorrectly handle the data, resulting in a fatal error.
  - A workaround fix has been applied to how snapshots are encoded/decoded.
  - Apple responded to the report and mentioned that this should be resolved in OS 26.3.

## Limitations

### APIs are not ready for mutating the database

Using `DatabaseQueue` and `DatabaseConnection` to mutate the database rather than saving changes with `ModelContext` can result in the following:
- Inconsistent persistent history tracking.
- Stale references for to-many or many-to-many relationships.
- Unhandled external storage.

In order to save changes manually while ensuring completeness, you can use the same method SwiftData calls when it makes a save request by supplying it with a `DatabaseSaveChanges` type. You must correctly assign which snapshots to insert, update, or delete. This should also include any affected relationships, which may need to be fetched beforehand.

## Known Issues

- **Required one-to-one relationships can form dependency cycles**<br>
  Cyclic non-optional to-one relationships are currently not supported during a single insert pass. Newly inserted models that reference each other through a bidirectional non-optional one-to-one relationship can fail to save in the same operation, because DataStoreKit resolves required to-one dependencies before inserting a snapshot. A required to-one dependency blocks insertion when its related identifier is still uncommitted. When both sides require the other side to already exist, no valid insertion order can be established. As a result, both inserts may be repeatedly deferred until the save operation reaches its maximum retry count.<br>
  - **Workaround:** Make one side optional during insertion.
- **Tombstones cannot be instantiated**<br>
  SwiftData does not provide a way to create `HistoryTombstone` for preserved values.<br>
  - **Workaround:** Use the subscript on the `DatabaseHistoryDelete` instance rather than its `tombstone` property.
- **Generic or protocol-constrained key paths can't be matched to schema metadata**<br>
  When a model is accessed through a protocol or generic constraint rather than its concrete type, the key path identity changes enough that the key path dictionary lookup misses. The parse-based fallback helps in some cases, but isn't reliable across all shapes.
- **Key paths that traverse an optional value cannot be resolved**<br>
  `AnyKeyPath.appending(path:)` returns `nil` when the left-hand side produces an optional value type and the right-hand side expects the unwrapped type. It is currently unknown how to dynamically append through an optional boundary. Any predicate or sort descriptor that chains through an optional intermediate cannot be reconstructed into a key path for SQL generation.
- **`SortDescriptor` on a relationship's attribute requires a predicate referencing that relationship**<br>
  Sort descriptors that traverse a relationship path, such as `\Model.relationship.name`, require a predicate that also references the relationship. DataStoreKit derives relationship traversal information for SQL generation from `#Predicate`. Without a predicate touching that relationship, no `JOIN` is generated, and the sort clause references a table that isn't in the `FROM` clause. The sort is silently omitted.

## Roadmap

Expect significant changes to the API and documentation.

Planned:
- [ ] Comparable feature parity with SwiftData's `DefaultStore`.
- [ ] Migration for SwiftData and SQLite schema.
- [ ] CloudKit support.
- [ ] Inheritance support.

## FAQ

**Q: Can you reuse an existing data store created by `ModelConfiguration`?**<br>
A: No, the schema is incompatible. A runtime error should be thrown if the file is missing the metadata required to identify it as a DataStoreKit store.

**Q: Can you use multiple `DatabaseConfiguration` instances for different stores?**<br>
A: Yes.

**Q: Can you access the underlying SQL database directly?**<br>
A: Yes. Refer to the mapping reference in the documentation.

**Q: Is the database format compatible with other SQLite tools?**<br>
A: Yes. It should be readable by other SQLite tools. The schema doesn't do anything special to the actual store.

**Q: Does `@Fetch` support the same descriptors and predicates as `@Query`?**<br>
A: Yes, they should be "plug-and-play".

**Q: When should `DatabaseQueue` and `DatabaseConnection` be used instead of `ModelContext`?**<br>
A: You use it if you want to customize the SQL engine's behavior or perform maintenance. You can simply use it to get specific values without having to rebuild a model snapshot. You can have more dynamism when you need it.

**Q: Is inheritance supported and how is it implemented?**<br>
A: No. It's not fully implemented yet. But the current implementation uses Class Table Inheritance.

## Changelog
See [CHANGELOG.md](CHANGELOG.md).

## License
This project is licensed under the **Apache 2.0** License. See [LICENSE](LICENSE).

2026-03-12
