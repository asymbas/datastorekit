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
- [Roadmap](#roadmap)
- [Limitations](#limitations)
- [FAQ](#faq)
- [Changelog](#changelog)
- [License](#license)

---

## Features

- SwiftData integration.
- Provides `DatabaseStore` as SwiftData's SQLite storage backend and is set up using `DatabaseConfiguration` through `ModelContainer`.
- View all supported predicate expressions in `/Sources/DataStoreRuntime/SQLQuery/PredicateExpressions+SQLPredicateExpression.swift`.
- Extended SwiftData features and conveniences.
- Automatic persistence handling for custom value types that conform to `RawRepresentable` and `OptionSet`.
- Conforming types will be stored as raw values.
- Allows you to use typed cases and constants in `#Predicate` (you are still required to capture their value as expected by the macro).
- Use `#Predicate` to query attributes that are Swift collection types, such as `Dictionary`, `Set`, and `Array`.
- Supports caching references, snapshots, and queries.
- References between entities are managed by the `ReferenceGraph` to reduce fetching overhead from the database.
- Snapshots for the model's backing data are cached by one or more associated `ModelContext` instances.
- Queries cache and rebuild results based on matching hash keys.
- Implicit prefetching for relationships (e.g., preferring to include already cached snapshots in the fetch result).
- Supports SwiftData and custom fetch/save request/result types.
- `PreloadFetchRequest` warms up an upcoming fetch by offloading the work asynchronously.
- `ModelContext.preload(_:for:)` is used to manually fetch and process the result ahead of time on a background actor using the `async`/`await` syntax. Follow up by switching to the desired actor to fetch the prepared results.
- `ModelContext.preloadedFetch(_:)` is an instance method that conveniently wraps the actor switching for you using the `@concurrent` attribute.
- `@Fetch` is a new property wrapper that behaves similarly to SwiftData’s built-in `@Query`, but moves the expensive work onto a background actor. The `@MainActor` then only applies the prepared results, which significantly reduces UI stutters on large databases.
- Represent your models as snapshots or rows (array or dictionary).
- Continue to use your SwiftData `PersistentModel` models as an observable reference with the `ModelContext`.
- Use `DatabaseSnapshot` as a DTO or as a value-type representation of your model. This object conforms to `Codable` and `Sendable`.
- Create a snapshot from a model using `DatabaseSnapshot(_:)`.
- Create a model from a snapshot using `PersistentModel(snapshot:modelContext:)`.
- Use `[any Sendable]` and `[String: any Sendable]` when fetching row data.
- Combine ORM and SQL workflow/patterns.
- Provides access to `DatabaseStore` to manually make requests for fetching and saving.
- Provides shared resource access to `DatabaseQueue`, where you can lease a noncopyable `DatabaseConnection` instance.
- View more features that are in development or planned in the documentation, roadmap, or tasks.

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
dependencies: [
.product(name: "DataStoreKit", package: "datastorekit")
]
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
try store.queue.withConnection { connection in
try connection.fetch(Model.self)
}
```

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

- Editor application: `https://github.com/asymbas/editor`
- Check out the Xcode project or the original Swift Playground used to develop this library.
- Demonstrates SwiftData and DataStoreKit features.
- The application is intended to be a tool for this library when it completes.

## Documentation

- API documentation: `https://www.asymbas.com/documentation/datastorekit`

## Compatibility

- OS 26.1 and OS 26.2 causes an issue with `Schema` where it would unintentionally define Swift collections as transformable attributes when the elements contain simple types. This causes ModelCoders to incorrectly handle the data, resulting in a fatal error.
- A workaround fix has been applied to how snapshots are encoded/decoded.
- Apple responded to the report and mentioned that this should be resolved in OS 26.3.

## Roadmap

Expect significant changes to the API and documentation.

Planned:
- [ ] Comparable feature parity with SwiftData's `DefaultStore`.
- [ ] Migration for SwiftData and SQLite schema.
- [ ] CloudKit support.
- [ ] Inheritance support.

## Limitations

### APIs are not ready for mutating the database

Using `DatabaseQueue` and `DatabaseConnection` to mutate the database rather than saving changes with `ModelContext` can result in the following:
- Inconsistent persistent history tracking.
- Stale references for to-many or many-to-many relationships.
- Unhandled external storage.

In order to save changes manually while ensuring completeness, you can use the same method SwiftData calls when it makes a save request by supplying it with a `DatabaseSaveChanges` type. You must correctly assign which snapshots to insert, update, or delete. This should also include any affected relationships, which may need to be fetched beforehand.

## FAQ

**Q: Can you reuse an existing data store created by `ModelConfiguration`?**<br>
A: No, the schema is incompatible. A runtime error should be thrown if the file is missing the metadata required to identify it as a DataStoreKit store.

**Q: Can you use multiple `DatabaseConfiguration` instances for different stores?**<br>
A: Yes.

**Q: Can you access the underlying SQL database directly?**<br>
A: Yes and refer to the mapping reference in the documentation.

**Q: Is the database format compatible with other SQLite tools?**<br>
A: Yes, it should be able to read fine. The schema doesn't do anything special to the actual store.

**Q: How does caching work and can I control its behavior?**<br>
A: ---

**Q: Does `@Fetch` support the same sort descriptors and predicates as `@Query`?**<br>
A: Yes, they should be basically 'plug-and-play'.

## Changelog
See [CHANGELOG.md](CHANGELOG.md).

## License
This project is licensed under the **Apache 2.0** License. See [LICENSE](LICENSE).

2026-03-01
