# SwiftData implementation notes

@Comment {
    SwiftData reference material.
}

## DataStore

```
DataStore.fetch(_:)
```

- `ModelContext` resolves models by sending a `DataStoreFetchRequest<T>` where the `FetchDescriptor<T>` predicate uses a `Set<PersistentIdentifier>`. This occurs when `ModelContext.model(for:)` is called and when faulting relationships.

## ModelContext

- `ModelContext.fetchCount(_:)` does not call `DataStore.fetchCount(_:)` when it has unsaved changes. It uses `DataStore.fetchIdentifiers(_:)` instead.
- Delete rules are enforced at runtime by `ModelContext`.

---

@Small {
    Last updated: 2026-03-23
}
