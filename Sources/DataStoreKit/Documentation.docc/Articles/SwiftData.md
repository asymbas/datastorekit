# SwiftData implementation notes

@Comment {
    SwiftData reference material.
}

## DataStore

```
DataStore.fetch(_:)
```

- `ModelContext` resolves models by providing a `DataStoreFetchRequest<T>` where the `FetchDescriptor<T>` predicate uses a `Set<PersistentIdentifier>`.

---

@Small {
    Last updated: 2026-03-23
}
