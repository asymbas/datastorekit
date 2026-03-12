//
//  ModelContext+DataStore.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreSupport
import SwiftData
import Synchronization

extension ModelContext {
    package var store: DatabaseStore {
        guard let store = DataStoreContainer.load(editingState: editingState),
              let store = store as? DatabaseStore else {
            fatalError("Expected a DatabaseStore")
        }
        return store
    }
}

extension ModelContext {
    /// Sends a preload fetch request for the following `ModelContext` you intend to fetch from.
    ///
    /// Call this method in a `Task` on any actor and await for the preloading warm-up to complete.
    /// Follow up by switching over to the actor the `ModelContext` is isolated to.
    ///
    /// ```swift
    /// let descriptor = FetchDescriptor<T>()
    /// let editingState = self.modelContext.editingState
    /// Task { @concurrent in
    ///     try await ModelContext.preload(descriptor, for: editingState)
    ///     try await MainActor.run {
    ///         let results = try modelContext.fetch(descriptor)
    ///         self.models = results
    ///     }
    /// }
    /// ```
    ///
    /// - Note:
    ///   - Ensure the `FetchDescriptor` is not modified in between preloading and fetching.
    ///   - Preloading is reserved once per `ModelContext` and the descriptors must be identical.
    /// - Parameters:
    ///   - descriptor: The same `FetchDescriptor` instance used for the following fetch.
    ///   - editingState: The `EditingState` associated to the `ModelContext` you plan to use.
    nonisolated public static func preload<T>(
        _ descriptor: FetchDescriptor<T>,
        for editingState: EditingState
    ) async throws where T: PersistentModel {
        guard let store = await DataStoreContainer.load(editingState: editingState),
              let store = store as? DatabaseStore else {
            fatalError("Expected a DatabaseStore")
        }
        try Task.checkCancellation()
        try await store.preload(PreloadFetchRequest(
            isUnchecked: false,
            modifier: nil,
            descriptor: descriptor,
            editingState: .init(
                id: editingState.id,
                author: editingState.author
            )
        ))
    }
    
    public func preloadedFetch<T>(
        _ descriptor: FetchDescriptor<T>,
        isolation: isolated (any Actor)? = #isolation
    ) async throws -> [T] where T: PersistentModel {
        let editingState = self.editingState
        try await Task { @concurrent in
            try await ModelContext.preload(descriptor, for: editingState)
        }.value
        return try fetch(descriptor)
    }
}



#if false
public struct SectionedFetchResults<SectionID, Element>: Sendable
where SectionID: Hashable & Sendable, Element: PersistentModel {
    public var sections: [Section]
    
    public struct Section: Sendable {
        public var id: SectionID
        public var count: Int
        public var elements: [Element]
    }
}
public struct SectionedFetchDescriptor<T, SectionID>: Sendable
where T: PersistentModel, SectionID: Hashable & Sendable {
    public var fetchDescriptor: FetchDescriptor<T>
    public var sectionIdentifier: KeyPath<T, SectionID>
    public var sectionSort: SortOrder
    public var fetchLimitPerSection: Int?
    public var includesEmptySections: Bool
    
    public init(
        _ fetchDescriptor: FetchDescriptor<T> = .init(),
        sectionIdentifier: KeyPath<T, SectionID>,
        sectionSort: SortOrder = .forward,
        fetchLimitPerSection: Int? = nil,
        includesEmptySections: Bool = false
    ) {
        self.fetchDescriptor = fetchDescriptor
        self.sectionIdentifier = sectionIdentifier
        self.sectionSort = sectionSort
        self.fetchLimitPerSection = fetchLimitPerSection
        self.includesEmptySections = includesEmptySections
    }
}

extension ModelContext {
    nonisolated public static func preloadSectioned<T, SectionID>(
        _ descriptor: SectionedFetchDescriptor<T, SectionID>,
        for editingState: EditingState
    ) async throws
    where T: PersistentModel, SectionID: Hashable & Sendable {
        guard let store = await DataStoreContainer.load(editingState: editingState),
              let store = store as? DatabaseStore else {
            fatalError("Expected a DatabaseStore")
        }
        try Task.checkCancellation()
        try await store.preloadSectioned(
            PreloadSectionedFetchRequest(
                descriptor: descriptor,
                editingState: .init(id: editingState.id, author: editingState.author)
            )
        )
    }
}
extension ModelContext {
    public func sectionedFetch<T, SectionID>(
        _ descriptor: SectionedFetchDescriptor<T, SectionID>,
        isolate: isolated Actor = #isolation
    ) async throws -> SectionedFetchResults<SectionID, T>
    where T: PersistentModel, SectionID: Hashable & Sendable {
        let editingState = self.editingState
        try await Task { @concurrent in
            try await ModelContext.preloadSectioned(descriptor, for: editingState)
        }.value
        return try fetchSectioned(descriptor)
    }
}

extension ModelContext {
    public func fetchSectioned<T, SectionID>(
        _ descriptor: SectionedFetchDescriptor<T, SectionID>
    ) throws -> SectionedFetchResults<SectionID, T>
    where T: PersistentModel, SectionID: Hashable & Sendable {
        fatalError("Backed by your custom store implementation")
    }
}
#endif
