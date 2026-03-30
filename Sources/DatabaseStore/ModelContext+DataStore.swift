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
import Foundation
import SwiftData
import Synchronization

extension ModelContext {
    package var store: DatabaseStore {
        guard let store = try? DataStoreAggregate.load(editingState: editingState),
              let store = store as? DatabaseStore else {
            preconditionFailure("Expected a DatabaseStore")
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
        for editingState: EditingState,
        modifier: String? = nil
    ) async throws -> String? where T: PersistentModel {
        guard let store = try? DataStoreAggregate.load(editingState: editingState),
              let store = store as? DatabaseStore else {
            preconditionFailure("Expected a DatabaseStore")
        }
        try Task.checkCancellation()
        return try await store.preload(PreloadFetchRequest(
            isUnchecked: false,
            modifier: modifier,
            descriptor: descriptor,
            editingState: .init(id: editingState.id, author: editingState.author)
        ))?.modifier
    }
    
    public func preloadedFetch<T>(
        _ descriptor: FetchDescriptor<T>,
        isolation: isolated (any Actor)? = #isolation
    ) async throws -> [T] where T: PersistentModel {
        let editingState = self.editingState
        defer {
            if self.editingState.author != editingState.author {
                self.editingState.author = editingState.author
            }
        }
        let modifier = try await Task { @concurrent in
            try await ModelContext.preload(descriptor, for: editingState, modifier: UUID().uuidString)
        }.value
        self.editingState.author = modifier
        let result = try fetch(descriptor)
        return result
    }
}

extension ModelContext {
    public func preloadedSectionedFetch<T, SectionID>(
        _ sectionedDescriptor: SectionedFetchDescriptor<T, SectionID>,
        isolation: isolated (any Actor)? = #isolation
    ) async throws -> SectionedFetchResults<SectionID, T>
    where T: PersistentModel, SectionID: Hashable & Sendable {
        let editingState = self.editingState
        _ = try await Task { @concurrent in
            try await ModelContext.preload(sectionedDescriptor.descriptor, for: editingState)
        }.value
        return try fetchSectioned(sectionedDescriptor)
    }
}

extension ModelContext {
    public func fetchSectioned<T, SectionID>(_ descriptor: SectionedFetchDescriptor<T, SectionID>)
    throws -> SectionedFetchResults<SectionID, T> where T: PersistentModel, SectionID: Hashable & Sendable {
        guard !descriptor.includesEmptySections else {
            throw DataStoreError.unsupportedFeature
        }
        let fetchedElements = try fetch(descriptor.descriptor)
        let count = fetchedElements.count
        var sections = [SectionedFetchResults<SectionID, T>.Section]()
        sections.reserveCapacity(count)
        var sectionIndexes = [SectionID: Int]()
        sectionIndexes.reserveCapacity(count)
        let fetchLimitPerSection = descriptor.limitPerSection
        for element in fetchedElements {
            let sectionID = element[keyPath: descriptor.sectionKeyPath]
            if let index = sectionIndexes[sectionID] {
                sections[index].count += 1
                if let fetchLimitPerSection,
                   sections[index].elements.count >= fetchLimitPerSection {
                    continue
                }
                sections[index].elements.append(element)
            } else {
                let elements: [T]
                if let fetchLimitPerSection, fetchLimitPerSection <= 0 {
                    elements = []
                } else {
                    elements = [element]
                }
                sectionIndexes[sectionID] = sections.endIndex
                sections.append(.init(id: sectionID, count: 1, elements: elements))
            }
        }
        if descriptor.sectionSortBy == .reverse { sections.reverse() }
        return .init(sections: sections)
    }
}
