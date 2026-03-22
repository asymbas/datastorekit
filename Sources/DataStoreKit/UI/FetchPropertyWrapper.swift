//
//  FetchPropertyWrapper.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreRuntime
import DataStoreSupport
import Logging
import SwiftData
import SwiftUI

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.ui")

internal enum DataStoreFetchIntent: Sendable {
    case ui
    case preload
    case fault
}

internal enum DataStoreFetchIntentContext {
    @TaskLocal internal static var current: DataStoreFetchIntent = .fault
}

extension EnvironmentValues {
    @Entry public var isFetching: Bool = false
}

@MainActor @propertyWrapper
public struct Fetch<Model: PersistentModel>: @preconcurrency DynamicProperty, Sendable {
    @Environment(\.modelContext) private var modelContext
    @State private var observer: DataStoreSaveObserver = .init()
    @State private var models: [Model]
    @State private var error: (any Swift.Error)?
    @State private var task: Task<Void, Never>?
    @State private var lastModelContextIdentifier: ObjectIdentifier?
    @State private var lastTrigger: AnyHashable?
    @State private var lastIssuedRunID: UInt64 = 0
    @State private var pendingRunID: UInt64?
    @State private var pendingEditingState: EditingState?
    private let trigger: AnyHashable?
    private let animation: Animation?
    nonisolated private let makeFetchDescriptor: @Sendable () -> FetchDescriptor<Model>
    
    public init(wrappedValue: [Model]) {
        self.trigger = nil
        self.animation = nil
        self.makeFetchDescriptor = { .init() }
        _models = .init(initialValue: wrappedValue)
        _error = .init(initialValue: nil)
        _lastModelContextIdentifier = .init(initialValue: nil)
        _lastTrigger = .init(initialValue: nil)
    }
    
    public init(
        _ trigger: AnyHashable? = nil,
        animation: Animation? = nil,
        descriptor makeFetchDescriptor:
        @escaping @Sendable () -> FetchDescriptor<Model>
    ) {
        self.trigger = trigger
        self.animation = animation
        self.makeFetchDescriptor = makeFetchDescriptor
        _models = .init(initialValue: [])
        _error = .init(initialValue: nil)
        _lastModelContextIdentifier = .init(initialValue: nil)
        _lastTrigger = .init(initialValue: nil)
    }
    
    public init(
        _ trigger: AnyHashable? = nil,
        predicate: Predicate<Model>? = nil,
        sortBy: [SortDescriptor<Model>] = [],
        fetchLimit: Int? = nil,
        fetchOffset: Int? = nil,
        animation: Animation? = nil
    ) {
        self.init(trigger, animation: animation) {
            var fetchDescriptor = FetchDescriptor<Model>(predicate: predicate, sortBy: sortBy)
            if let fetchLimit { fetchDescriptor.fetchLimit = fetchLimit }
            if let fetchOffset { fetchDescriptor.fetchOffset = fetchOffset }
            return fetchDescriptor
        }
    }
    
    public init(
        trigger: AnyHashable? = nil,
        _ descriptor: FetchDescriptor<Model>? = nil,
        animation: Animation? = nil
    ) {
        self.init(trigger, animation: animation) {
            descriptor ?? .init()
        }
    }
    
    @concurrent private func execute(editingState: EditingState, runID: UInt64) async {
        let descriptor = self.makeFetchDescriptor()
        do {
            try await DataStoreFetchIntentContext.$current.withValue(.preload) {
                try await ModelContext.preload(descriptor, for: editingState)
            }
        } catch {
            guard error is CancellationError == false else {
                return
            }
            await MainActor.run {
                guard runID == self.lastIssuedRunID else { return }
                withAnimation(animation) {
                    $models.wrappedValue = []
                    $error.wrappedValue = error
                }
            }
            return
        }
        await Task.yield()
        await MainActor.run {
            do {
                try Task.checkCancellation()
                let models = try DataStoreFetchIntentContext.$current.withValue(.ui) {
                    try self.modelContext.fetch(descriptor)
                }
                try Task.checkCancellation()
                guard runID == self.lastIssuedRunID else {
                    return
                }
                withAnimation(animation) {
                    $models.wrappedValue = models
                    $error.wrappedValue = nil
                }
            } catch {
                guard error is CancellationError == false else {
                    return
                }
                guard runID == self.lastIssuedRunID else {
                    return
                }
                withAnimation(animation) {
                    $models.wrappedValue = []
                    $error.wrappedValue = error
                }
            }
        }
    }
    
    @MainActor private func schedule(editingState: EditingState, runID: UInt64) {
        #if !SwiftPlayground && swift(>=6.2)
        if #available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *) {
            self.task = Task.immediate(priority: .userInitiated) { @concurrent in
                await run(editingState: editingState, runID: runID)
            }
        } else {
            self.task = Task(priority: .userInitiated) { @concurrent in
                await run(editingState: editingState, runID: runID)
            }
        }
        #else
        self.task = Task(priority: .userInitiated) { @concurrent in
            await run(editingState: editingState, runID: runID)
        }
        #endif
    }
    
    nonisolated(nonsending) private func run(editingState: EditingState, runID: UInt64) async {
        await self.execute(editingState: editingState, runID: runID)
        await MainActor.run {
            self.task = nil
            guard Task.isCancelled == false else {
                self.pendingRunID = nil
                self.pendingEditingState = nil
                return
            }
            guard let pendingRunID = self.pendingRunID else { return }
            let pendingEditingState = self.pendingEditingState ?? editingState
            self.pendingRunID = nil
            self.pendingEditingState = nil
            self.schedule(editingState: pendingEditingState, runID: pendingRunID)
        }
    }
    
    public var wrappedValue: [Model] {
        models
    }
    
    public var projectedValue: FetchProjection {
        let editingState = self.modelContext.editingState
        let refreshAction = {
            self.lastIssuedRunID &+= 1
            let runID = self.lastIssuedRunID
            if self.task != nil {
                self.pendingRunID = runID
                self.pendingEditingState = editingState
            }
            self.schedule(editingState: editingState, runID: runID)
        }
        return .init(error: error, refresh: refreshAction)
    }
    
    /// Inherited from `DynamicProperty.update()`.
    @MainActor public mutating func update() {
        let refresh = self.projectedValue.refresh
        observer.arm(contextID: ObjectIdentifier(modelContext), refresh: refresh)
        let currentModelContextIdentifier = ObjectIdentifier(modelContext)
        let shouldRefetch =
        lastModelContextIdentifier != currentModelContextIdentifier ||
        lastTrigger != trigger ||
        lastModelContextIdentifier == nil
        guard shouldRefetch else { return }
        self.lastModelContextIdentifier = currentModelContextIdentifier
        self.lastTrigger = trigger
        refresh()
    }
    
    public struct FetchProjection {
        nonisolated internal let error: (any Swift.Error)?
        nonisolated private let action: () -> Void
        
        nonisolated internal init(
            error: (any Swift.Error)?,
            refresh action: @escaping () -> Void
        ) {
            self.error = error
            self.action = action
        }
        
        nonisolated internal func refresh() {
            action()
        }
    }
    
    @MainActor private final class DataStoreSaveObserver {
        private var task: Task<Void, Never>?
        private var contextID: ObjectIdentifier?
        private var refresh: (() -> Void)?
        
        internal func arm(contextID: ObjectIdentifier, refresh: @escaping () -> Void) {
            self.refresh = refresh
            if self.contextID != contextID {
                self.contextID = contextID
                task?.cancel()
                #if swift(>=6.2) && !SwiftPlaygrounds
                self.task = Task { @MainActor in
                    for await _ in NotificationCenter.default.notifications(named: .dataStoreDidSave) {
                        self.refresh?()
                    }
                }
                #endif
            }
        }
        
        deinit {
            task?.cancel()
        }
    }
}
