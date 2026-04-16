//
//  DatabaseActor.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import Dispatch
public import SwiftData

public final class DefaultDatabaseSerialModelExecutor: SerialModelExecutor {
    nonisolated(unsafe) public final let modelContext: ModelContext
    
    public init(modelContext: ModelContext) {
        self.modelContext = modelContext
    }
    
    @inlinable nonisolated public init(modelContainer: ModelContainer) {
        self.modelContext = ModelContext(modelContainer)
        Task(executorPreference: DatabaseSerialModelExecutor.shared) {
            await withTaskExecutorPreference(DatabaseSerialModelExecutor.shared) {
                DatabaseActor.preconditionIsolated()
            }
        }
    }
    
    /// Inherited from `SerialExecutor.enqueue(_:)`.
    @inlinable nonisolated public final func enqueue(_ job: consuming ExecutorJob) {
        DatabaseSerialModelExecutor.shared.enqueue(job)
    }
}

public final class DatabaseSerialModelExecutor: SerialExecutor, TaskExecutor {
    @usableFromInline nonisolated internal
    static let shared: DatabaseSerialModelExecutor = .init()
    @usableFromInline nonisolated internal
    final let queue: DispatchSerialQueue = .init(label: "com.asymbas.datastorekit.actor")
    
    nonisolated public init() {}
    
    /// Inherited from `SerialExecutor.enqueue(_:)`.
    @inlinable nonisolated public final func enqueue(_ job: consuming ExecutorJob) {
        let unownedJob = UnownedJob(job)
        let serialExecutor = asUnownedSerialExecutor()
        let taskExecutor = asUnownedTaskExecutor()
        queue.async {
            unownedJob.runSynchronously(
                isolatedTo: serialExecutor,
                taskExecutor: taskExecutor
            )
        }
    }
    
    /// Inherited from `TaskExecutor.asUnownedTaskExecutor()`.
    @inlinable nonisolated public final func asUnownedTaskExecutor() -> UnownedTaskExecutor {
        UnownedTaskExecutor(ordinary: self)
    }
    
    /// Inherited from `SerialExecutor.asUnownedSerialExecutor()`.
    @inlinable nonisolated public final func asUnownedSerialExecutor() -> UnownedSerialExecutor {
        UnownedSerialExecutor(ordinary: self)
    }
}

@globalActor public final actor DatabaseActor: Actor, GlobalActor {
    /// Inherited from `GlobalActor.shared`.
    nonisolated public static let shared: DatabaseActor = .init()
    /// Inherited from `Actor.unownedExecutor`.
    @inlinable nonisolated public final var unownedExecutor: UnownedSerialExecutor {
        DatabaseSerialModelExecutor.shared.asUnownedSerialExecutor()
    }
    
    nonisolated public static func run<T>(
        resultType: T.Type = T.self,
        body: @DatabaseActor () throws -> T
    ) async rethrows -> T where T: Sendable {
        try await body()
    }
    
    nonisolated public static func run<T>(
        resultType: T.Type = T.self,
        body: @DatabaseActor () async throws -> T
    ) async rethrows -> T where T: Sendable {
        try await body()
    }
}
