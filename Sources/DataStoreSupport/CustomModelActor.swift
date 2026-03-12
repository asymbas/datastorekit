//
//  CustomModelActor.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Dispatch
import SwiftData

public final class CustomModelSerialExecutor: SerialModelExecutor {
    nonisolated(unsafe) public final let modelContext: ModelContext
    nonisolated internal final let executor: CustomSerialExecutor
    
    nonisolated public init(modelContainer: ModelContainer) {
        self.modelContext = ModelContext(modelContainer)
        self.executor = .init()
    }
    
    /// Inherited from `SerialExecutor.enqueue(_:)`.
    nonisolated public final func enqueue(_ job: consuming ExecutorJob) {
        executor.enqueue(job)
    }
    
    /// Inherited from `SerialExecutor.asUnownedSerialExecutor()`.
    nonisolated public final func asUnownedSerialExecutor() -> UnownedSerialExecutor {
        executor.asUnownedSerialExecutor()
    }
}

public final class CustomSerialExecutor: SerialExecutor, TaskExecutor {
    nonisolated private
    final let queue: DispatchSerialQueue = .init(label: "com.asymbas.datastorekit.modelactor")
    
    nonisolated public init() {}
    
    /// Inherited from `SerialExecutor.enqueue(_:)`.
    nonisolated public final func enqueue(_ job: consuming ExecutorJob) {
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

public actor CustomModelActor {
    nonisolated public final let modelContainer: ModelContainer
    nonisolated public final let executor: CustomModelSerialExecutor
    
    @inlinable nonisolated(unsafe) public final var modelContext: ModelContext {
        executor.modelContext
    }
    
    @inlinable public init(modelContainer: ModelContainer) {
        self.modelContainer = modelContainer
        self.executor = .init(modelContainer: modelContainer)
    }
    
    /// Inherited from `Actor.unownedExecutor`.
    @inlinable nonisolated public final var unownedExecutor: UnownedSerialExecutor {
        executor.asUnownedSerialExecutor()
    }
}
