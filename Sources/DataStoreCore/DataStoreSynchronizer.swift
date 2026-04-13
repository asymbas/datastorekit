//
//  DataStoreSynchronizer.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import Foundation
import SwiftData

public protocol DataStoreSynchronizer<Store>: Sendable {
    associatedtype Store: DataStore & HistoryProviding
    associatedtype SyncConfiguration: DataStoreSynchronizerConfiguration<Store>
    nonisolated var id: String { get }
    nonisolated var remoteAuthor: String { get }
    nonisolated var lastProcessedToken: Store.HistoryType.TokenType? { get }
    func prepare() async throws
    func sync(transactions: [Store.HistoryType]) async throws
}

public protocol DataStoreSynchronizerConfiguration<Store>: Sendable {
    associatedtype Store: DataStore & HistoryProviding where Synchronizer.Store == Store
    associatedtype Synchronizer: DataStoreSynchronizer<Store>
    var id: String { get }
    var remoteAuthor: String { get }
    func makeSynchronizer(store: Store) -> any DataStoreSynchronizer<Store>
}

package struct SynchronizationState: Sendable {
    nonisolated package var task: Task<Void, Never>?
    nonisolated package var isPending: Bool
    
    nonisolated package init(task: Task<Void, Never>? = nil, isPending: Bool = false) {
        self.task = task
        self.isPending = isPending
    }
}

public struct SynchronizationStatus: Sendable {
    nonisolated public let id: String
    nonisolated public var phase: SynchronizationPhase
    nonisolated public var isPending: Bool
    nonisolated package var lastSyncDate: Date?
    nonisolated package var lastError: (any Swift.Error)?
    
    nonisolated package init(
        id: String,
        phase: SynchronizationPhase = .idle,
        isPending: Bool = false,
        lastSyncDate: Date? = nil,
        lastError: (any Swift.Error)? = nil
    ) {
        self.id = id
        self.phase = phase
        self.isPending = isPending
        self.lastSyncDate = lastSyncDate
        self.lastError = lastError
    }
}

public enum SynchronizationPhase: String, Sendable {
    case idle
    case scheduled
    case preparing
    case sending
    case fetching
    case finished
    case failed
}
