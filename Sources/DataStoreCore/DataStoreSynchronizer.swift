//
//  DataStoreSynchronizer.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

internal import DataStoreSupport
public import Foundation
public import SwiftData

public protocol DataStoreSynchronizer<Store>: Sendable {
    associatedtype Store: DataStore & HistoryProviding
    associatedtype SyncConfiguration: DataStoreSynchronizerConfiguration<Store>
    var id: String { get }
    nonisolated var remoteAuthor: String { get }
    nonisolated var lastProcessedToken: Store.HistoryType.TokenType? { get }
    func prepare() async throws
    func sync(transactions: [Store.HistoryType]) async throws
}

nonisolated public protocol DataStoreSynchronizerConfiguration<Store>: Sendable {
    associatedtype Store: DataStore & HistoryProviding where Synchronizer.Store == Store
    associatedtype Synchronizer: DataStoreSynchronizer<Store>
    var id: String { get }
    var remoteAuthor: String { get }
    func makeSynchronizer(store: Store) -> any DataStoreSynchronizer<Store>
}

nonisolated package struct SynchronizationState: Sendable {
    package var task: Task<Void, Never>?
    package var isPending: Bool
    
    package init(task: Task<Void, Never>? = nil, isPending: Bool = false) {
        self.task = task
        self.isPending = isPending
    }
}

nonisolated public struct SynchronizationStatus: Sendable {
    public let id: String
    public var phase: SynchronizationPhase
    public var isPending: Bool
    public var lastSyncDate: Date?
    public var lastError: (any Swift.Error)?
    public var lastRemoteApplyErrorMessage: String?
    public var resolvedIdentityConflicts: Int
    public var pendingUnresolvedCount: Int
    
    package init(
        id: String,
        phase: SynchronizationPhase = .idle,
        isPending: Bool = false,
        lastSyncDate: Date? = nil,
        lastError: (any Swift.Error)? = nil,
        lastRemoteApplyErrorMessage: String? = nil,
        resolvedIdentityConflicts: Int = 0,
        pendingUnresolvedCount: Int = 0
    ) {
        self.id = id
        self.phase = phase
        self.isPending = isPending
        self.lastSyncDate = lastSyncDate
        self.lastError = lastError
        self.lastRemoteApplyErrorMessage = lastRemoteApplyErrorMessage
        self.resolvedIdentityConflicts = resolvedIdentityConflicts
        self.pendingUnresolvedCount = pendingUnresolvedCount
    }
}

nonisolated public enum SynchronizationPhase: String, Sendable {
    case idle
    case scheduled
    case preparing
    case sending
    case fetching
    case finished
    case failed
}
