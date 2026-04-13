//
//  HistoryState.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreRuntime
import DataStoreSupport
import Foundation
import Logging
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.transaction")

public extension Notification.Name {
    static let dataStoreSynchronizationStatusDidChange: Self = .init("dataStoreSynchronizationStatusDidChange")
}

extension DataStoreSynchronizerConfiguration {
    internal func makeDatabaseSynchronizer(store: Any) -> any DataStoreSynchronizer<DatabaseStore> {
        guard let store = store as? Self.Store else {
            preconditionFailure("Synchronizer store type mismatch: expected \(Self.Store.self)")
        }
        guard let synchronizer = makeSynchronizer(store: store) as? any DataStoreSynchronizer<DatabaseStore> else {
            preconditionFailure("Synchronizer must use DatabaseStore: \(Self.Synchronizer.self)")
        }
        return synchronizer
    }
}

@DatabaseActor internal final class HistoryState: Sendable {
    nonisolated internal weak let store: DatabaseStore?
    internal let synchronizers: [any DataStoreSynchronizer<DatabaseStore>]
    internal var synchronizationState: SynchronizationState
    internal var synchronizationStatusesByID: [String: SynchronizationStatus]
    internal let historyTTL: DateComponents
    internal let calendar: Calendar
    internal let requireArchivedBeforeDelete: Bool
    internal let deleteArchivedExpiredTransactions: Bool
    internal let archiveBatchSize: Int
    internal let archiveMaxBatches: Int
    internal var historyPurgeState: HistoryPurgeState
    internal var lastHistoryPurgeDate: Date
    internal var nextHistoryPurgeDate: Date
    internal var historyArchiveState: HistoryArchiveState
    internal var lastHistoryArchiveDate: Date
    internal var nextHistoryArchiveDate: Date
    
    internal var cloudKitSyncState: SynchronizationState {
        get { synchronizationState }
        set { synchronizationState = newValue }
    }
    
    internal enum HistoryPurgeState: UInt8 {
        case idle = 0
        case pending
        case purging
    }
    
    internal enum HistoryArchiveState: UInt8 {
        case idle = 0
        case pending
        case archiving
    }
    
    nonisolated internal init(
        store: DatabaseStore,
        synchronizerConfigurations: [any DataStoreSynchronizerConfiguration],
        historyTTL: DateComponents = HistoryTable.defaultHistoryTTL(),
        calendar: Calendar = .current,
        shouldPurgeOnInitialTransaction: Bool = true,
        requireArchivedBeforeDelete: Bool = true,
        deleteArchivedExpiredTransactions: Bool = true,
        archiveBatchSize: Int = 2_000,
        archiveMaxBatches: Int = 32
    ) {
        self.store = store
        self.historyTTL = historyTTL
        self.calendar = calendar
        self.requireArchivedBeforeDelete = requireArchivedBeforeDelete
        self.deleteArchivedExpiredTransactions = deleteArchivedExpiredTransactions
        self.archiveBatchSize = archiveBatchSize
        self.archiveMaxBatches = archiveMaxBatches
        let now = Date()
        let initialNextDelay = Self.randomHistoryPurgeDelay(ttl: historyTTL, calendar: calendar, now: now)
        let initialBackfill = TimeInterval.random(in: 0 ... max(0, initialNextDelay))
        self.historyPurgeState = shouldPurgeOnInitialTransaction ? .pending : .idle
        self.lastHistoryPurgeDate = now.addingTimeInterval(-initialBackfill)
        self.nextHistoryPurgeDate = shouldPurgeOnInitialTransaction ? now : now.addingTimeInterval(initialNextDelay)
        let initialArchiveDelay = Self.randomHistoryArchiveDelay()
        let initialArchiveBackfill = TimeInterval.random(in: 0 ... max(0, initialArchiveDelay))
        self.historyArchiveState = .idle
        self.lastHistoryArchiveDate = now.addingTimeInterval(-initialArchiveBackfill)
        self.nextHistoryArchiveDate = now
        self.synchronizationState = .init()
        let synchronizers = synchronizerConfigurations.map { synchronizerConfiguration in
            synchronizerConfiguration.makeDatabaseSynchronizer(store: store)
        }
        self.synchronizers = synchronizers
        self.synchronizationStatusesByID = .init(uniqueKeysWithValues: synchronizers.map { ($0.id, .init(id: $0.id)) })
    }
    
    @discardableResult
    internal func beginHistoryPurge(now: Date = .init(), force: Bool = false) -> Bool {
        switch historyPurgeState {
        case .purging:
            return false
        case .pending:
            self.historyPurgeState = .purging
            return true
        case .idle:
            if force == false {
                guard now >= nextHistoryPurgeDate else { return false }
            }
            self.historyPurgeState = .purging
            return true
        }
    }
    
    internal func finishHistoryPurge(at date: Date = .init()) {
        self.lastHistoryPurgeDate = date
        self.nextHistoryPurgeDate = date.addingTimeInterval(Self.randomHistoryPurgeDelay(
            ttl: historyTTL,
            calendar: calendar,
            now: date
        ))
        self.historyPurgeState = .idle
    }
    
    @discardableResult
    internal func beginHistoryArchive(now: Date = .init(), force: Bool = false) -> Bool {
        switch historyArchiveState {
        case .archiving:
            return false
        case .pending:
            historyArchiveState = .archiving
            return true
        case .idle:
            if force == false {
                guard now >= nextHistoryArchiveDate else { return false }
            }
            self.historyArchiveState = .archiving
            return true
        }
    }
    
    internal func finishHistoryArchive(at date: Date = .init()) {
        self.lastHistoryArchiveDate = date
        self.nextHistoryArchiveDate = date.addingTimeInterval(Self.randomHistoryArchiveDelay())
        self.historyArchiveState = .idle
    }
    
    nonisolated internal func run(force: Bool = false) {
        guard let store = self.store else { return }
        Task { @DatabaseActor in
            let now = Date()
            let shouldArchive = beginHistoryArchive(now: now, force: force)
            let shouldPurge = beginHistoryPurge(now: now, force: force)
            guard shouldArchive || shouldPurge else { return }
            if shouldArchive { finishHistoryArchive(at: now) }
            if shouldPurge { finishHistoryPurge(at: now) }
            do {
                try store.queue.withConnection(.writer) { connection in
                    try HistoryTable.maintainHistory(
                        in: store.identifier,
                        connection: connection,
                        calendar: calendar,
                        now: now,
                        ttl: historyTTL,
                        archiveBatchSize: archiveBatchSize,
                        archiveMaxBatches: archiveMaxBatches,
                        requireArchivedCopyBeforeDelete: requireArchivedBeforeDelete,
                        deleteAfterArchiveAndTTL: shouldPurge ? deleteArchivedExpiredTransactions : false
                    )
                }
                logger.debug("Executed history maintenance: \(store.identifier)", metadata: [
                    "archive_due": "\(shouldArchive)",
                    "purge_due": "\(shouldPurge)",
                    "require_archive_before_delete": "\(requireArchivedBeforeDelete)",
                    "delete_archived_expired": "\(shouldPurge ? deleteArchivedExpiredTransactions : false)",
                ])
            } catch {
                logger.error("Failed to maintain history: \(error)")
            }
        }
    }
    
    nonisolated internal static func randomHistoryArchiveDelay() -> TimeInterval {
        TimeInterval.random(in: 6 * 60 * 60 ... 24 * 60 * 60)
    }
    
    nonisolated internal static func randomHistoryPurgeDelay(
        ttl: DateComponents,
        calendar: Calendar,
        now: Date
    ) -> TimeInterval {
        let ttlInterval = Self.ttlIntervalSeconds(ttl: ttl, calendar: calendar, now: now)
        let lower = max(60, ttlInterval / 30)
        let upper = max(lower, ttlInterval / 10)
        return TimeInterval.random(in: lower ... upper)
    }
    
    nonisolated internal static func ttlIntervalSeconds(
        ttl: DateComponents,
        calendar: Calendar,
        now: Date
    ) -> TimeInterval {
        let delta = Self.negated(ttl)
        guard let expiration = calendar.date(byAdding: delta, to: now) else {
            return 30 * 24 * 60 * 60
        }
        let interval = now.timeIntervalSince(expiration)
        return max(1, interval)
    }
    
    nonisolated internal static func negated(_ ttl: DateComponents) -> DateComponents {
        var delta = DateComponents()
        if let year = ttl.year { delta.year = -year }
        if let month = ttl.month { delta.month = -month }
        if let weekOfYear = ttl.weekOfYear { delta.weekOfYear = -weekOfYear }
        if let day = ttl.day { delta.day = -day }
        if let hour = ttl.hour { delta.hour = -hour }
        if let minute = ttl.minute { delta.minute = -minute }
        if let second = ttl.second { delta.second = -second }
        return delta
    }
}

// MARK: Synchronization

extension HistoryState {
    internal func synchronizationStatus(for id: String) -> SynchronizationStatus? {
        synchronizationStatusesByID[id]
    }
    
    internal func allSynchronizationStatuses() -> [SynchronizationStatus] {
        synchronizers.compactMap { synchronizationStatusesByID[$0.id] }
    }
    
    internal func scheduleSynchronizationIfNeeded() {
        guard synchronizers.isEmpty == false else {
            return
        }
        if synchronizationState.task != nil {
            self.synchronizationState.isPending = true
            for synchronizer in synchronizers {
                updateSynchronizationStatus(for: synchronizer.id, phase: .scheduled)
            }
            return
        }
        self.synchronizationState.isPending = false
        for synchronizer in synchronizers {
            updateSynchronizationStatus(for: synchronizer.id, phase: .scheduled)
        }
        self.synchronizationState.task = Task { @DatabaseActor in
            await self.runSynchronizationLoop()
        }
    }
    
    internal func scheduleCloudKitSyncIfNeeded() {
        scheduleSynchronizationIfNeeded()
    }
    
    internal func runSynchronizationLoop() async {
        guard let store = self.store else {
            return
        }
        defer {
            self.synchronizationState.task = nil
            if synchronizationState.isPending {
                self.synchronizationState.isPending = false
                scheduleSynchronizationIfNeeded()
            } else {
                for synchronizer in synchronizers {
                    updateSynchronizationStatus(for: synchronizer.id, phase: .idle)
                }
            }
        }
        for synchronizer in synchronizers {
            do {
                updateSynchronizationStatus(for: synchronizer.id, phase: .preparing)
                #if DEBUG
                // Temporary - used to reset CloudKit records.
                if false, let replicator = synchronizer as? DatabaseConfiguration.CloudKitDatabase.Replicator {
                    try await replicator.prepare()
                    try await replicator.eraseCloudKitData(recreateEmptyZone: true)
                }
                #endif
                try await synchronizer.prepare()
                updateSynchronizationStatus(for: synchronizer.id, phase: .sending)
                let token = synchronizer.lastProcessedToken
                let transactions = try fetchTransactions(excludingAuthor: synchronizer.remoteAuthor, after: token)
                try await synchronizer.sync(transactions: transactions)
                updateSynchronizationStatus(for: synchronizer.id, phase: .finished)
            } catch {
                logger.error("Synchronizer error: \(error)", metadata: [
                    "store_identifier": "\(store.identifier)",
                    "synchronizer_id": "\(synchronizer.id)"
                ])
                updateSynchronizationStatus(for: synchronizer.id, phase: .failed, error: error)
            }
        }
    }
    
    nonisolated internal func fetchTransactions(
        excludingAuthor author: String,
        after token: DatabaseHistoryToken?
    ) throws -> [DatabaseHistoryTransaction] {
        guard let store = self.store else {
            return []
        }
        let watermark = token?.watermark(for: store.identifier) ?? 0
        let descriptor = HistoryDescriptor<DatabaseHistoryTransaction>()
        return try store.fetchHistory(descriptor)
            .filter { transaction in
                transaction.author != author &&
                transaction.changes.contains { changeIdentifier(of: $0) > watermark }
            }
            .sorted { $0.transactionIdentifier < $1.transactionIdentifier }
    }
    
    nonisolated private func changeIdentifier(of change: HistoryChange) -> Int64 {
        switch change {
        case .insert(let insert): insert.changeIdentifier as? Int64 ?? 0
        case .update(let update): update.changeIdentifier as? Int64 ?? 0
        case .delete(let delete): delete.changeIdentifier as? Int64 ?? 0
        @unknown default:
            fatalError()
        }
    }
    
    internal func runCloudKitSyncLoop() async {
        await runSynchronizationLoop()
    }
    
    internal func updateSynchronizationStatus(
        for id: String,
        phase: SynchronizationPhase,
        error: (any Swift.Error)? = nil
    ) {
        guard var status = self.synchronizationStatusesByID[id] else {
            return
        }
        status.phase = phase
        status.isPending = self.synchronizationState.isPending
        if phase == .finished {
            status.lastSyncDate = .init()
            status.lastError = nil
        } else if let error {
            status.lastError = error
        }
        self.synchronizationStatusesByID[id] = status
        DispatchQueue.main.async {
            NotificationCenter.default.post(
                name: .dataStoreSynchronizationStatusDidChange,
                object: self.store,
                userInfo: ["id": id, "status": status]
            )
        }
    }
    
    internal func updateCloudKitSyncStatus(phase: SynchronizationPhase, error: (any Swift.Error)? = nil) {
        guard let cloudKitSynchronizer = self.synchronizers.first(where: { $0 is DatabaseConfiguration.CloudKitDatabase.Replicator }) else {
            return
        }
        updateSynchronizationStatus(for: cloudKitSynchronizer.id, phase: phase, error: error)
    }
}
