//
//  HistoryState.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreRuntime
import DataStoreSupport
import Foundation
import Logging
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.transaction")

@DatabaseActor internal final class HistoryState: Sendable {
    nonisolated internal unowned let store: DatabaseStore
    internal let cloudKitReplicator: CloudKitReplicator?
    internal let cloudKitSyncState: CloudKitSyncState
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
        cloudKit: CloudKitConfiguration?,
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
        let initialNextDelay = Self.randomHistoryPurgeDelay(
            ttl: historyTTL,
            calendar: calendar,
            now: now
        )
        let initialBackfill = TimeInterval.random(in: 0 ... max(0, initialNextDelay))
        self.historyPurgeState = shouldPurgeOnInitialTransaction ? .pending : .idle
        self.lastHistoryPurgeDate = now.addingTimeInterval(-initialBackfill)
        self.nextHistoryPurgeDate = shouldPurgeOnInitialTransaction
        ? now : now.addingTimeInterval(initialNextDelay)
        let initialArchiveDelay = Self.randomHistoryArchiveDelay()
        let initialArchiveBackfill = TimeInterval.random(in: 0 ... max(0, initialArchiveDelay))
        self.historyArchiveState = .idle
        self.lastHistoryArchiveDate = now.addingTimeInterval(-initialArchiveBackfill)
        self.nextHistoryArchiveDate = now
#if canImport(CloudKit)
        self.cloudKitSyncState = .init()
            if let cloudKit {
                logger.info("Setting up CloudKit configuration: \(cloudKit)")
                cloudKitReplicator = CloudKitReplicator(store: store, configuration: cloudKit)
//                cloudKitSyncState.task?.cancel()
//                cloudKitSyncState.task = Task {
                Task {
                    do {
                        try await cloudKitReplicator?.prepare()
                        try await cloudKitReplicator?.sync()
                    } catch {
                        logger.error("CloudKit sync error: \(error)")
                    }
                }
//                }
            } else {
                self.cloudKitReplicator = nil
            }
#endif
    }
    
    @discardableResult
    internal func beginHistoryPurge(now: Date = .init(), force: Bool = false) -> Bool {
        switch historyPurgeState {
        case .purging:
            return false
        case .pending:
            historyPurgeState = .purging
            return true
        case .idle:
            if force == false {
                guard now >= nextHistoryPurgeDate else { return false }
            }
            historyPurgeState = .purging
            return true
        }
    }
    
    internal func finishHistoryPurge(at date: Date = .init()) {
        lastHistoryPurgeDate = date
        nextHistoryPurgeDate = date.addingTimeInterval(
            Self.randomHistoryPurgeDelay(ttl: historyTTL, calendar: calendar, now: date)
        )
        historyPurgeState = .idle
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
            historyArchiveState = .archiving
            return true
        }
    }
    
    internal func finishHistoryArchive(at date: Date = .init()) {
        lastHistoryArchiveDate = date
        nextHistoryArchiveDate = date.addingTimeInterval(Self.randomHistoryArchiveDelay())
        historyArchiveState = .idle
    }
    
    nonisolated internal func run(force: Bool = false) {
        Task { @DatabaseActor in
            let now = Date()
            let shouldArchive = beginHistoryArchive(now: now, force: force)
            let shouldPurge = beginHistoryPurge(now: now, force: force)
            guard shouldArchive || shouldPurge else { return }
            if shouldArchive {
                finishHistoryArchive(at: now)
            }
            if shouldPurge {
                finishHistoryPurge(at: now)
            }
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
                logger.debug(
                    "Executed history maintenance: \(store.identifier)",
                    metadata: [
                        "archive_due": "\(shouldArchive)",
                        "purge_due": "\(shouldPurge)",
                        "require_archive_before_delete": "\(requireArchivedBeforeDelete)",
                        "delete_archived_expired": "\(shouldPurge ? deleteArchivedExpiredTransactions : false)",
                    ]
                )
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
