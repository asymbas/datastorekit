//
//  CloudKitDatabase+CKSyncEngineDelegate.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreRuntime
import DataStoreSQL
import DataStoreSupport
import Foundation
import Logging
import SQLiteHandle
import SQLSupport
import Synchronization

#if swift(>=6.2)
import SwiftData
#else
@preconcurrency import SwiftData
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.cloudkit")

#if canImport(CloudKit)

import CloudKit

extension DatabaseConfiguration.CloudKitDatabase.Replicator: CKSyncEngineDelegate {
    /// Inherited from `CKSyncEngineDelegate.handleEvent(_:syncEngine:)`.
    public func handleEvent(_ event: CKSyncEngine.Event, syncEngine: CKSyncEngine) async {
        do {
            logger.trace("Received sync engine event.", metadata: ["event": "\(event)"])
            switch event {
            case .stateUpdate(let event):
                try saveState(stateSerialization: event.stateSerialization, clearErrorCode: true)
                logger.debug("Saved sync state update: \(event.stateSerialization)")
            case .accountChange(let event):
                logger.trace("Received CloudKit account change.", metadata: ["change_type": "\(event.changeType)"])
                guard isHandlingAccountChange == false else {
                    logger.trace("Skipped CloudKit account change handling because another change is already being processed.")
                    return
                }
                self.isHandlingAccountChange = true
                defer { self.isHandlingAccountChange = false }
                switch event.changeType {
                case .signIn: try scheduleInitialUploadIfNeeded()
                case .switchAccounts, .signOut: try resetForAccountChange()
                @unknown default: logger.trace("Encountered unknown CloudKit account change type.")
                }
            case .willFetchChanges:
                break
            case .willFetchRecordZoneChanges:
                break
            case .fetchedRecordZoneChanges(let event):
                let changed = event.modifications.map(\.record)
                let deleted = event.deletions.map(\.recordID)
                logger.trace("Received fetched record zone changes.", metadata: [
                    "changed_count": "\(changed.count)",
                    "deleted_count": "\(deleted.count)"
                ])
                if changed.isEmpty == false {
                    logger.trace("Loaded changed record names from fetched record zone changes.", metadata: [
                        "record_names": "\(changed.map { $0.recordID.recordName })"
                    ])
                }
                if deleted.isEmpty == false {
                    logger.trace("Loaded deleted record names from fetched record zone changes.", metadata: [
                        "record_names": "\(deleted.map { $0.recordName })"
                    ])
                }
                try applyRemoteChanges(changed: event.modifications, deleted: event.deletions)
                try saveState(clearErrorCode: true)
                logger.trace("Completed fetched record zone change handling.")
            case .didFetchRecordZoneChanges:
                break
            case .didFetchChanges:
                break
            case .fetchedDatabaseChanges:
                logger.trace("Received fetched database changes.")
                break
            case .willSendChanges:
                break
            case .sentDatabaseChanges:
                logger.trace("Processed sent database changes.")
                try saveState(clearErrorCode: true)
            case .sentRecordZoneChanges(let event):
                logger.trace("Processed sent record zone changes.", metadata: [
                    "saved_record_count": "\(event.savedRecords.count)",
                    "deleted_record_id_count": "\(event.deletedRecordIDs.count)",
                    "failed_record_save_count": "\(event.failedRecordSaves.count)",
                    "failed_record_delete_count": "\(event.failedRecordDeletes.count)"
                ])
                for savedRecord in event.savedRecords {
                    enqueuedChangesByRecordID[savedRecord.recordID] = nil
                    try persistSavedRecordMetadata(savedRecord)
                    logger.trace("Persisted metadata for saved CloudKit record.", metadata: [
                        "record_type": "\(savedRecord.recordType)",
                        "record_name": "\(savedRecord.recordID.recordName)"
                    ])
                }
                for deletedRecordID in event.deletedRecordIDs {
                    enqueuedChangesByRecordID[deletedRecordID] = nil
                    try deleteRecordMetadata(recordName: deletedRecordID.recordName)
                    logger.trace("Removed metadata for deleted CloudKit record.", metadata: [
                        "record_name": "\(deletedRecordID.recordName)"
                    ])
                }
                var newPendingRecordZoneChanges = [CKSyncEngine.PendingRecordZoneChange]()
                var newPendingDatabaseChanges = [CKSyncEngine.PendingDatabaseChange]()
                for failedRecordSave in event.failedRecordSaves {
                    let recordID = failedRecordSave.record.recordID
                    let error = failedRecordSave.error
                    logger.error("Handling failed CloudKit record save.", metadata: [
                        "record_name": "\(recordID.recordName)",
                        "record_type": "\(failedRecordSave.record.recordType)",
                        "error_code": "\(error.code.rawValue)",
                        "error": "\(error)"
                    ])
                    switch error.code {
                    case .serverRecordChanged:
                        logger.trace("Resolving CloudKit server record conflict.", metadata: [
                            "record_name": "\(recordID.recordName)",
                            "record_type": "\(failedRecordSave.record.recordType)"
                        ])
                        let conflictMetadata = try loadRecordMetadata(recordName: recordID.recordName)
                        let isReferenceRecord = conflictMetadata?.targetPrimaryKey != nil
                        logger.trace("Resolved conflict ownership details.", metadata: [
                            "record_name": "\(recordID.recordName)",
                            "is_reference_record": "\(isReferenceRecord)"
                        ])
                        if isReferenceRecord {
                            logger.trace("Conflict belongs to intermediary reference record.", metadata: [
                                "record_name": "\(recordID.recordName)"
                            ])
                            if let serverRecord = loadServerRecord(from: error) {
                                try persistSavedRecordMetadata(serverRecord)
                                logger.trace("Persisted metadata from server conflict record for intermediary reference.", metadata: [
                                    "record_name": "\(recordID.recordName)"
                                ])
                            } else {
                                logger.trace("Server conflict record was unavailable for intermediary reference.", metadata: [
                                    "record_name": "\(recordID.recordName)"
                                ])
                            }
                            newPendingRecordZoneChanges.append(.saveRecord(recordID))
                            logger.trace("Re-enqueued intermediary reference record after conflict.", metadata: [
                                "record_name": "\(recordID.recordName)"
                            ])
                            break
                        }
                        let entityName: String
                        let primaryKey: String
                        let persistentIdentifier: PersistentIdentifier
                        if let enqueued = enqueuedChangesByRecordID[recordID] {
                            entityName = enqueued.entityName
                            primaryKey = enqueued.primaryKey()
                            persistentIdentifier = enqueued
                        } else if let conflictMetadata, conflictMetadata.targetPrimaryKey == nil {
                            entityName = conflictMetadata.entityName
                            primaryKey = conflictMetadata.primaryKey
                            persistentIdentifier = try .identifier(for: store.identifier, entityName: conflictMetadata.entityName, primaryKey: conflictMetadata.primaryKey)
                        } else {
                            logger.trace("Skipped conflict resolution because root ownership could not be resolved.", metadata: [
                                "record_name": "\(recordID.recordName)"
                            ])
                            break
                        }
                        logger.trace("Resolved root conflict target.", metadata: [
                            "record_name": "\(recordID.recordName)",
                            "entity_name": "\(entityName)",
                            "primary_key": "\(primaryKey)"
                        ])
                        guard let _ = try snapshot(for: persistentIdentifier) else {
                            logger.trace("Skipped conflict resolution because the local snapshot was missing.", metadata: [
                                "record_name": "\(recordID.recordName)",
                                "entity_name": "\(entityName)",
                                "primary_key": "\(primaryKey)"
                            ])
                            break
                        }
                        guard let serverRecord = loadServerRecord(from: error) else {
                            logger.trace("Skipped conflict resolution because the server record was missing.", metadata: [
                                "record_name": "\(recordID.recordName)"
                            ])
                            break
                        }
                        let _ = try Store.Snapshot(
                            record: serverRecord,
                            store: store
                        ) { recordName, destinationEntityName in
                            try self.loadRecordMetadata(recordName: recordName)?.primaryKey
                        }
                        logger.trace("Loaded conflict snapshots for merge.", metadata: [
                            "record_name": "\(recordID.recordName)",
                            "entity_name": "\(entityName)",
                            "primary_key": "\(primaryKey)"
                        ])
                        newPendingRecordZoneChanges.append(.saveRecord(recordID))
                        logger.trace("Re-enqueued root record after conflict merge.", metadata: [
                            "record_name": "\(recordID.recordName)"
                        ])
                    case .zoneNotFound:
                        logger.trace("Recovering from missing CloudKit zone during save.", metadata: [
                            "record_name": "\(recordID.recordName)"
                        ])
                        newPendingDatabaseChanges.append(.saveZone(CKRecordZone(zoneID: recordID.zoneID)))
                        newPendingRecordZoneChanges.append(.saveRecord(recordID))
                        try deleteRecordMetadata(recordName: recordID.recordName)
                        logger.trace("Re-enqueued zone save and record save after missing zone failure.", metadata: [
                            "record_name": "\(recordID.recordName)"
                        ])
                    case .unknownItem:
                        logger.trace("Recovering from missing CloudKit item during save.", metadata: [
                            "record_name": "\(recordID.recordName)"
                        ])
                        newPendingRecordZoneChanges.append(.saveRecord(recordID))
                        try deleteRecordMetadata(recordName: recordID.recordName)
                        logger.trace("Re-enqueued record save after missing item failure.", metadata: [
                            "record_name": "\(recordID.recordName)"
                        ])
                    case
                            .networkFailure,
                            .networkUnavailable,
                            .zoneBusy,
                            .serviceUnavailable,
                            .notAuthenticated,
                            .operationCancelled:
                        logger.trace("Encountered transient CloudKit save failure.", metadata: [
                            "record_name": "\(recordID.recordName)",
                            "error_code": "\(error.code.rawValue)"
                        ])
                        break
                    default:
                        logger.trace("Encountered unhandled CloudKit save failure.", metadata: [
                            "record_name": "\(recordID.recordName)",
                            "error_code": "\(error.code.rawValue)",
                            "error": "\(error)"
                        ])
                    }
                }
                for (recordID, error) in event.failedRecordDeletes {
                    switch error.code {
                    case .unknownItem, .zoneNotFound:
                        enqueuedChangesByRecordID[recordID] = nil
                        try deleteRecordMetadata(recordName: recordID.recordName)
                        logger.trace("Resolved failed delete as already removed remotely.", metadata: [
                            "record_name": "\(recordID.recordName)"
                        ])
                    case
                            .networkFailure,
                            .networkUnavailable,
                            .zoneBusy,
                            .serviceUnavailable,
                            .notAuthenticated,
                            .operationCancelled:
                        logger.trace("Encountered transient CloudKit delete failure: \(error)", metadata: [
                            "record_name": "\(recordID.recordName)",
                            "error_code": "\(error.code.rawValue)"
                        ])
                        break
                    default:
                        logger.trace("Encountered unhandled CloudKit delete failure: \(error)", metadata: [
                            "record_name": "\(recordID.recordName)",
                            "error_code": "\(error.code.rawValue)"
                        ])
                    }
                }
                syncEngine.state.add(pendingDatabaseChanges: newPendingDatabaseChanges)
                syncEngine.state.add(pendingRecordZoneChanges: newPendingRecordZoneChanges)
                logger.trace("Added retry changes after processing sent record zone changes.", metadata: [
                    "pending_database_change_count": "\(newPendingDatabaseChanges.count)",
                    "pending_record_zone_change_count": "\(newPendingRecordZoneChanges.count)"
                ])
                try saveState(clearErrorCode: true)
                logger.trace("Completed sent record zone change handling.")
            case .didSendChanges:
                break
            @unknown default:
                break
            }
        } catch {
            logger.trace("Failed while handling sync engine event: \(error)")
            try? saveState(errorCode: cloudKitErrorCodeString(error))
        }
    }
    
    public func nextRecordZoneChangeBatch(_ context: CKSyncEngine.SendChangesContext, syncEngine: CKSyncEngine)
    async -> CKSyncEngine.RecordZoneChangeBatch? {
        let scope = context.options.scope
        let changes = syncEngine.state.pendingRecordZoneChanges.filter { scope.contains($0) }
        logger.debug("Creating record zone change batch.", metadata: [
            "pending_total": "\(syncEngine.state.pendingRecordZoneChanges.count)",
            "filtered_count": "\(changes.count)",
            "scope": "\(scope)"
        ])
        let cached = cachedRecordsForBatch
        let enqueued = enqueuedChangesByRecordID
        let batch = await CKSyncEngine.RecordZoneChangeBatch(pendingChanges: changes) { recordID in
            if let record = cached[recordID] {
                return record
            }
            guard let identifier = enqueued[recordID] else {
                syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
                return nil
            }
            guard self.configuration.delegate.shouldSyncEntity(identifier.entityName) else {
                syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
                return nil
            }
            guard let currentSnapshot = try? await self.snapshot(for: identifier) else {
                syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
                return nil
            }
            guard let records = try? await self.projectedRecords(for: currentSnapshot) else {
                syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
                return nil
            }
            return records.first { $0.recordID == recordID }
        }
        cachedRecordsForBatch.removeAll()
        return batch
    }
}

#endif
