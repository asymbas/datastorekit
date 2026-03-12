//
//  SaveChangesResultProtocol.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import SwiftData

public protocol SaveChangesResult<SnapshotType>: AnyObject, Sendable, SendableMetatype {
    associatedtype SnapshotType: DataStoreSnapshot
    var storeIdentifier: String { get }
    var remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier] { get }
    var snapshotsToReregister: [PersistentIdentifier: SnapshotType] { get }
    
    init(
        for storeIdentifier: String,
        remappedIdentifiers: [PersistentIdentifier: PersistentIdentifier],
        snapshotsToReregister: [PersistentIdentifier: SnapshotType]
    )
}

extension DataStoreSaveChangesResult: SaveChangesResult {}

public final class DatabaseSaveChangesResult<T, Snapshot>: SaveChangesResult
where T: PersistentModel, Snapshot: DataStoreSnapshot {
    public typealias SnapshotType = Snapshot
    nonisolated public let storeIdentifier: String
    nonisolated public let remappedIdentifiers: [PersistentIdentifier : PersistentIdentifier]
    
    nonisolated public let snapshotsToReregister: [PersistentIdentifier : Snapshot]
    
    nonisolated public init(
        for storeIdentifier: String,
        remappedIdentifiers: [PersistentIdentifier : PersistentIdentifier],
        snapshotsToReregister: [PersistentIdentifier : Snapshot]
    ) {
        self.storeIdentifier = storeIdentifier
        self.remappedIdentifiers = remappedIdentifiers
        self.snapshotsToReregister = snapshotsToReregister
    }
}
