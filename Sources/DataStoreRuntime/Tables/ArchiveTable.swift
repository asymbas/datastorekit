//
//  ArchiveTable.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public enum ArchiveTable: String {
    nonisolated public static let tableName: String = "_Archive"
    /// Indicates that the archive has finalized.
    case isComplete = "is_complete"
    /// Records the last cursor metadata save.
    case checkpointTimestamp = "checkpoint_timestamp"
    /// The last archived history event timestamp.
    case cursorTimestamp = "cursor_timestamp"
    /// The last archived history row identifier or primary key.
    case cursorIdentifier = "cursor_identifier"
    /// The archive year that is being partitioned.
    case year
    /// The group key that represents the model's associated store.
    case storeIdentifier = "store_identifier"
    
    nonisolated package static var createTable: String {
        """
        CREATE TABLE IF NOT EXISTS \(Self.tableName) (
            \(Self.isComplete.rawValue) INTEGER NOT NULL DEFAULT 0,
            \(Self.checkpointTimestamp.rawValue) INTEGER NOT NULL,
            \(Self.cursorTimestamp.rawValue) INTEGER NOT NULL,
            \(Self.cursorIdentifier.rawValue) INTEGER NOT NULL,
            \(Self.year.rawValue) INTEGER NOT NULL,
            \(Self.storeIdentifier.rawValue) TEXT NOT NULL,
            PRIMARY KEY (\(Self.storeIdentifier.rawValue), \(Self.year.rawValue))
        )
        """
    }
}
