//
//  HistoryTable.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public enum HistoryTable: String {
    nonisolated public static let tableName: String = "_History"
    /// The auto-incrementing primary key that is unique per row (per event, not transaction).
    case pk
    /// The type of data store operation (insert, update, or delete).
    case event
    /// The table name for the model being logged.
    case recordTarget = "record_target"
    /// The primary key of the model being logged.
    case recordIdentifier = "record_identifier"
    /// The modified columns when its an update or preserved values when its a delete.
    case context
    /// The timestamp of the transaction when it begins.
    case timestamp
    /// An author that was configured in and provided by `ModelContext`.
    case author
    /// The group key that represents the model's associated store.
    case storeIdentifier = "store_identifier"
    
    nonisolated package static var createTable: String {
        createTable(databaseName: nil, autoIncrement: true)
    }
    
    nonisolated package static func createTable(
        databaseName: String?,
        autoIncrement: Bool
    ) -> String {
        let qualifiedTableName = if let databaseName {
            "\(databaseName).\(Self.tableName)"
        } else {
            Self.tableName
        }
        let primaryKey = autoIncrement
        ? "INTEGER PRIMARY KEY AUTOINCREMENT"
        : "INTEGER PRIMARY KEY"
        return """
            CREATE TABLE IF NOT EXISTS \(qualifiedTableName) (
                \(Self.pk.rawValue) \(primaryKey),
                \(Self.event.rawValue) TEXT NOT NULL,
                \(Self.recordTarget.rawValue) TEXT NOT NULL,
                \(Self.recordIdentifier.rawValue) TEXT NOT NULL,
                \(Self.context.rawValue) TEXT,
                \(Self.timestamp.rawValue) INTEGER NOT NULL,
                \(Self.author.rawValue) TEXT,
                \(Self.storeIdentifier.rawValue) TEXT NOT NULL
            )
            """
    }
}
