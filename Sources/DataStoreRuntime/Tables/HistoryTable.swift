//
//  HistoryTable.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore

/// A type that defines the history table definition for tracking model changes.
package enum HistoryTable: String {
    nonisolated package static let tableName: String = "_History"
    /// The primary key for the change event.
    case pk
    /// The type of change operation (insert, update, or delete).
    case event
    /// The name of the entity whose model data has changed.
    case entityName = "entity_name"
    /// The primary key of the entity whose model data has changed.
    case entityPrimaryKey = "entity_pk"
    /// The changed properties when it's an update or the preserved values keyed by their properties when its a delete.
    case context
    /// The timestamp of the transaction before any change operations occur.
    case timestamp
    /// An author that was assigned in an `EditingState`.
    case author
    /// The group key that represents the model's associated store.
    case storeIdentifier = "store_identifier"
    
    nonisolated package static var createTable: String {
        createTable(databaseName: nil, autoIncrement: true)
    }
    
    nonisolated package static func createTable(databaseName: String?, autoIncrement: Bool) -> String {
        let qualifiedTableName = if let databaseName {
            "\(databaseName).\(Self.tableName)"
        } else {
            Self.tableName
        }
        let primaryKey = autoIncrement ? "INTEGER PRIMARY KEY AUTOINCREMENT" : "INTEGER PRIMARY KEY"
        return """
            CREATE TABLE IF NOT EXISTS \(qualifiedTableName) (
                \(Self.pk.rawValue) \(primaryKey),
                \(Self.event.rawValue) TEXT NOT NULL,
                \(Self.entityName.rawValue) TEXT NOT NULL,
                \(Self.entityPrimaryKey.rawValue) TEXT NOT NULL,
                \(Self.context.rawValue) TEXT,
                \(Self.timestamp.rawValue) INTEGER NOT NULL,
                \(Self.author.rawValue) TEXT,
                \(Self.storeIdentifier.rawValue) TEXT NOT NULL
            )
            """
    }
    
    nonisolated package static func changedPropertyNames(_ context: String?) -> Set<String> {
        guard let context, context.isEmpty == false else { return [] }
        let names = Set(context.split(separator: ",").map(String.init))
        return names
    }
    
    package struct Row: Sendable {
        nonisolated package let pk: Int64
        nonisolated package let changeType: DataStoreOperation
        nonisolated package let transactionIdentifier: Int64
        nonisolated package let entityName: String
        nonisolated package let entityPrimaryKey: String
        nonisolated package let author: String?
        nonisolated package let context: String?
        
        nonisolated package init(
            pk: Int64,
            changeType: DataStoreOperation,
            transactionIdentifier: Int64,
            entityName: String,
            entityPrimaryKey: String,
            author: String?,
            context: String?
        ) {
            self.pk = pk
            self.changeType = changeType
            self.transactionIdentifier = transactionIdentifier
            self.entityName = entityName
            self.entityPrimaryKey = entityPrimaryKey
            self.author = author
            self.context = context
        }
    }
}
