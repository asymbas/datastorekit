//
//  DataStore+HistoryProviding.swift
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
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.transaction")

extension DatabaseStore: HistoryProviding {
    /// Inherited from `HistoryProviding.HistoryType`.
    public typealias HistoryType = DatabaseHistoryTransaction
    
    /// Inherited from `HistoryProviding.historyType`.
    nonisolated public static var historyType: HistoryType.Type {
        HistoryType.self
    }
    
    /// Inherited from `HistoryProviding.fetchHistory(_:)`.
    nonisolated public func fetchHistory(_ descriptor: HistoryDescriptor<HistoryType>) throws -> [HistoryType] {
        let connection = try self.queue.connection(.reader)
        return try fetchHistory(descriptor, connection: connection)
    }
    
    nonisolated public func fetchHistory(
        _ descriptor: HistoryDescriptor<HistoryType>,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws -> [HistoryType] {
        #if DEBUG
        let translator = SQLHistoryTranslator<HistoryType>()
        _ = try translator.translate(descriptor)
        let start = Date()
        let logThisCall = shouldLogHistoryFetch(limit: descriptor.fetchLimit, hasPredicate: descriptor.predicate != nil)
        if logThisCall {
            let counts = HistoryTable.counts(in: self.identifier, connection: connection)
            logger.debug("History fetch (start).", metadata: [
                "limit": "\(descriptor.fetchLimit)",
                "duration": "\(Date().timeIntervalSince(start))s",
                "rows": "\(counts.rows)",
                "transactions": "\(counts.transactions)",
                "thread": "\(threadDescription)"
            ])
            logger.trace("History fetch call stack:\n\(Thread.callStackSymbols.joined(separator: "\n"))")
        }
        #endif
        let storeIdentifierColumn = HistoryTable.storeIdentifier.rawValue
        let transactionIdentifierColumn = HistoryTable.timestamp.rawValue
        let primaryKeyColumn = HistoryTable.pk.rawValue
        let fetchLimit: Int = descriptor.fetchLimit > UInt64(Int.max) ? Int.max : Int(descriptor.fetchLimit)
        let shouldLimit = descriptor.fetchLimit > 0
        let pageSize: Int = {
            switch shouldLimit {
            case true: max(64, min(512, fetchLimit))
            case false: 256
            }
        }()
        var results = [DatabaseHistoryTransaction]()
        results.reserveCapacity(shouldLimit ? fetchLimit : 0)
        var seenTransactionIdentifiers = Set<Int64>()
        if shouldLimit { seenTransactionIdentifiers.reserveCapacity(fetchLimit) }
        func shouldStop() -> Bool { shouldLimit && results.count >= fetchLimit }
        func parseTransactions(from rows: [[String: any Sendable]]) throws -> [HistoryType] {
            var transactions = [HistoryType]()
            transactions.reserveCapacity(shouldLimit ? min(pageSize, fetchLimit) : pageSize)
            var currentTransactionIdentifier: Int64?
            var currentStoreIdentifier: String?
            var currentAuthor: String?
            var currentTimestamp: Date?
            var currentChanges = [HistoryChange]()
            func flushCurrent() {
                guard let transactionIdentifier = currentTransactionIdentifier,
                      let storeIdentifier = currentStoreIdentifier,
                      let timestamp = currentTimestamp else {
                    return
                }
                transactions.append(DatabaseHistoryTransaction(
                    timestamp: timestamp,
                    transactionIdentifier: transactionIdentifier,
                    token: .init(
                        id: Int(transactionIdentifier),
                        tokenValue: [storeIdentifier: transactionIdentifier]
                    ),
                    storeIdentifier: storeIdentifier,
                    author: currentAuthor,
                    changes: currentChanges
                ))
            }
            for row in rows {
                let transactionIdentifier = row[transactionIdentifierColumn] as? Int64 ?? -1
                if currentTransactionIdentifier != transactionIdentifier {
                    flushCurrent()
                    currentTransactionIdentifier = transactionIdentifier
                    currentStoreIdentifier = row[storeIdentifierColumn] as? String ?? self.identifier
                    currentAuthor = row[HistoryTable.author.rawValue] as? String
                    currentTimestamp = Date(timeIntervalSince1970: Double(transactionIdentifier) / 1_000_000)
                    currentChanges.removeAll(keepingCapacity: true)
                }
                guard let type = row[HistoryTable.event.rawValue] as? String,
                      let entityName = row[HistoryTable.entityName.rawValue] as? String,
                      let entityPrimaryKey = row[HistoryTable.entityPrimaryKey.rawValue] as? String,
                      let modelType = TypeRegistry.getType(forName: entityName) as? any PersistentModel.Type else {
                    logger.warning("Unable to parse row for history transaction changes: \(row)")
                    continue
                }
                guard let storeIdentifier = currentStoreIdentifier else {
                    throw Self.Error.invalidStoreIdentifier
                }
                let persistentIdentifier = try PersistentIdentifier.identifier(
                    for: storeIdentifier,
                    entityName: entityName,
                    primaryKey: entityPrimaryKey
                )
                let changeIdentifier = row[primaryKeyColumn] as? Int64 ?? 0
                switch DataStoreOperation(rawValue: type) {
                case .insert:
                    currentChanges.append(makeInsertChange(
                        as: modelType,
                        transactionIdentifier: transactionIdentifier,
                        changeIdentifier: changeIdentifier,
                        changedPersistentIdentifier: persistentIdentifier
                    ))
                case .update:
                    currentChanges.append(makeUpdateChange(
                        as: modelType,
                        transactionIdentifier: transactionIdentifier,
                        changeIdentifier: changeIdentifier,
                        changedPersistentIdentifier: persistentIdentifier,
                        changedPropertyNames: (row[HistoryTable.propertyNames.rawValue] as? String)?
                            .split(separator: ",")
                            .map(String.init) ?? []
                    ))
                case .delete:
                    currentChanges.append(makeDeleteChange(
                        as: modelType,
                        transactionIdentifier: transactionIdentifier,
                        changeIdentifier: changeIdentifier,
                        changedPersistentIdentifier: persistentIdentifier,
                        changedPropertyNames: (row[HistoryTable.propertyNames.rawValue] as? String)?
                            .split(separator: ",")
                            .map(String.init) ?? [],
                        preservedValues: row[HistoryTable.preservedValues.rawValue] as? Data
                    ))
                default:
                    preconditionFailure("Invalid history change type: \(type)")
                }
            }
            flushCurrent()
            return transactions
        }
        func fetchTransactionIdentifiers(in databaseName: String, before: Int64?) throws -> [Int64] {
            var sql: String
            var bindings: [any Sendable] = [self.identifier]
            if let before {
                sql = """
                SELECT DISTINCT \(transactionIdentifierColumn) AS \(transactionIdentifierColumn)
                FROM \(databaseName).\(HistoryTable.tableName)
                WHERE \(storeIdentifierColumn) = ?
                AND \(transactionIdentifierColumn) < ?
                ORDER BY \(transactionIdentifierColumn) DESC
                LIMIT ?
                """
                bindings.append(before)
                bindings.append(Int64(pageSize))
            } else {
                sql = """
                SELECT DISTINCT \(transactionIdentifierColumn) AS \(transactionIdentifierColumn)
                FROM \(databaseName).\(HistoryTable.tableName)
                WHERE \(storeIdentifierColumn) = ?
                ORDER BY \(transactionIdentifierColumn) DESC
                LIMIT ?
                """
                bindings.append(Int64(pageSize))
            }
            return try connection.fetch(sql, bindings: bindings).compactMap { $0[0] as? Int64 }
        }
        func fetchRows(in databaseName: String, transactionIdentifiers: [Int64]) throws -> [[String: any Sendable]] {
            guard transactionIdentifiers.isEmpty == false else { return [] }
            let placeholders = Array(repeating: "?", count: transactionIdentifiers.count)
                .joined(separator: ", ")
            let sql = """
                SELECT * FROM \(databaseName).\(HistoryTable.tableName)
                WHERE \(storeIdentifierColumn) = ?
                AND \(transactionIdentifierColumn) IN (\(placeholders))
                ORDER BY \(transactionIdentifierColumn) DESC,
                \(primaryKeyColumn) DESC
                """
            var bindings: [any Sendable] = [self.identifier]
            bindings.append(contentsOf: transactionIdentifiers)
            return try connection.query(sql, bindings: bindings)
        }
        func scanDatabase(named databaseName: String) throws {
            var before: Int64? = nil
            while true {
                if shouldStop() { return }
                let transactionIdentifiers = try fetchTransactionIdentifiers(in: databaseName, before: before)
                guard let nextBefore = transactionIdentifiers.last else { return }
                before = nextBefore
                let rows = try fetchRows(in: databaseName, transactionIdentifiers: transactionIdentifiers)
                if rows.isEmpty { return }
                var transactions = try parseTransactions(from: rows)
                if let predicate = descriptor.predicate {
                    transactions = try transactions.filter { try predicate.evaluate($0) }
                }
                for transaction in transactions {
                    if seenTransactionIdentifiers.insert(transaction.transactionIdentifier).inserted {
                        results.append(transaction)
                        if shouldStop() { return }
                    }
                }
            }
        }
        func archivedYears(mainURL: URL) -> [Int] {
            do {
                let years = try connection.fetch(
                    """
                    SELECT DISTINCT \(ArchiveTable.year.rawValue)
                    FROM \(ArchiveTable.tableName)
                    WHERE \(ArchiveTable.storeIdentifier.rawValue) = ?
                    ORDER BY \(ArchiveTable.year.rawValue) DESC
                    """,
                    bindings: [self.identifier]
                ).compactMap { row -> Int? in
                    switch row[0] {
                    case let value as String: Int(value)
                    case let value as Int64: Int(value)
                    case let value as Int: value
                    default: nil
                    }
                }
                if years.isEmpty == false { return years }
            } catch {
                // Fetch error is okay, because the archives are lazy.
                logger.debug("Failed to read archived transactions: \(error)")
            }
            let archiveDirectoryURL = HistoryTable.archiveDirectoryURL(mainURL: mainURL)
            let baseName = mainURL.deletingPathExtension().lastPathComponent
            let prefix = "\(baseName)-Transactions-"
            let suffix = ".archive"
            guard let urls = try? FileManager.default.contentsOfDirectory(
                at: archiveDirectoryURL,
                includingPropertiesForKeys: nil,
                options: [.skipsHiddenFiles]
            ) else {
                return []
            }
            let years = urls.compactMap { url -> Int? in
                let fileName = url.lastPathComponent
                guard fileName.hasPrefix(prefix), fileName.hasSuffix(suffix) else {
                    return nil
                }
                let yearString = String(fileName.dropFirst(prefix.count).dropLast(suffix.count))
                return Int(yearString)
            }.sorted(by: >)
            return years
        }
        try scanDatabase(named: "main")
        if !shouldStop(),
           let mainURL = try connection.mainDatabaseURL() {
            for year in archivedYears(mainURL: mainURL) {
                if shouldStop() { break }
                let archiveURL = HistoryTable.makeYearlyArchiveDatabaseURL(year: year, mainURL: mainURL)
                guard FileManager.default.fileExists(atPath: archiveURL.path) else {
                    continue
                }
                let archiveName = "archive_\(year)"
                do {
                    try connection.attachDatabase(at: archiveURL, as: archiveName)
                    defer { try? connection.detachDatabaseIfAttached(named: archiveName) }
                    try scanDatabase(named: archiveName)
                } catch {
                    try? connection.detachDatabaseIfAttached(named: archiveName)
                }
            }
        }
        #if DEBUG
        if logThisCall {
            logger.debug("History fetch (end).", metadata: [
                "transactions": "\(results.count)",
                "duration": "\(Date().timeIntervalSince(start))s"
            ])
        }
        #endif
        return results
    }
    
    /// Inherited from `HistoryProviding.deleteHistory(_:)`.
    nonisolated public func deleteHistory(_ descriptor: HistoryDescriptor<HistoryType>) throws {
        let connection = try self.queue.connection(.writer)
        let transactions = try fetchHistory(descriptor, connection: connection)
        guard transactions.isEmpty == false else { return }
        let transactionIdentifierColumn = HistoryTable.timestamp.rawValue
        let storeIdentifierColumn = HistoryTable.storeIdentifier.rawValue
        let transactionIdentifiers = transactions.map(\.transactionIdentifier)
        try deleteRows(in: "main", transactionIdentifiers: transactionIdentifiers)
        guard let mainURL = try connection.mainDatabaseURL() else {
            return
        }
        var utcCalendar = Calendar(identifier: .gregorian)
        utcCalendar.timeZone = TimeZone(secondsFromGMT: 0)!
        let archiveYears = Set(transactions.map { utcCalendar.component(.year, from: $0.timestamp) }).sorted(by: >)
        for year in archiveYears {
            let archiveURL = HistoryTable.makeYearlyArchiveDatabaseURL(year: year, mainURL: mainURL)
            guard FileManager.default.fileExists(atPath: archiveURL.path) else {
                continue
            }
            let archiveName = "archive_\(year)"
            do {
                try connection.attachDatabase(at: archiveURL, as: archiveName)
                defer { try? connection.detachDatabaseIfAttached(named: archiveName) }
                try deleteRows(in: archiveName, transactionIdentifiers: transactionIdentifiers)
            } catch {
                try? connection.detachDatabaseIfAttached(named: archiveName)
                throw error
            }
        }
        func deleteRows(in databaseName: String, transactionIdentifiers: [Int64], chunkSize: Int = 500) throws {
            guard transactionIdentifiers.isEmpty == false else {
                return
            }
            var startIndex = 0
            while startIndex < transactionIdentifiers.count {
                let endIndex = min(startIndex + chunkSize, transactionIdentifiers.count)
                let chunk = Array(transactionIdentifiers[startIndex..<endIndex])
                let placeholders = Array(repeating: "?", count: chunk.count).joined(separator: ", ")
                try PreparedStatement(
                    sql: """
                    DELETE FROM \(databaseName).\(HistoryTable.tableName)
                    WHERE \(storeIdentifierColumn) = ?
                    AND \(transactionIdentifierColumn) IN (\(placeholders))
                    """,
                    bindings: [SQLValue.text(self.identifier)] + chunk.map { SQLValue.integer($0) },
                    handle: connection.handle
                ).run()
                startIndex = endIndex
            }
        }
    }
}

nonisolated private func makeInsertChange<T: PersistentModel & SendableMetatype>(
    as type: T.Type,
    transactionIdentifier: DatabaseHistoryInsert.TransactionIdentifier,
    changeIdentifier: DatabaseHistoryInsert.ChangeIdentifier,
    changedPersistentIdentifier: PersistentIdentifier
) -> HistoryChange {
    .insert(DatabaseHistoryInsert(
        as: type,
        transactionIdentifier: transactionIdentifier,
        changeIdentifier: changeIdentifier,
        changedPersistentIdentifier: changedPersistentIdentifier
    ))
}

nonisolated private func makeUpdateChange<T: PersistentModel & SendableMetatype>(
    as type: T.Type,
    transactionIdentifier: DatabaseHistoryUpdate.TransactionIdentifier,
    changeIdentifier: DatabaseHistoryUpdate.ChangeIdentifier,
    changedPersistentIdentifier: PersistentIdentifier,
    changedPropertyNames keys: [String]
) -> HistoryChange {
    .update(DatabaseHistoryUpdate(
        as: type,
        transactionIdentifier: transactionIdentifier,
        changeIdentifier: changeIdentifier,
        changedPersistentIdentifier: changedPersistentIdentifier,
        changedPropertyNames: keys
    ))
}

nonisolated private func makeDeleteChange<T: PersistentModel & SendableMetatype>(
    as type: T.Type,
    transactionIdentifier: DatabaseHistoryDelete.TransactionIdentifier,
    changeIdentifier: DatabaseHistoryDelete.ChangeIdentifier,
    changedPersistentIdentifier: PersistentIdentifier,
    changedPropertyNames keys: [String],
    preservedValues: Data?
) -> HistoryChange {
    .delete(DatabaseHistoryDelete(
        as: type,
        transactionIdentifier: transactionIdentifier,
        changeIdentifier: changeIdentifier,
        changedPersistentIdentifier: changedPersistentIdentifier,
        changedPropertyNames: keys,
        preservedValues: preservedValues
    ))
}

public struct DatabaseHistoryTransaction: HistoryTransaction {
    /// Inherited from `Identifiable.ID`.
    public typealias ID = Int64
    /// Inherited from `HistoryTransaction.TransactionIdentifier`.
    public typealias TransactionIdentifier = Int64
    /// Inherited from `HistoryTransaction.TokenType`.
    public typealias TokenType = DatabaseHistoryToken
    /// Inherited from `HistoryTransaction.timestamp`.
    ///
    /// The timestamp when the transaction was created.
    nonisolated public var timestamp: Date
    /// Inherited from `HistoryTransaction.transactionIdentifier`.
    ///
    /// The raw value of the transaction creation timestamp.
    nonisolated public var transactionIdentifier: TransactionIdentifier
    /// Inherited from `HistoryTransaction.token`.
    nonisolated public var token: TokenType
    /// Inherited from `HistoryTransaction.storeIdentifier`.
    nonisolated public var storeIdentifier: String
    /// Inherited from `HistoryTransaction.author`.
    nonisolated public var author: String?
    /// Inherited from `HistoryTransaction.changes`.
    nonisolated public var changes: [HistoryChange]
    
    /// Inherited from `Identifiable.id`.
    nonisolated public var id: ID {
        transactionIdentifier
    }
    
    nonisolated public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.id == rhs.id
    }
    
    nonisolated public func hash(into hasher: inout Hasher) {
        hasher.combine(id)
    }
    
    nonisolated internal init(
        timestamp: Date,
        transactionIdentifier: TransactionIdentifier,
        token: TokenType,
        storeIdentifier: String,
        author: String? = nil,
        changes: [HistoryChange]
    ) {
        self.timestamp = timestamp
        self.transactionIdentifier = transactionIdentifier
        self.token = token
        self.storeIdentifier = storeIdentifier
        self.author = author
        self.changes = changes
    }
    
    nonisolated public func isNewer(than token: DatabaseHistoryToken?) -> Bool {
        guard let token else { return true }
        return transactionIdentifier > token.watermark(for: storeIdentifier)
    }
}

public struct DatabaseHistoryToken: HistoryToken {
    /// Inherited from `Identifiable.ID`.
    public typealias ID = Int
    /// Inherited from `HistoryToken.TokenType`.
    public typealias TokenType = [String: Int64]
    /// Inherited from `Identifiable.id`.
    nonisolated public var id: ID
    /// Inherited from `HistoryToken.tokenValue`.
    nonisolated public var tokenValue: TokenType?
    
    nonisolated public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.id == rhs.id && lhs.tokenValue == rhs.tokenValue
    }
    
    nonisolated public static func < (lhs: Self, rhs: Self) -> Bool {
        lhs.id < rhs.id
    }
    
    nonisolated public func watermark(for storeIdentifier: String) -> Int64 {
        tokenValue?[storeIdentifier] ?? Int64(id)
    }
}

public struct DatabaseHistoryInsert<T>: HistoryInsert
where T: PersistentModel & SendableMetatype {
    /// Inherited from `HistoryInsert.Model`.
    public typealias Model = T
    /// Inherited from `HistoryInsert.TransactionIdentifier`.
    public typealias TransactionIdentifier = Int64
    /// Inherited from `HistoryInsert.ChangeIdentifier`.
    public typealias ChangeIdentifier = Int64
    /// Inherited from `HistoryInsert.transactionIdentifier`.
    ///
    /// The raw value of the transaction creation timestamp.
    nonisolated public var transactionIdentifier: TransactionIdentifier
    /// Inherited from `HistoryInsert.changeIdentifier`.
    ///
    /// The primary key in the `History` table. It is auto-incrementing for inline persistent history tracking.
    /// In SQL, this value identifies the row for a single operation.
    nonisolated public var changeIdentifier: ChangeIdentifier
    /// Inherited from `HistoryInsert.changedPersistentIdentifier`.
    ///
    /// The persistent identifier of the inserted model.
    nonisolated public var changedPersistentIdentifier: PersistentIdentifier
    
    nonisolated internal init(
        as type: Model.Type,
        transactionIdentifier: TransactionIdentifier,
        changeIdentifier: ChangeIdentifier,
        changedPersistentIdentifier: PersistentIdentifier
    ) {
        self.transactionIdentifier = transactionIdentifier
        self.changeIdentifier = changeIdentifier
        self.changedPersistentIdentifier = changedPersistentIdentifier
    }
}

public struct DatabaseHistoryUpdate<T>: HistoryUpdate
where T: PersistentModel & SendableMetatype {
    /// Inherited from `HistoryUpdate.Model`.
    public typealias Model = T
    /// Inherited from `HistoryUpdate.TransactionIdentifier`.
    public typealias TransactionIdentifier = Int64
    /// Inherited from `HistoryUpdate.ChangeIdentifier`.
    public typealias ChangeIdentifier = Int64
    /// Inherited from `HistoryUpdate.PropertyUpdate`.
    public typealias PropertyUpdate = any PartialKeyPath<Model> & Sendable
    /// Inherited from `HistoryUpdate.transactionIdentifier`.
    ///
    /// The raw value of the transaction creation timestamp.
    nonisolated public var transactionIdentifier: TransactionIdentifier
    /// Inherited from `HistoryUpdate.changeIdentifier`.
    ///
    /// The primary key in the `History` table. It is auto-incrementing for inline persistent history tracking.
    /// In SQL, this value identifies the row for a single operation.
    nonisolated public var changeIdentifier: ChangeIdentifier
    /// Inherited from `HistoryUpdate.changedPersistentIdentifier`.
    ///
    /// The persistent identifier of the updated model.
    nonisolated public var changedPersistentIdentifier: PersistentIdentifier
    /// Inherited from `HistoryUpdate.updatedAttributes`.
    ///
    /// The key paths for the properties that were updated.
    nonisolated public var updatedAttributes: [PropertyUpdate]
    
    nonisolated internal var fields: [String]
    
    nonisolated internal init(
        as type: Model.Type,
        transactionIdentifier: TransactionIdentifier,
        changeIdentifier: ChangeIdentifier,
        changedPersistentIdentifier: PersistentIdentifier,
        changedPropertyNames keys: [String]
    ) {
        self.transactionIdentifier = transactionIdentifier
        self.changeIdentifier = changeIdentifier
        self.changedPersistentIdentifier = changedPersistentIdentifier
        self.fields = keys
        self.updatedAttributes = keys.reduce(into: .init()) { partialResult, propertyName in
            switch T.schemaMetadata(for: propertyName) {
            case let property?:
                guard let keyPath: PartialKeyPath<T> & Sendable = sendable(cast: property.keyPath) else {
                    preconditionFailure("The property key path does not conform to Sendable: \(property)")
                }
                partialResult.append(keyPath)
            case nil:
                logger.warning("Unable to find a property that matches to the updated attribute name.", metadata: [
                    "key": "\(Schema.entityName(for: type)).\(propertyName)"
                ])
            }
        }
    }
}

public struct DatabaseHistoryDelete<T>: HistoryDelete
where T: PersistentModel & SendableMetatype {
    /// Inherited from `HistoryDelete.Model`.
    public typealias Model = T
    /// Inherited from `HistoryDelete.TransactionIdentifier`.
    public typealias TransactionIdentifier = Int64
    /// Inherited from `HistoryDelete.ChangeIdentifier`.
    public typealias ChangeIdentifier = Int64
    /// Inherited from `HistoryDelete.transactionIdentifier`.
    ///
    /// The raw value of the transaction creation timestamp.
    nonisolated public var transactionIdentifier: TransactionIdentifier
    /// Inherited from `HistoryDelete.changeIdentifier`.
    ///
    /// The primary key in the `History` table. It is auto-incrementing for inline persistent history tracking.
    /// In SQL, this value identifies the row for a single operation.
    nonisolated public var changeIdentifier: ChangeIdentifier
    /// Inherited from `HistoryDelete.changedPersistentIdentifier`.
    ///
    /// The persistent identifier of the deleted model.
    nonisolated public var changedPersistentIdentifier: PersistentIdentifier
    /// The preserved values mapped to the deleted model's key paths.
    nonisolated private var preservedValues: [PartialKeyPath<Model> & Sendable: any Sendable]
    
    nonisolated public subscript(keyPath: PartialKeyPath<Model> & Sendable) -> (any Sendable)? {
        preservedValues[keyPath]
    }
    
    nonisolated internal init(
        as type: Model.Type,
        transactionIdentifier: TransactionIdentifier,
        changeIdentifier: ChangeIdentifier,
        changedPersistentIdentifier: PersistentIdentifier,
        changedPropertyNames keys: [String],
        preservedValues: Data?
    ) {
        self.transactionIdentifier = transactionIdentifier
        self.changeIdentifier = changeIdentifier
        self.changedPersistentIdentifier = changedPersistentIdentifier
        guard let preservedValues else {
            self.preservedValues = [:]
            return
        }
        do {
            guard let values = try JSONSerialization.jsonObject(
                with: preservedValues,
                options: [.fragmentsAllowed]
            ) as? [Any] else {
                preconditionFailure("Preserved values must be a JSON array.")
            }
            guard keys.count == values.count else {
                preconditionFailure("Property names count (\(keys.count)) does not match preserved values count (\(values.count)).")
            }
            self.preservedValues = .init(uniqueKeysWithValues: zip(keys, values).compactMap { propertyName, rawValue in
                guard let property = type.schemaMetadata(for: propertyName) else {
                    logger.warning("Unable to assign preserved value to a non-existent property.", metadata: [
                        "key": "\(Schema.entityName(for: type)).\(propertyName)",
                        "value": "\(rawValue)"
                    ])
                    return nil
                }
                let valueType = unwrapOptionalMetatype(property.valueType)
                guard let valueType = valueType as? any DataStoreSnapshotValue.Type else {
                    preconditionFailure("The property value type does not conform to DataStoreSnapshotValue: \(property)")
                }
                guard let keyPath: PartialKeyPath<T> & Sendable = sendable(cast: property.keyPath) else {
                    preconditionFailure("The property key path does not conform to Sendable: \(property)")
                }
                if rawValue is NSNull {
                    return nil
                }
                let sqlType = SQLType(for: valueType)
                let bridgedValue: any Codable & Sendable
                if let string = rawValue as? String {
                    bridgedValue = string
                } else if let number = rawValue as? NSNumber {
                    if CFGetTypeID(number) == CFBooleanGetTypeID() {
                        bridgedValue = number.boolValue ? Int64(1) : Int64(0)
                    } else if sqlType == .real {
                        bridgedValue = number.doubleValue
                    } else {
                        bridgedValue = number.int64Value
                    }
                } else if let data = rawValue as? Data {
                    bridgedValue = data
                } else {
                    preconditionFailure("Unsupported preserved value type: \(T.self).\(propertyName) = \(rawValue) as \(Swift.type(of: rawValue))")
                }
                guard let value = SQLValue.convert(bridgedValue, as: valueType) else {
                    preconditionFailure("Unable to convert preserved value: \(T.self).\(propertyName) = \(rawValue) as \(Swift.type(of: rawValue)) to \(valueType)")
                }
                logger.debug("Tombstone value: \(T.self).\(propertyName) = \(value) as \(valueType)")
                return (keyPath, value)
            })
        } catch {
            fatalError("Failed to decode preserved values: \(error)")
        }
    }
    
    @available(*, unavailable, message: "")
    nonisolated fileprivate init(
        as type: Model.Type,
        transactionIdentifier: TransactionIdentifier,
        changeIdentifier: ChangeIdentifier,
        changedPersistentIdentifier: PersistentIdentifier,
        preservedValues: any DataStoreSnapshotValue
    ) {
        self.transactionIdentifier = transactionIdentifier
        self.changeIdentifier = changeIdentifier
        self.changedPersistentIdentifier = changedPersistentIdentifier
        guard let preservedValues = preservedValues as? String,
              let data = preservedValues.data(using: .utf8) else {
            preconditionFailure()
        }
        do {
            guard let object = try JSONSerialization.jsonObject(
                with: data,
                options: [.fragmentsAllowed]
            ) as? [String: Any] else {
                preconditionFailure()
            }
            let decoder = JSONDecoder()
            self.preservedValues = try .init(uniqueKeysWithValues: object.compactMap { column, rawValue in
                guard let property = type.schemaMetadata(for: column) else {
                    preconditionFailure()
                }
                let valueType = unwrapOptionalMetatype(property.valueType)
                guard let valueType = valueType as? any DataStoreSnapshotValue.Type else {
                    preconditionFailure()
                }
                guard let keyPath: PartialKeyPath<T> & Sendable = sendable(cast: property.keyPath) else {
                    preconditionFailure()
                }
                let fieldData = try JSONSerialization.data(
                    withJSONObject: rawValue,
                    options: [.fragmentsAllowed]
                )
                let value = try decoder.decode(valueType, from: fieldData)
                return (keyPath, value)
            })
        } catch {
            fatalError()
        }
    }
    
    /// Inherited from `HistoryDelete.tombstone`.
    @available(*, deprecated, message: "HistoryTombstone<Model> has no public initializer.")
    nonisolated public var tombstone: HistoryTombstone<Model> {
        let temporaryModel = Model(backingData: Model.createBackingData())
        for (keyPath, value) in preservedValues {
            guard let value: any DataStoreSnapshotValue = sendable(cast: value) else {
                preconditionFailure()
            }
            insert(value)
            func insert<Value: DataStoreSnapshotValue>(_ value: Value) {
                if let keyPath = keyPath as? ReferenceWritableKeyPath<Model, Value> {
                    temporaryModel.persistentBackingData.setValue(forKey: keyPath, to: value)
                    temporaryModel.setValue(forKey: keyPath, to: value)
                    temporaryModel[keyPath: keyPath] = value
                }
                if let keyPath = keyPath as? ReferenceWritableKeyPath<Model, Value?> {
                    temporaryModel.persistentBackingData.setValue(forKey: keyPath, to: value)
                    temporaryModel.setValue(forKey: keyPath, to: value)
                    temporaryModel[keyPath: keyPath] = value
                }
            }
        }
        guard let tombstone = [temporaryModel] as? HistoryTombstone<Model> else {
            fatalError("HistoryTombstone<\(Model.self)> cannot be instantiated.")
        }
        return tombstone
    }
}

#if DEBUG

nonisolated private let lastHistoryFetchLogTime: Mutex<TimeInterval> = .init(0)

nonisolated private func shouldLogHistoryFetch(
    limit: UInt64,
    hasPredicate: Bool,
    now: TimeInterval = Date().timeIntervalSinceReferenceDate,
    minInterval: TimeInterval = 10
) -> Bool {
    let isFullFetch = (limit <= 0) && !hasPredicate
    if isFullFetch { return true }
    return lastHistoryFetchLogTime.withLock { lastHistoryFetchLogTime in
        if now - lastHistoryFetchLogTime >= minInterval {
            lastHistoryFetchLogTime = now
            return true
        }
        return false
    }
}

#endif
