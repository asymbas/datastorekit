//
//  HistoryTable+SQL.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreRuntime
import DataStoreSQL
import Foundation
import Logging
import SQLiteHandle
import SQLSupport

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.transaction")

extension HistoryTable {
    nonisolated package static func counts(
        in storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) -> (rows: Int64, transactions: Int64) {
        let rowAlias = "row_count"
        let transactionAlias = "transaction_count"
        let sql = """
            SELECT
                COUNT(*) AS \(rowAlias),
                COUNT(DISTINCT \(Self.timestamp.rawValue)) AS \(transactionAlias)
            FROM \(Self.tableName)
            WHERE \(Self.storeIdentifier.rawValue) = ?
            """
        do {
            let result = try connection.query(sql, bindings: storeIdentifier).first
            let rows = result?[rowAlias] as? Int64 ?? 0
            let transactions = result?[transactionAlias] as? Int64 ?? 0
            return (rows, transactions)
        } catch {
            return (0, 0)
        }
    }
}

extension HistoryTable {
    nonisolated static func archiveDatabaseName(year: Int) -> String {
        "archive_\(year)"
    }

    nonisolated static func archiveYear(from archiveURL: URL) throws -> Int {
        let fileName = archiveURL.deletingPathExtension().lastPathComponent
        guard let yearText = fileName.split(separator: "-").last,
              let year = Int(yearText) else {
            throw SQLError(message: "Invalid history archive file name: \(fileName)")
        }
        return year
    }

    nonisolated static func archiveDatabaseName(for archiveURL: URL) throws -> String {
        try archiveDatabaseName(year: archiveYear(from: archiveURL))
    }
    
    private func withAttachedHistoryArchive<Result>(
        at archiveURL: URL,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        _ body: (String) throws -> Result
    ) throws -> Result {
        let archiveName = try HistoryTable.archiveDatabaseName(for: archiveURL)
        try connection.attachDatabase(at: archiveURL, as: archiveName)
        defer { try? connection.detachDatabaseIfAttached(named: archiveName) }
        return try body(archiveName)
    }
}

extension HistoryTable {
    nonisolated package static func maintainHistory(
        in storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        calendar: Calendar = .current,
        now: Date = .now,
        ttl: DateComponents = Self.defaultHistoryTTL(),
        archiveBatchSize: Int = 2_000,
        archiveMaxBatches: Int = 32,
        requireArchivedCopyBeforeDelete: Bool = true,
        deleteAfterArchiveAndTTL: Bool = true
    ) throws {
        try archiveTransactions(
            in: storeIdentifier,
            connection: connection,
            calendar: calendar,
            now: now,
            ttl: ttl,
            batchSize: archiveBatchSize,
            maxBatches: archiveMaxBatches
        )
        guard deleteAfterArchiveAndTTL else { return }
        try purgeExpiredTransactions(
            olderThan: ttl,
            in: storeIdentifier,
            connection: connection,
            calendar: calendar,
            now: now,
            requireArchivedCopyBeforeDelete: requireArchivedCopyBeforeDelete,
            archiveBeforeDelete: false
        )
    }
    
    nonisolated package static func maintainHistory(
        in storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        calendar: Calendar = .current,
        now: Date = .init(),
        ttlDays: Int,
        archiveBatchSize: Int = 2_000,
        archiveMaxBatches: Int = 32,
        requireArchivedCopyBeforeDelete: Bool = true,
        deleteAfterArchiveAndTTL: Bool = true
    ) throws {
        var ttl = DateComponents()
        ttl.day = ttlDays
        try maintainHistory(
            in: storeIdentifier,
            connection: connection,
            calendar: calendar,
            now: now,
            ttl: ttl,
            archiveBatchSize: archiveBatchSize,
            archiveMaxBatches: archiveMaxBatches,
            requireArchivedCopyBeforeDelete: requireArchivedCopyBeforeDelete,
            deleteAfterArchiveAndTTL: deleteAfterArchiveAndTTL
        )
    }
    
    nonisolated package static func purgeExpiredTransactions(
        olderThan components: DateComponents = Self.defaultHistoryTTL(),
        in storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        calendar: Calendar = .current,
        now: Date = .init(),
        requireArchivedCopyBeforeDelete: Bool = true,
        archiveBeforeDelete: Bool = true,
        archiveBatchSize: Int = 2_000,
        archiveMaxBatches: Int = 32
    ) throws {
        let expirationDate = try Self.expirationDate(now: now, subtracting: components, calendar: calendar)
        let cutoffTimestamp = Int64(expirationDate.timeIntervalSince1970 * 1_000_000)
        if archiveBeforeDelete {
            try archiveTransactions(
                in: storeIdentifier,
                connection: connection,
                calendar: calendar,
                now: now,
                ttl: components,
                batchSize: archiveBatchSize,
                maxBatches: archiveMaxBatches
            )
        }
        if requireArchivedCopyBeforeDelete {
            try purgeArchivedExpiredTransactions(
                cutoffTimestamp: cutoffTimestamp,
                in: storeIdentifier,
                connection: connection,
                calendar: calendar
            )
        } else {
            try deleteExpiredTransactions(
                cutoffTimestamp: cutoffTimestamp,
                in: storeIdentifier,
                connection: connection
            )
        }
        logger.debug("Purged history older than \(Self.describe(components)) for store: \(storeIdentifier)")
    }
    
    nonisolated package static func purgeExpiredTransactions(
        olderThan days: Int = 30,
        in storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        calendar: Calendar = .current,
        now: Date = .now,
        requireArchivedCopyBeforeDelete: Bool = true,
        archiveBeforeDelete: Bool = true,
        archiveBatchSize: Int = 2_000,
        archiveMaxBatches: Int = 32
    ) throws {
        var ttl = DateComponents()
        ttl.day = days
        try purgeExpiredTransactions(
            olderThan: ttl,
            in: storeIdentifier,
            connection: connection,
            calendar: calendar,
            now: now,
            requireArchivedCopyBeforeDelete: requireArchivedCopyBeforeDelete,
            archiveBeforeDelete: archiveBeforeDelete,
            archiveBatchSize: archiveBatchSize,
            archiveMaxBatches: archiveMaxBatches
        )
    }
    
    nonisolated private static func deleteExpiredTransactions(
        cutoffTimestamp: Int64,
        in storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        try PreparedStatement(
            sql: """
            DELETE FROM \(Self.tableName)
            WHERE \(Self.storeIdentifier.rawValue) = ?
            AND \(Self.timestamp.rawValue) < ?
            """,
            bindings: [storeIdentifier, cutoffTimestamp],
            handle: connection.handle
        ).run()
    }
    
    nonisolated private static func purgeArchivedExpiredTransactions(
        cutoffTimestamp: Int64,
        in storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        calendar: Calendar
    ) throws {
        let yearExpression = "CAST(strftime('%Y', \(Self.timestamp.rawValue) / 1000000, 'unixepoch') AS INTEGER)"
        let years = try connection.query(
            """
            SELECT DISTINCT \(yearExpression) AS \(ArchiveTable.year.rawValue)
            FROM \(Self.tableName)
            WHERE \(Self.storeIdentifier.rawValue) = ?
            AND \(Self.timestamp.rawValue) < ?
            ORDER BY \(ArchiveTable.year.rawValue) ASC
            """,
            bindings: [storeIdentifier, cutoffTimestamp]
        ).compactMap { row -> Int? in
            if let value = row[ArchiveTable.year.rawValue] as? Int64 { return Int(value) }
            if let value = row[ArchiveTable.year.rawValue] as? Int { return value }
            if let value = row[ArchiveTable.year.rawValue] as? String { return Int(value) }
            return nil
        }
        guard years.isEmpty == false else { return }
        let timestampKey = Self.timestamp.rawValue
        let pkKey = Self.pk.rawValue
        let storeIdentifierKey = Self.storeIdentifier.rawValue
        for year in years {
            guard let cursor = try readArchiveCursor(
                year: year,
                storeIdentifier: storeIdentifier,
                connection: connection
            ) else {
                continue
            }
            guard let yearStartDate = calendar.date(from: DateComponents(year: year, month: 1, day: 1)),
                  let yearEndDate = calendar.date(from: DateComponents(year: year + 1, month: 1, day: 1)) else {
                continue
            }
            let yearStartTimestamp = Int64(yearStartDate.timeIntervalSince1970 * 1_000_000)
            let yearEndTimestamp = Int64(yearEndDate.timeIntervalSince1970 * 1_000_000)
            if cursor.isComplete, cutoffTimestamp >= yearEndTimestamp {
                try PreparedStatement(
                    sql: """
                    DELETE FROM \(Self.tableName)
                    WHERE \(storeIdentifierKey) = ?
                    AND \(timestampKey) >= ?
                    AND \(timestampKey) < ?
                    """,
                    bindings: [
                        SQLValue.text(storeIdentifier),
                        SQLValue.integer(yearStartTimestamp),
                        SQLValue.integer(yearEndTimestamp)
                    ],
                    handle: connection.handle
                ).run()
                continue
            }
            try PreparedStatement(
                sql: """
                DELETE FROM \(Self.tableName)
                WHERE \(storeIdentifierKey) = ?
                AND \(timestampKey) >= ?
                AND \(timestampKey) < ?
                AND (
                    \(timestampKey) < ?
                    OR (\(timestampKey) = ? AND \(pkKey) <= ?)
                )
                """,
                bindings: [
                    SQLValue.text(storeIdentifier),
                    SQLValue.integer(yearStartTimestamp),
                    SQLValue.integer(cutoffTimestamp),
                    SQLValue.integer(cursor.lastTimestamp),
                    SQLValue.integer(cursor.lastTimestamp),
                    SQLValue.integer(cursor.lastChangeIdentifier)
                ],
                handle: connection.handle
            ).run()
        }
    }
    
    nonisolated internal static func defaultHistoryTTL() -> DateComponents {
        var ttl = DateComponents(); ttl.day = 30
        return ttl
    }
    
    nonisolated private static func expirationDate(
        now: Date,
        subtracting ttl: DateComponents,
        calendar: Calendar
    ) throws -> Date {
        var delta = DateComponents()
        if let year = ttl.year { delta.year = -year }
        if let month = ttl.month { delta.month = -month }
        if let weekOfYear = ttl.weekOfYear { delta.weekOfYear = -weekOfYear }
        if let day = ttl.day { delta.day = -day }
        if let hour = ttl.hour { delta.hour = -hour }
        if let minute = ttl.minute { delta.minute = -minute }
        if let second = ttl.second { delta.second = -second }
        guard let date = calendar.date(byAdding: delta, to: now) else {
            throw PurgeError.invalidTTL
        }
        return date
    }
    
    nonisolated private static func describe(_ components: DateComponents) -> String {
        var parts = [String]()
        if let year = components.year { parts.append("\(year)y") }
        if let month = components.month { parts.append("\(month)mo") }
        if let week = components.weekOfYear { parts.append("\(week)w") }
        if let day = components.day { parts.append("\(day)d") }
        if let hour = components.hour { parts.append("\(hour)h") }
        if let minute = components.minute { parts.append("\(minute)m") }
        if let second = components.second { parts.append("\(second)s") }
        return parts.isEmpty ? "0" : parts.joined(separator: " ")
    }
}

private struct HistoryArchiveCursor: Sendable {
    nonisolated var isComplete: Bool
    nonisolated var cursorTimestamp: Int64
    nonisolated var cursorIdentifier: Int64
    nonisolated var lastTimestamp: Int64 { cursorTimestamp }
    nonisolated var lastChangeIdentifier: Int64 { cursorIdentifier }
}

extension HistoryTable {
    nonisolated package static func archiveTransactions(
        in storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        calendar: Calendar = .current,
        now: Date = .init(),
        ttl: DateComponents = Self.defaultHistoryTTL(),
        batchSize: Int = 2_000,
        maxBatches: Int = 32
    ) throws {
        guard let mainURL = try connection.mainDatabaseURL() else { return }
        let nowTimestamp = Int64(now.timeIntervalSince1970 * 1_000_000)
        let cutoffDate = try Self.expirationDate(now: now, subtracting: ttl, calendar: calendar)
        let cutoffTimestamp = Int64(cutoffDate.timeIntervalSince1970 * 1_000_000)
        let yearExpression = "CAST(strftime('%Y', \(Self.timestamp.rawValue) / 1000000, 'unixepoch') AS INTEGER)"
        let years = try connection.query(
            """
            SELECT DISTINCT \(yearExpression) AS \(ArchiveTable.year.rawValue)
            FROM main.\(Self.tableName)
            WHERE \(Self.storeIdentifier.rawValue) = ?
            AND \(Self.timestamp.rawValue) < ?
            ORDER BY \(ArchiveTable.year.rawValue) ASC
            """,
            bindings: [storeIdentifier, cutoffTimestamp]
        ).compactMap { row -> Int? in
            if let value = row[ArchiveTable.year.rawValue] as? Int64 { return Int(value) }
            if let value = row[ArchiveTable.year.rawValue] as? Int { return value }
            if let value = row[ArchiveTable.year.rawValue] as? String { return Int(value) }
            return nil
        }
        guard years.isEmpty == false else { return }
        var remainingBatches = maxBatches
        for year in years {
            guard remainingBatches > 0 else { return }
            try archiveYear(
                year,
                mainURL: mainURL,
                storeIdentifier: storeIdentifier,
                connection: connection,
                calendar: calendar,
                nowTimestamp: nowTimestamp,
                cutoffTimestamp: cutoffTimestamp,
                batchSize: batchSize,
                remainingBatches: &remainingBatches
            )
        }
    }
    
    nonisolated package static func archiveCutoffDate(
        for year: Int,
        in storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws -> Date? {
        guard let cursor = try readArchiveCursor(
            year: year,
            storeIdentifier: storeIdentifier,
            connection: connection
        ) else { return nil }
        return Date(timeIntervalSince1970: Double(cursor.lastTimestamp) / 1_000_000)
    }
    
    nonisolated private static func archiveYear(
        _ year: Int,
        mainURL: URL,
        storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        calendar: Calendar,
        nowTimestamp: Int64,
        cutoffTimestamp: Int64,
        batchSize: Int,
        remainingBatches: inout Int
    ) throws {
        guard let yearStartDate = calendar.date(from: DateComponents(year: year, month: 1, day: 1)),
              let yearEndDate = calendar.date(from: DateComponents(year: year + 1, month: 1, day: 1)) else {
            return
        }
        let yearStartTimestamp = Int64(yearStartDate.timeIntervalSince1970 * 1_000_000)
        let yearEndTimestamp = Int64(yearEndDate.timeIntervalSince1970 * 1_000_000)
        let upperBoundTimestamp = min(yearEndTimestamp, cutoffTimestamp)
        guard upperBoundTimestamp > yearStartTimestamp else { return }
        if let cursor = try readArchiveCursor(year: year, storeIdentifier: storeIdentifier, connection: connection),
           cursor.isComplete,
           nowTimestamp >= yearEndTimestamp {
            return
        }
        let archiveName = "archive"
        let archiveURL = makeYearlyArchiveDatabaseURL(year: year, mainURL: mainURL)
        logger.trace("Archiving history year \(year): \(archiveURL.path)")
        try FileManager.default.createDirectory(
            at: archiveURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try connection.attachDatabase(at: archiveURL, as: archiveName)
        defer { try? connection.detachDatabaseIfAttached(named: archiveName) }
        try ensureArchiveHistoryTable(connection: connection, archiveName: archiveName)
        var cursor = try readArchiveCursor(
            year: year,
            storeIdentifier: storeIdentifier,
            connection: connection
        ) ?? .init(
            isComplete: false,
            cursorTimestamp: yearStartTimestamp - 1,
            cursorIdentifier: 0
        )
        while remainingBatches > 0 {
            if Task.isCancelled {
                connection.interrupt()
                throw CancellationError()
            }
            try copyArchiveBatch(
                year: year,
                storeIdentifier: storeIdentifier,
                archiveName: archiveName,
                connection: connection,
                yearStartTimestamp: yearStartTimestamp,
                upperBoundTimestamp: upperBoundTimestamp,
                batchSize: batchSize,
                cursor: &cursor,
                nowTimestamp: nowTimestamp,
                yearEndTimestamp: yearEndTimestamp
            )
            remainingBatches -= 1
            if cursor.isComplete { return }
        }
    }
    
    nonisolated private static func copyArchiveBatch(
        year: Int,
        storeIdentifier: String,
        archiveName: String,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        yearStartTimestamp: Int64,
        upperBoundTimestamp: Int64,
        batchSize: Int,
        cursor: inout HistoryArchiveCursor,
        nowTimestamp: Int64,
        yearEndTimestamp: Int64
    ) throws {
        let storeIdentifierKey = Self.storeIdentifier.rawValue
        let timestampKey = Self.timestamp.rawValue
        let pkKey = Self.pk.rawValue
        let insertSQL = """
            INSERT OR IGNORE INTO \(archiveName).\(Self.tableName) (
                \(pkKey),
                \(Self.event.rawValue),
                \(timestampKey),
                \(storeIdentifierKey),
                \(Self.author.rawValue),
                \(Self.entityName.rawValue),
                \(Self.entityPrimaryKey.rawValue),
                \(Self.propertyNames.rawValue),
                \(Self.preservedValues.rawValue)
            )
            SELECT
                \(pkKey),
                \(Self.event.rawValue),
                \(timestampKey),
                \(storeIdentifierKey),
                \(Self.author.rawValue),
                \(Self.entityName.rawValue),
                \(Self.entityPrimaryKey.rawValue),
                \(Self.propertyNames.rawValue),
                \(Self.preservedValues.rawValue)
            FROM main.\(Self.tableName)
            WHERE \(storeIdentifierKey) = ?
            AND \(timestampKey) >= ?
            AND \(timestampKey) < ?
            AND (
                \(timestampKey) > ?
                OR (\(timestampKey) = ? AND \(pkKey) > ?)
            )
            ORDER BY \(timestampKey) ASC, \(pkKey) ASC
            LIMIT ?
            """
        try PreparedStatement(
            sql: insertSQL,
            bindings: [
                SQLValue.text(storeIdentifier),
                SQLValue.integer(yearStartTimestamp),
                SQLValue.integer(upperBoundTimestamp),
                SQLValue.integer(cursor.lastTimestamp),
                SQLValue.integer(cursor.lastTimestamp),
                SQLValue.integer(cursor.lastChangeIdentifier),
                SQLValue.integer(Int64(batchSize)),
            ],
            handle: connection.handle
        ).run()
        let lastSelectedRowSQL = """
            SELECT ts, pk FROM (
                SELECT \(timestampKey) AS ts, \(pkKey) AS pk
                FROM main.\(Self.tableName)
                WHERE \(storeIdentifierKey) = ?
                AND \(timestampKey) >= ?
                AND \(timestampKey) < ?
                AND (
                    \(timestampKey) > ?
                    OR (\(timestampKey) = ? AND \(pkKey) > ?)
                )
                ORDER BY \(timestampKey) ASC, \(pkKey) ASC
                LIMIT ?
            )
            ORDER BY ts DESC, pk DESC
            LIMIT 1
            """
        let last = try connection.query(
            lastSelectedRowSQL,
            bindings: [
                storeIdentifier,
                yearStartTimestamp,
                upperBoundTimestamp,
                cursor.lastTimestamp,
                cursor.lastTimestamp,
                cursor.lastChangeIdentifier,
                batchSize,
            ]
        ).first
        guard let lastTimestamp = last?["ts"] as? Int64,
              let lastChangeIdentifier = last?["pk"] as? Int64 else {
            let isFinalized = nowTimestamp >= yearEndTimestamp
            if isFinalized {
                cursor.isComplete = true
                try writeArchiveCursor(
                    year: year,
                    storeIdentifier: storeIdentifier,
                    cursor: cursor,
                    connection: connection,
                    nowTimestamp: nowTimestamp
                )
            }
            return
        }
        cursor.cursorTimestamp = lastTimestamp
        cursor.cursorIdentifier = lastChangeIdentifier
        let remainingSQL = """
            SELECT 1
            FROM main.\(Self.tableName)
            WHERE \(storeIdentifierKey) = ?
            AND \(timestampKey) >= ?
            AND \(timestampKey) < ?
            AND (
                \(timestampKey) > ?
                OR (\(timestampKey) = ? AND \(pkKey) > ?)
            )
            LIMIT 1
            """
        let hasRemaining = (try connection.query(
            remainingSQL,
            bindings: [
                storeIdentifier,
                yearStartTimestamp,
                upperBoundTimestamp,
                cursor.lastTimestamp,
                cursor.lastTimestamp,
                cursor.lastChangeIdentifier,
            ]
        ).first) != nil
        let isFinalized = nowTimestamp >= yearEndTimestamp
        cursor.isComplete = (hasRemaining == false) && isFinalized
        try writeArchiveCursor(
            year: year,
            storeIdentifier: storeIdentifier,
            cursor: cursor,
            connection: connection,
            nowTimestamp: nowTimestamp
        )
    }
    
    nonisolated internal static func archiveDirectoryURL(mainURL: URL) -> URL {
        let directoryURL = mainURL.deletingLastPathComponent()
        return directoryURL.appendingPathComponent("Transactions", isDirectory: true)
    }
    
    nonisolated internal static func makeYearlyArchiveDatabaseURL(year: Int, mainURL: URL) -> URL {
        let archiveDirectoryURL = Self.archiveDirectoryURL(mainURL: mainURL)
        let baseName = mainURL.deletingPathExtension().lastPathComponent
        let component = "\(baseName)-Transactions-\(year).archive"
        return archiveDirectoryURL.appendingPathComponent(component)
    }
    
    nonisolated private static func ensureArchiveStateTable(
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws {
        try PreparedStatement(sql: ArchiveTable.createTable, handle: connection.handle).run()
    }
    
    nonisolated private static func ensureArchiveHistoryTable(
        connection: borrowing DatabaseConnection<DatabaseStore>,
        archiveName: String
    ) throws {
        try PreparedStatement(
            sql: Self.createTable(databaseName: archiveName, autoIncrement: false),
            handle: connection.handle
        ).run()
    }
    
    nonisolated private static func readArchiveCursor(
        year: Int,
        storeIdentifier: String,
        connection: borrowing DatabaseConnection<DatabaseStore>
    ) throws -> HistoryArchiveCursor? {
        let rows: [[String: Any]]
        do {
            rows = try connection.query(
             """
             SELECT
                 \(ArchiveTable.isComplete.rawValue),
                 \(ArchiveTable.cursorTimestamp.rawValue),
                 \(ArchiveTable.cursorIdentifier.rawValue)
             FROM \(ArchiveTable.tableName)
             WHERE \(ArchiveTable.storeIdentifier.rawValue) = ?
             AND \(ArchiveTable.year.rawValue) = ?
             LIMIT 1
             """,
             bindings: [storeIdentifier, year]
            )
        } catch {
            return nil
        }
        guard let row = rows.first,
              let lastTimestamp = row[ArchiveTable.cursorTimestamp.rawValue] as? Int64,
              let lastChangeIdentifier = row[ArchiveTable.cursorIdentifier.rawValue] as? Int64 else {
            return nil
        }
        let complete = (row[ArchiveTable.isComplete.rawValue] as? Int64 ?? 0) != 0
        return .init(
            isComplete: complete,
            cursorTimestamp: lastTimestamp,
            cursorIdentifier: lastChangeIdentifier
        )
    }
    
    nonisolated private static func writeArchiveCursor(
        year: Int,
        storeIdentifier: String,
        cursor: HistoryArchiveCursor,
        connection: borrowing DatabaseConnection<DatabaseStore>,
        nowTimestamp: Int64
    ) throws {
        try ensureArchiveStateTable(connection: connection)
        try PreparedStatement(
            sql: """
            INSERT INTO \(ArchiveTable.tableName) (
                \(ArchiveTable.isComplete.rawValue),
                \(ArchiveTable.checkpointTimestamp.rawValue),
                \(ArchiveTable.cursorTimestamp.rawValue),
                \(ArchiveTable.cursorIdentifier.rawValue),
                \(ArchiveTable.year.rawValue),
                \(ArchiveTable.storeIdentifier.rawValue)
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (
                \(ArchiveTable.storeIdentifier.rawValue),
                \(ArchiveTable.year.rawValue)
            ) DO UPDATE SET
            \(ArchiveTable.isComplete.rawValue) = excluded.\(ArchiveTable.isComplete.rawValue),
            \(ArchiveTable.checkpointTimestamp.rawValue) = excluded.\(ArchiveTable.checkpointTimestamp.rawValue),
            \(ArchiveTable.cursorTimestamp.rawValue) = excluded.\(ArchiveTable.cursorTimestamp.rawValue),
            \(ArchiveTable.cursorIdentifier.rawValue) = excluded.\(ArchiveTable.cursorIdentifier.rawValue)
            """,
            bindings: [
                .integer(cursor.isComplete ? 1 : 0),
                .integer(nowTimestamp),
                .integer(cursor.lastTimestamp),
                .integer(cursor.lastChangeIdentifier),
                .integer(Int64(year)),
                .text(storeIdentifier)
            ] as [SQLValue],
            handle: connection.handle
        ).run()
        logger.trace(
            "Archive cursor updated.",
            metadata: [
                "store": "\(storeIdentifier)",
                "year": "\(year)",
                "is_complete": "\(cursor.isComplete)",
                "cursor_timestamp": "\(cursor.lastTimestamp)",
                "cursor_identifier": "\(cursor.lastChangeIdentifier)"
            ]
        )
    }
    
    private enum PurgeError: Error {
        case invalidTTL
    }
}
