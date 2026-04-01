//
//  SQLStatementShortcut+SQLite.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import DataStoreSupport
import Foundation
import Logging
import SQLite3
import SQLiteStatement
import SQLSupport

nonisolated private let logger: Logger = .init(label: "com.asymbas.sqlite")

extension SQLStatementShortcut where Handle == SQLite {
    nonisolated public consuming func fetchRow(
        for primaryKey: some Sendable,
        at column: String = "pk",
        in table: String
    ) throws -> [String: any Sendable]? {
        var result: [String: any Sendable]?
        let statement = try PreparedStatement(
            sql: """
            SELECT * FROM "\(table)"
            WHERE "\(column)" = ?
            LIMIT 1
            """,
            bindings: [primaryKey],
            handle: handle
        )
        for row in statement.rows {
            if result == nil { result = [:] }
            for column in row.columns {
                result?[column.name] = column.value
            }
        }
        try statement.finalize()
        return result
    }
    
    nonisolated internal func fetchExistingRow(
        in table: String,
        columns: [String],
        values: [any Sendable],
        uniquenessConstraint: [String]
    ) throws -> [String: any Sendable]? {
        let uniquenessConstraintSQL = uniquenessConstraint
            .map { "\(quote($0)) = ?" }
            .joined(separator: " \nAND ")
        let sql = """
            SELECT \(columns.map(quote).joined(separator: ", "))
            FROM \(table)
            WHERE \(uniquenessConstraintSQL)
            LIMIT 1
            """
        let bindings = uniquenessConstraint.map { values[columns.firstIndex(of: $0)!] }
        var result: [String: any Sendable]?
        let statement = try PreparedStatement(sql: sql, bindings: bindings, handle: handle)
        for row in statement.rows {
            if result == nil { result = [:] }
            for column in row.columns {
                result?[column.name] = column.value
            }
            break
        }
        try statement.finalize()
        return result
    }
    
    nonisolated private func fetchSingleRow(
        from table: String,
        columns: [String],
        where predicate: String?,
        bindings: [any Sendable]
    ) throws -> (columns: [String], values: [any Sendable])? {
        let sql: String
        if let predicate {
            sql = """
            SELECT \(columns.map(quote).joined(separator: ", "))
            FROM "\(table)"
            WHERE \(predicate)
            LIMIT 2
            """
        } else {
            sql = """
            SELECT \(columns.map(quote).joined(separator: ", "))
            FROM "\(table)"
            LIMIT 2
            """
        }
        var first: [any Sendable]?
        var seen = 0
        try handle.withPreparedStatement(sql, bindings: bindings) { statement in
            for row in statement.rows {
                seen += 1
                if seen == 1 {
                    var values: [any Sendable] = []
                    for column in row.columns {
                        values.append(column.value)
                    }
                    first = values
                } else {
                    throw SQLError(
                        "Mutation prefetch matched multiple rows.",
                        sql: sql,
                        bindings: bindings
                    )
                }
            }
        }
        guard let first else {
            return nil
        }
        return (columns, first)
    }
    
    nonisolated public func fetchByUniqueness(
        from table: String,
        columns: [String],
        values: [any Sendable],
        onConflict uniquenessConstraint: [String]
    ) throws -> [any Sendable]? {
        let quotedColumns = columns.map(quote).joined(separator: ", ")
        var predicatePlaceholders = [String]()
        var predicateBindings = [any Sendable]()
        for uniqueColumn in uniquenessConstraint {
            guard let index = columns.firstIndex(of: uniqueColumn) else {
                throw SQLError(.columnNotFound(uniqueColumn), bindings: values)
            }
            let value = values[index]
            if value is SQLNull || value is NSNull {
                return nil
            } else {
                predicatePlaceholders.append("\(quote(uniqueColumn)) = ?")
                predicateBindings.append(value)
            }
        }
        let whereClause = predicatePlaceholders.joined(separator: " AND ")
        let sql = """
            SELECT \(quotedColumns) FROM "\(table)"
            WHERE \(whereClause)
            LIMIT 2
            """
        var first: [any Sendable]?
        var seen = 0
        try handle.withPreparedStatement(sql, bindings: predicateBindings) { statement in
            for row in statement.rows {
                seen += 1
                if seen == 1 {
                    var values: [any Sendable] = []
                    for column in row.columns {
                        values.append(column.value)
                    }
                    first = values
                } else {
                    throw SQLError(
                        "Uniqueness violation: Multiple rows match (\(uniquenessConstraint.joined(separator: ", ")))",
                        sql: sql,
                        bindings: predicateBindings
                    )
                }
            }
        }
        return first
    }
    
    nonisolated public consuming func primaryKeys<PrimaryKey>(
        for table: String? = nil,
        at column: String = "pk",
        as type: PrimaryKey.Type = String.self
    ) throws -> [PrimaryKey] where PrimaryKey: LosslessStringConvertible {
        var primaryKeys = [PrimaryKey]()
        let tables: [String]
        if let table {
            tables = [table]
        } else {
            tables = try handle.query(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            ).compactMap {
                $0["name"] as? String
            }
        }
        for table in tables {
            let results = try handle.query(
                """
                SELECT "\(column)" FROM "\(table)"
                WHERE "\(column)" IS NOT NULL
                """
            )
            for row in results {
                if let value = row[column] as? PrimaryKey {
                    primaryKeys.append(value)
                }
            }
        }
        return primaryKeys
    }
    
    nonisolated public consuming func primaryKeys<PrimaryKey>(
        from table: String,
        as alias: String? = nil,
        at column: String = "pk",
        where clause: String? = nil,
        bindings: [any Sendable] = []
    ) throws -> [PrimaryKey] where PrimaryKey: LosslessStringConvertible {
        let column = quote(column)
        let table = "\(quote(table))\(alias == nil ? "" : " AS \(quote(alias!))")"
        var sql = "SELECT \(column) FROM \(table)"
        if let clause {
            sql += " WHERE \(clause)"
        }
        return try handle.query(sql, bindings: bindings).compactMap { row in
            if let value = row[column] {
                return value as? PrimaryKey
            } else {
                return nil
            }
        }
    }
    
    nonisolated public consuming func primaryKey<PrimaryKey>(
        from table: String,
        as alias: String? = nil,
        at column: String = "pk",
        where clause: String? = nil,
        bindings: [any Sendable] = []
    ) throws -> PrimaryKey? where PrimaryKey: LosslessStringConvertible {
        let result: [PrimaryKey] = try primaryKeys(from: table, as: alias, at: column, where: clause, bindings: bindings)
        guard result.count == 1 else {
            throw SQLError(message: "Result set for primary key query is ambiguous: \(result)")
        }
        return result[0]
    }
    
    nonisolated public consuming func tableExists(for table: String) throws -> Bool {
        let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = ?"
        return !(try handle.query(sql, bindings: [table as String])).isEmpty
    }
    
    nonisolated public consuming func rowExists<PrimaryKey>(
        for primaryKey: PrimaryKey,
        in table: String,
        column: String = "pk"
    ) throws -> Bool where PrimaryKey: LosslessStringConvertible {
        return !(try handle.query(
            """
            SELECT 1 FROM \(quote(table))
            WHERE \(quote(column)) = ? LIMIT 1
            """,
            bindings: [String(describing: primaryKey)]
        )).isEmpty
    }
    
    nonisolated public func count(
        _ sql: String,
        bindings: [any Sendable] = []
    ) throws -> Int {
        try handle.withPreparedStatement(sql, bindings: bindings) { statementPointer in
            var result: Int = 0
            let resultCode = try statementPointer.step
            if resultCode == .row {
                result = Int(sqlite3_column_int64(statementPointer.pointer, 0))
            } else if resultCode != .done {
                let handle = sqlite3_db_handle(statementPointer.pointer)
                throw SQLError(
                    SQLite.Error(sqlite: sqlite3_errcode(handle)),
                    SQLite.Error.extended(sqlite: sqlite3_extended_errcode(handle)),
                    sql: sql,
                    bindings: bindings
                )
            }
            return result
        }
    }
    
    nonisolated public func count(
        from table: String,
        where clause: String? = nil,
        bindings: [any Sendable] = []
    ) throws -> Int {
        try count(
            clause == nil
            ? "SELECT COUNT(*) FROM \(quote(table))"
            : "SELECT COUNT(*) FROM \(quote(table)) WHERE \(clause.unsafelyUnwrapped)",
            bindings: bindings
        )
    }
    
    nonisolated package func count(@SQLBuilder statement: () -> [any SQLFragment]) throws -> Int {
        try count(SQL(statement()))
    }
    
    nonisolated package func count(_ statement: SQL) throws -> Int {
        try count(statement.sql, bindings: statement.bindings)
    }
    
    @discardableResult nonisolated public consuming func upsert(
        into table: String,
        columns: [String],
        values: [any Sendable],
        onConflict uniquenessConstraints: consuming [[String]],
        columnsToIgnore: [String] = ["pk"],
        shouldUpdate: Bool = true,
        preferSpecificConstraint: Bool = true
    ) throws -> (
        changes: Int32,
        isInsertOperation: Bool,
        rowID: Int64,
        returningResultColumns: [String: String]
    ) {
        let table = quote(table)
        let placeholders = Array(repeating: "?", count: columns.count).joined(separator: ", ")
        let eligibleKeys = columns.filter { !columnsToIgnore.contains($0) }
        let overwriteAssignments = eligibleKeys
            .map { "\(quote($0)) = excluded.\(quote($0))" }
            .joined(separator: ", ")
        let hasValidChangesClause = eligibleKeys.isEmpty ? nil : eligibleKeys
            .map { "NOT (\(quote($0)) IS excluded.\(quote($0)))" }
            .joined(separator: "\nOR ")
        if preferSpecificConstraint {
            let rowKeys = Set(columns)
            uniquenessConstraints = uniquenessConstraints
                .filter { Set($0).isSubset(of: rowKeys) }
                .sorted { $0.count > $1.count }
        }
        var lastError: (any Swift.Error)?
        let columnsSQL = columns.map(quote).joined(separator: ", ")
        for uniquenessConstraint in uniquenessConstraints.isEmpty ? [[]] : uniquenessConstraints {
            let statement = SQL {
                "INSERT INTO \(table) (\(columnsSQL))"
                "VALUES (\(placeholders))"
                if !uniquenessConstraint.isEmpty {
                    "ON CONFLICT (\(uniquenessConstraint.map(quote).joined(separator: ", ")))"
                    if shouldUpdate && !overwriteAssignments.isEmpty {
                        "DO UPDATE SET \(overwriteAssignments)"
                        if let clause = hasValidChangesClause { "WHERE \(clause)" }
                    } else {
                        "DO NOTHING"
                    }
                }
                let returningClause = ([
                    "(rowid = last_insert_rowid()) AS isInsert",
                    "rowid"
                ] + columnsToIgnore.map(quote)).joined(separator: ", ")
                "RETURNING \(returningClause)"
            }
            do {
                let sql = statement.sql
                let result = try handle.withPreparedStatement(sql, bindings: values) {
                    switch try $0.step {
                    case .row:
                        do {
                            let result = try extractReturningRow($0.pointer, columnOffset: 1 /*2*/)
                            return (
                                changes: handle.rowChanges,
                                isInsertOperation: sqlite3_column_int($0.pointer, 0) != 0,
                                rowID: result.rowID,
                                returningResultColumns: result.returningResultColumns
                            )
                        } catch {
                            guard !uniquenessConstraint.isEmpty else {
                                throw error
                            }
                            fallthrough
                        }
                    case .done where !uniquenessConstraint.isEmpty:
                        let uniquenessConstraintSQL = uniquenessConstraint
                            .map { "\(quote($0)) = ?" }
                            .joined(separator: " \nAND ")
                        let sql = """
                            SELECT \((["rowid"] + columnsToIgnore.map(quote)).joined(separator: ", \n"))
                            FROM \(table)
                            WHERE \(uniquenessConstraintSQL)
                            LIMIT 1
                            """
                        let bindings = uniquenessConstraint.map { values[columns.firstIndex(of: $0)!] }
                        return try handle.withPreparedStatement(sql, bindings: bindings) {
                            guard try $0.step == .row else {
                                throw SQLError(
                                    message: "Fallback lookup failed: \(handle.error)",
                                    sql: sql,
                                    bindings: bindings
                                )
                            }
                            let result = try extractReturningRow($0.pointer, columnOffset: 0)
                            return (
                                changes: 0,
                                isInsertOperation: false,
                                rowID: result.rowID,
                                returningResultColumns: result.returningResultColumns
                            )
                        }
                    default:
                        let error = self.handle.error
                        throw SQLError(
                            message: "Unexpected result from `sqlite3_step(_:)`: \(error)",
                            sql: sql,
                            bindings: values
                        )
                    }
                }
                return result
            } catch {
                lastError = error
                logger.debug("Error upserting into \(table): \(error.localizedDescription)")
                continue
            }
        }
        throw lastError ?? SQLError(message: "All upsert attempts have failed: \(values)")
        func extractReturningRow(
            _ statementPointer: OpaquePointer?,
            columnOffset: Int = 0
        ) throws -> (
            rowID: Int64,
            returningResultColumns: [String: String]
        ) {
            let rowID = sqlite3_column_int64(statementPointer, 0 + Int32(columnOffset))
            var returningResultColumns = [String: String]()
            for (index, key) in columnsToIgnore.enumerated() {
                let columnIndex = Int32(index + 1 + columnOffset)
                guard let textPointer = sqlite3_column_text(statementPointer, columnIndex) else {
                    let sql = String(cString: sqlite3_sql(statementPointer))
                    let message = "Returned PRIMARY KEY was NULL: \(key)"
                    throw SQLError(message: message, sql: sql, bindings: values)
                }
                returningResultColumns[key] = String(cString: textPointer)
            }
            return (rowID, returningResultColumns)
        }
        func previousPrimaryKeyValue(
            from row: [String: any Sendable]?,
            columnsToIgnore: [String]
        ) -> String? {
            guard let row else { return nil }
            let key: String
            if columnsToIgnore.contains(pk) {
                key = pk
            } else if let first = columnsToIgnore.first {
                key = first
            } else {
                return nil
            }
            guard let value = row[key], !(value is SQLNull) else { return nil }
            if let string = value as? String { return string }
            return String(describing: value)
        }
        func changedColumns(
            existingRow: [String: any Sendable]?,
            insertColumns: [String],
            insertValues: [any Sendable],
            eligibleKeys: [String],
            changes: Int32
        ) -> [String] {
            guard changes != 0 else { return [] }
            guard let existingRow else { return eligibleKeys }
            var changed = [String]()
            for key in eligibleKeys {
                guard let index = insertColumns.firstIndex(of: key) else {
                    continue
                }
                let oldValue = existingRow[key] ?? SQLNull()
                let newValue = insertValues[index]
                if !(SQLValue(any: oldValue) == SQLValue(any: newValue)) {
                    changed.append(key)
                }
            }
            return changed
        }
    }
    
    @discardableResult nonisolated public consuming func upsert(
        into table: String,
        values row: consuming [String: any Sendable],
        onConflict uniquenessConstraints: consuming [[String]],
        columnsToIgnore: [String] = ["pk"],
        shouldUpdate: Bool = true,
        preferSpecificConstraint: Bool = true
    ) throws -> (
        changes: Int32,
        isInsertOperation: Bool,
        rowID: Int64,
        returningResultColumns: [String: String]
    ) {
        let columns = Array(row.keys)
        let values = columns.map { row[$0].unsafelyUnwrapped }
        return try upsert(
            into: table,
            columns: columns,
            values: values,
            onConflict: consume uniquenessConstraints,
            columnsToIgnore: columnsToIgnore,
            shouldUpdate: shouldUpdate,
            preferSpecificConstraint: preferSpecificConstraint
        )
    }
    
    @discardableResult nonisolated public consuming func insert(
        into table: String,
        orReplace: Bool = false,
        columns: [String],
        values: [any Sendable]
    ) throws -> Int64 {
        let sql = """
            \(orReplace ? "INSERT OR REPLACE" : "INSERT") INTO "\(table)" (
                \(columns.map(quote).joined(separator: ", "))
            )
            VALUES (\(Array(repeating: "?", count: columns.count).joined(separator: ", ")))
            """
        try handle.withPreparedStatement(sql, bindings: values) { statement in
            guard try statement.step == .done else {
                throw SQLError(message: handle.message, sql: sql, bindings: values)
            }
            logger.trace("Successfully inserted values. Rows affected: \(handle.rowChanges)")
        }
        return handle.lastInsertedRowID
    }
    
    @discardableResult nonisolated public consuming func insert(
        into table: String,
        orReplace: Bool = false,
        values row: [String: any Sendable]
    ) throws -> Int64 {
        try insert(
            into: table,
            orReplace: orReplace,
            columns: Array(row.keys),
            values: Array(row.values)
        )
    }
    
    nonisolated public consuming func upsertRow(
        table: String,
        values: [String: any Sendable]
    ) throws {
        let keys = values.keys.sorted()
        let columns = keys.map(quote(_:)).joined(separator: ", ")
        let placeholders = Array(repeating: "?", count: keys.count).joined(separator: ", ")
        let updates = keys
            .filter { $0 != pk }
            .map { "\(quote($0)) = excluded.\(quote($0))" }
            .joined(separator: ", ")
        try PreparedStatement(
            sql: #"""
            INSERT INTO "\#(table)" (\#(columns))
            VALUES (\#(placeholders))
            ON CONFLICT (\#(pk)) DO UPDATE SET \#(updates)
            """#,
            bindings: keys.map { SQLValue(any: values[$0] ?? NSNull()) },
            handle: handle
        ).run()
    }
    
    @discardableResult nonisolated public func update(
        for primaryKey: (some LosslessStringConvertible & Sendable)? = nil,
        table: String,
        columns columnsToUpdate: [String],
        from oldValues: [any Sendable]? = nil,
        to newValues: [any Sendable]
    ) throws -> Int32 {
        let predicate = "\(pk) = ?"
        let oldValues = try oldValues
        ?? (
            transaction == nil ? nil : fetchSingleRow(
                from: table,
                columns: columnsToUpdate,
                where: predicate,
                bindings: [primaryKey]
            )?.values
        )
        let rowChanges = try update(
            table: table,
            columns: columnsToUpdate,
            values: newValues,
            where: predicate,
            bindings: [primaryKey]
        )
        if let oldValues,
           let primaryKey = primaryKey ?? sendable(cast: newValues.first as Any) {
            transaction?.informDidUpdateRow(
                for: primaryKey,
                in: table,
                columns: columnsToUpdate,
                oldValues: oldValues,
                newValues: newValues
            )
        }
        return rowChanges
    }
    
    @discardableResult nonisolated public func update(
        table: String,
        columns: [String],
        values: [any Sendable],
        where predicate: String? = nil,
        bindings: [any Sendable] = []
    ) throws -> Int32 {
        let assignments = columns.map { "\(quote($0)) = ?" }.joined(separator: ", ")
        let sql = predicate == nil
        ? "UPDATE \(quote(table)) SET \(assignments)"
        : "UPDATE \(quote(table)) SET \(assignments) WHERE \(predicate.unsafelyUnwrapped)"
        let allBindings = values + bindings
        try handle.withPreparedStatement(sql, bindings: allBindings) { statement in
            guard try statement.step == .done else {
                throw SQLError(message: handle.message, sql: sql, bindings: allBindings)
            }
        }
        return handle.rowChanges
    }
    
    @discardableResult nonisolated public func update(
        table: String,
        values row: consuming [String: any Sendable],
        where clause: String? = nil,
        bindings: [any Sendable] = []
    ) throws -> Int32 {
        let columns = Array(row.keys)
        let values = columns.map { row[$0].unsafelyUnwrapped }
        return try update(
            table: table,
            columns: columns,
            values: values,
            where: clause,
            bindings: bindings
        )
    }
    
    nonisolated public consuming func deleteRow(
        from table: String,
        where clause: String = "pk = ?",
        bindings: [any Sendable] = []
    ) throws {
        try delete(from: table, where: clause, bindings: bindings)
    }
    
    @discardableResult nonisolated public consuming func delete(
        _ primaryKey: any Sendable,
        from table: String
    ) throws -> Int32 {
        let result = try delete(from: table, where: "pk = ?", bindings: [primaryKey])
        return result
    }
    
    @discardableResult nonisolated public consuming func delete(
        _ primaryKey: some LosslessStringConvertible & Sendable,
        from table: String,
        preservedColumns: [String]? = nil,
        preservedValues: [any Sendable]? = nil
    ) throws -> Int32 {
        let rowChanges = try delete(
            from: table,
            as: nil,
            where: "\(pk) = ?",
            bindings: [primaryKey]
        )
        if let transaction = self.transaction {
            transaction.informDidDeleteRow(
                primaryKey,
                in: table,
                preservedColumns: preservedColumns,
                preservedValues: preservedValues
            )
        }
        return rowChanges
    }
    
    /// Deletes matching rows from the specified table and returns the number of affected rows.
    ///
    /// - Warning:
    ///   All rows will be deleted if `clause` is set to `nil`.
    @discardableResult nonisolated public func delete(
        from table: String,
        as alias: String? = nil,
        where predicate: String?,
        bindings: [any Sendable],
        preservedColumns providedPreservedColumns: [String]? = nil,
        preservedValues providedPreservedValues: [any Sendable]? = nil
    ) throws -> Int32 {
        var sql = "DELETE FROM \(quote(table))"
        if let alias { sql += " AS \(quote(alias))" }
        if let clause = predicate { sql += " WHERE \(clause)" }
        try handle.withPreparedStatement(sql, bindings: bindings) { statement in
            guard try statement.step == .done else {
                throw SQLError(message: handle.message, sql: sql, bindings: bindings)
            }
        }
        return handle.rowChanges
    }
    
    nonisolated package consuming func create(
        index: some IndexDefinition,
        ifNotExists: Bool = true,
        isUnique: Bool = false
    ) throws {
        let statement = SQL {
            isUnique
            ? "CREATE UNIQUE INDEX\(ifNotExists ? " IF NOT EXISTS" : "")"
            : "CREATE INDEX\(ifNotExists ? " IF NOT EXISTS" : "")"
            index.sql
        }.sql
        logger.trace("Creating index: \(index.name)\n\(statement)")
        try handle.execute(statement.description)
    }
    
    nonisolated package consuming func create(
        table: some TableDefinition,
        ifNotExists: Bool = true,
        isTemporary: Bool = false
    ) throws {
        let statement = SQL {
            isTemporary
            ? "CREATE TEMPORARY TABLE\(ifNotExists ? " IF NOT EXISTS" : "")"
            : "CREATE TABLE\(ifNotExists ? " IF NOT EXISTS" : "")"
            table.sql
        }.sql
        logger.trace("Creating table: \(table.name)\n\(statement)")
        try handle.execute(statement)
    }
    
    nonisolated public consuming func eraseTable(_ table: String) throws {
        try handle.execute("DELETE FROM \(quote(table));")
    }
    
    nonisolated public consuming func eraseColumn(
        in table: String,
        column: String,
        to value: any Sendable = NSNull(),
        where clause: String? = nil,
        bindings: [any Sendable] = []
    ) throws {
        try PreparedStatement(
            sql: clause == nil
            ? "UPDATE \(quote(table)) SET \(quote(column)) = ?"
            : "UPDATE \(quote(table)) SET \(quote(column)) = ? WHERE \(clause!)",
            bindings: [value] + bindings,
            handle: handle
        ).run()
    }
    
    nonisolated package consuming func validateForeignKeyConstraints(
        for tables: [any TableDefinition],
        foreignKeyHandler: (any TableDefinition, [[String: any Sendable]]) throws -> Void
    ) throws {
        for table in tables {
            try foreignKeyHandler(
                table,
                handle.query("PRAGMA foreign_key_list(\(quote(table.name)))")
            )
        }
    }
}
