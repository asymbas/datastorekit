//
//  ConstraintDiagnostic.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import DataStoreSupport
import Logging
import SQLiteHandle

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

extension DatabaseConnection where Store.Handle == SQLite {
    nonisolated public func checkConstraintDiagnostics(_ error: any Swift.Error) -> [ConstraintViolation] {
        var violations = [ConstraintViolation]()
        do {
            let message = String(describing: error)
            logger.error("SQLite error: \(message)")
            let foreignKeyCheck = try query("PRAGMA foreign_key_check")
            if foreignKeyCheck.isEmpty == false {
                logger.error("FOREIGN KEY violations: \(foreignKeyCheck.count)")
            }
            for row in foreignKeyCheck {
                guard let table = row["table"] as? String else { continue }
                let rowid = (row["rowid"] as? Int64) ?? 0
                let parent = (row["parent"] as? String) ?? ""
                let fkid = (row["fkid"] as? Int64) ?? 0
                let fkList = try query("PRAGMA foreign_key_list(\(quote(table)))")
                let matching = fkList
                    .filter { ($0["id"] as? Int64) == fkid }
                    .sorted { (($0["seq"] as? Int64) ?? 0) < (($1["seq"] as? Int64) ?? 0) }
                if matching.isEmpty {
                    let header = "\(table) -> \(parent)"
                    let violation = ConstraintViolation(
                        kind: .foreignKey,
                        table: table,
                        header: header,
                        rowid: rowid,
                        parentTable: parent,
                        fkid: fkid,
                        mappings: []
                    )
                    violations.append(violation)
                    logger.error(
                        """
                        FOREIGN KEY violation:
                            table = \(table)
                            rowid = \(rowid)
                            parent = \(parent)
                            fkid = \(fkid)
                        """
                    )
                } else {
                    let parentTable = (matching.first?["table"] as? String) ?? parent
                    let mappings: [ConstraintViolation.Mapping] = matching.compactMap { row in
                        guard let from = row["from"] as? String else { return nil }
                        let toRaw = (row["to"] as? String) ?? ""
                        let to = toRaw.isEmpty ? "pk" : toRaw
                        return .init(from: from, to: to)
                    }
                    let header = mappings
                        .map { "\(table).\($0.from) -> \(parentTable).\($0.to)" }
                        .joined(separator: ", ")
                    let violation = ConstraintViolation(
                        kind: .foreignKey,
                        table: table,
                        header: header.isEmpty ? "\(table) -> \(parentTable)" : header,
                        rowid: rowid,
                        parentTable: parentTable,
                        fkid: fkid,
                        mappings: mappings
                    )
                    violations.append(violation)
                    logger.error(
                        """
                        FOREIGN KEY violation:
                            \(violation.header)
                            table = \(table)
                            rowid = \(rowid)
                            parent = \(parentTable)
                        """
                    )
                }
            }
            try appendColumnViolation(
                parseColumnFailure(message, prefix: "UNIQUE constraint failed:"),
                kind: .unique,
                label: "UNIQUE",
                includesIndexName: true,
                to: &violations
            )
            try appendColumnViolation(
                parseColumnFailure(message, prefix: "NOT NULL constraint failed:"),
                kind: .notNull,
                label: "NOT NULL",
                includesIndexName: false,
                to: &violations
            )
            try appendColumnViolation(
                parseColumnFailure(message, prefix: "PRIMARY KEY constraint failed:"),
                kind: .primaryKey,
                label: "PRIMARY KEY",
                includesIndexName: true,
                to: &violations
            )
            if let checkFailure = parseCheckFailure(message) {
                let violation = ConstraintViolation(
                    kind: .check,
                    table: "unknown",
                    header: checkFailure,
                    rowid: nil,
                    parentTable: nil,
                    fkid: nil,
                    mappings: []
                )
                violations.append(violation)
                logger.error(
                    """
                    CHECK violation:
                        \(checkFailure)
                    """
                )
            }
            if message.contains("FOREIGN KEY constraint failed"),
               foreignKeyCheck.isEmpty {
                let violation = ConstraintViolation(
                    kind: .foreignKey,
                    table: "unknown",
                    header: "FOREIGN KEY constraint failed",
                    rowid: nil,
                    parentTable: nil,
                    fkid: nil,
                    mappings: []
                )
                violations.append(violation)
            }
            if violations.isEmpty, isGenericConstraintFailure(message) {
                let violation = ConstraintViolation(
                    kind: .constraint,
                    table: "unknown",
                    header: message,
                    rowid: nil,
                    parentTable: nil,
                    fkid: nil,
                    mappings: []
                )
                violations.append(violation)
            }
        } catch {
            logger.error("Constraint diagnostics failed: \(error)")
        }
        return violations
    }
    
    nonisolated private func appendColumnViolation(
        _ failure: (table: String, columns: [String])?,
        kind: ConstraintViolation.Kind,
        label: String,
        includesIndexName: Bool,
        to violations: inout [ConstraintViolation]
    ) throws {
        guard let failure else { return }
        let columns = failure.columns.joined(separator: ",")
        let indexName = includesIndexName
        ? try findUniqueIndexName(table: failure.table, columns: failure.columns)
        : nil
        let header: String
        if let indexName {
            header = "\(failure.table).(\(columns)) [\(indexName)]"
        } else {
            header = "\(failure.table).(\(columns))"
        }
        let violation = ConstraintViolation(
            kind: kind,
            table: failure.table,
            header: header,
            rowid: nil,
            parentTable: nil,
            fkid: nil,
            mappings: []
        )
        violations.append(violation)
        var lines = [
            "\(label) violation:",
            "    table = \(failure.table)",
            "    columns = \(columns)"
        ]
        if let indexName { lines.append("    index = \(indexName)") }
        logger.error("\(lines.joined(separator: "\n"))")
    }
    
    nonisolated private func parseColumnFailure(
        _ message: String,
        prefix: String
    ) -> (table: String, columns: [String])? {
        guard let range = message.range(of: prefix) else { return nil }
        let tail = message[range.upperBound...].trimmingCharacters(in: .whitespaces)
        let parts = tail.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
        guard parts.isEmpty == false else { return nil }
        var tableName: String?
        var columns = [String]()
        for part in parts {
            let pair = part.split(separator: ".", maxSplits: 1).map(String.init)
            guard pair.count == 2 else { continue }
            if tableName == nil { tableName = pair[0] }
            columns.append(pair[1])
        }
        guard let tableName, columns.isEmpty == false else { return nil }
        return (tableName, columns)
    }
    
    nonisolated private func parseCheckFailure(_ message: String) -> String? {
        let prefix = "CHECK constraint failed:"
        guard let range = message.range(of: prefix) else {
            return nil
        }
        let tail = message[range.upperBound...].trimmingCharacters(in: .whitespaces)
        guard tail.isEmpty == false else { return nil }
        return .init(tail)
    }
    
    nonisolated private func isGenericConstraintFailure(_ message: String) -> Bool {
        let lowered = message.lowercased()
        if lowered.contains("sqlite_constraint") { return true }
        if lowered.contains("constraint failed") { return true }
        return false
    }
    
    nonisolated private func findUniqueIndexName(
        table: String,
        columns: [String]
    ) throws -> String? {
        let indexList = try query("PRAGMA index_list(\(quote(table)))")
        var uniqueIndexes = [(name: String, columns: [String])]()
        for row in indexList {
            guard let name = row["name"] as? String else {
                continue
            }
            let unique = (row["unique"] as? Int64) ?? 0
            guard unique == 1 else {
                continue
            }
            let indexInfo = try query("PRAGMA index_info(\(quote(name)))")
            let indexColumns = indexInfo
                .sorted { (($0["seqno"] as? Int64) ?? 0) < (($1["seqno"] as? Int64) ?? 0) }
                .compactMap { $0["name"] as? String }
            uniqueIndexes.append((name: name, columns: indexColumns))
        }
        if uniqueIndexes.isEmpty {
            return nil
        }
        for uniqueIndex in uniqueIndexes where uniqueIndex.columns == columns {
            return uniqueIndex.name
        }
        let targetSet = Set(columns)
        for uniqueIndex in uniqueIndexes where Set(uniqueIndex.columns) == targetSet {
            return uniqueIndex.name
        }
        return nil
    }
}
