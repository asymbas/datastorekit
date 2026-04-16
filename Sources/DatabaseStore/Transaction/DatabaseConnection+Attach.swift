//
//  DatabaseConnection+Attach.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Logging
private import SQLiteHandle
internal import DataStoreSQL
internal import Foundation

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.transaction")

extension DatabaseConnection where Store.Handle: DatabaseStore.Handle {
    nonisolated internal func mainDatabaseURL() throws -> URL? {
        for row in try query("PRAGMA database_list") {
            if (row["name"] as? String) == "main" {
                guard let file = row["file"] as? String, file.isEmpty == false else {
                    return nil
                }
                return URL(fileURLWithPath: file)
            }
        }
        return nil
    }
    
    nonisolated internal func isDatabaseAttached(named name: String) throws -> Bool {
        for row in try query("PRAGMA database_list") {
            if (row["name"] as? String) == name { return true }
        }
        return false
    }
    
    nonisolated internal func attachDatabase(at url: URL, as name: String) throws {
        guard Self.isValidDatabaseName(name) else {
            throw SQLError("Invalid database name: \(name)")
        }
        if try isDatabaseAttached(named: name) { return }
        try PreparedStatement(
            sql: "ATTACH DATABASE ? AS \(name)",
            bindings: [SQLValue.text(url.path)],
            handle: handle
        ).run()
        logger.info("Attached database: \(name)")
    }
    
    nonisolated internal func detachDatabase(named name: String) throws {
        guard Self.isValidDatabaseName(name) else {
            throw SQLError("Invalid database name: \(name)")
        }
        try PreparedStatement(
            sql: "DETACH DATABASE \(name)",
            handle: handle
        ).run()
        logger.info("Detached database: \(name)")
    }
    
    nonisolated internal func detachDatabaseIfAttached(named name: String) throws {
        if try isDatabaseAttached(named: name) { try detachDatabase(named: name) }
    }
    
    nonisolated private static func isValidDatabaseName(_ name: String) -> Bool {
        guard name.isEmpty == false else {
            return false
        }
        guard name.unicodeScalars.allSatisfy({
            CharacterSet.alphanumerics
                .union(.init(charactersIn: "_"))
                .contains($0)
        }) else {
            return false
        }
        if let first = name.unicodeScalars.first {
            return CharacterSet.letters
                .union(.init(charactersIn: "_"))
                .contains(first)
        }
        return false
    }
}
