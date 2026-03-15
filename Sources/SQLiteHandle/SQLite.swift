//
//  SQLite.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import Foundation
import Logging
import SQLite3
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.sqlite")

extension SQLite: DatabaseHandle {}

public final class SQLite: Sendable {
    nonisolated private let storage: Atomic<Int> = .init(0)
    nonisolated private let referenceCount: Atomic<Int> = .init(0)
    nonisolated internal let dataChangeNotificationContext: DataChangeNotificationContext?
    nonisolated public let id: UUID = .init()
    nonisolated public let role: DataStoreRole?
    
    nonisolated internal var pointer: OpaquePointer? {
        get { .init(bitPattern: storage.load(ordering: .sequentiallyConsistent)) }
        set {
            storage.store(
                newValue.map(Int.init(bitPattern:)) ?? 0,
                ordering: .sequentiallyConsistent
            )
            logger.debug(
                "SQLite handle set: \(newValue, default: "nil")",
                metadata: [
                    "id": "\(self.id)",
                    "role": "\(role, default: "nil")"
                ]
            )
        }
    }
    
    nonisolated public init(
        at location: SQLite.StoreType = .inMemory,
        flags: SQLite.Flags,
        role: DataStoreRole?,
        onChange: DataChangeNotificationCallback? = nil
    ) throws {
        switch role {
        case .reader:
            precondition(
                flags.contains(.readOnly),
                "DataStoreRole.reader requires SQLite.Flags.readOnly."
            )
            precondition(
                !flags.contains(.readWrite),
                "DataStoreRole.reader cannot include SQLite.Flags.readWrite."
            )
            precondition(
                !flags.contains(.create),
                "DataStoreRole.reader cannot include SQLite.Flags.create."
            )
        case .writer:
            precondition(
                flags.contains(.readWrite),
                "DataStoreRole.writer requires SQLite.Flags.readWrite."
            )
        default:
            break
        }
        var flags = flags.rawValue
        if location.requiresURI { flags |= SQLITE_OPEN_URI }
        var pointer: OpaquePointer?
        guard sqlite3_open_v2(location.description, &pointer, flags, nil) == SQLITE_OK else {
            fatalError(pointer.flatMap { String(cString: sqlite3_errmsg($0)) } ?? "unknown")
        }
        if let onChange {
            self.dataChangeNotificationContext = .init(handle: pointer, onChange: onChange)
        } else {
            self.dataChangeNotificationContext = nil
        }
        self.role = role
        self.pointer = pointer
    }
    
    deinit {
        do {
            try self.close()
            logger.debug("SQLite handle closed: \(id)")
        } catch {
            logger.error("SQLite handle did not close properly: \(error)")
        }
    }
    
    public enum TransactionMode: String, Equatable {
        case immediate
        case exclusive
    }
    
    nonisolated public var lastInsertedRowID: Int64 {
        sqlite3_last_insert_rowid(pointer)
    }
    
    nonisolated public var rowChanges: Int32 {
        sqlite3_changes(pointer)
    }
    
    nonisolated public var totalRowChanges: Int32 {
        sqlite3_total_changes(pointer)
    }
    
    nonisolated public var isInterrupted: Bool {
        sqlite3_is_interrupted(pointer) != 0
    }
    
    nonisolated public func interrupt() {
        sqlite3_interrupt(pointer)
    }
    
    nonisolated public var message: String {
        String(cString: sqlite3_errmsg(pointer))
    }
    
    nonisolated public var errorCode: Int32 {
        sqlite3_errcode(pointer)
    }
    
    nonisolated public var extendedErrorCode: Int32 {
        sqlite3_extended_errcode(pointer)
    }
    
    nonisolated public var error: any Swift.Error {
        let handle = self.pointer
        let code = sqlite3_errcode(handle)
        let extended = sqlite3_extended_errcode(handle)
        let message = String(cString: sqlite3_errmsg(handle))
        return SQLError(
            SQLite.Error(sqlite: code),
            SQLite.Error.extended(sqlite: extended),
            message: message
        )
    }
    
    nonisolated public var isReadOnly: Bool {
        switch sqlite3_db_readonly(pointer, "main") {
        case 0:
            logger.debug("DatabaseHandle is read-write.")
            return false
        case 1:
            logger.debug("DatabaseHandle is read-only.")
            return true
        case -1:
            fatalError("Invalid schema name.")
        case let mode:
            fatalError("Unexpected result: \(mode)")
        }
    }
    
    nonisolated public consuming func close() throws {
        dataChangeNotificationContext?.remove(handle: self)
        guard let handlePointer = self.pointer else {
            logger.debug("SQLite pointer is already deallocated: \(id)")
            return
        }
        let resultCode = sqlite3_close_v2(handlePointer)
        if resultCode != SQLITE_OK {
            throw Self.Error(sqlite: resultCode)
        }
        self.pointer = nil
        logger.debug("SQLite handle closed: \(id)")
    }
    
    #if DEBUG
    
    nonisolated private consuming func _close() {
        sqlite3_close(pointer)
        self.pointer = nil
    }
    
    #endif
    
    nonisolated public final var filename: String? {
        guard let cPath: sqlite3_filename = sqlite3_db_filename(pointer, nil) else {
            return nil
        }
        return .init(cString: cPath)
    }
    
    @discardableResult nonisolated public final func execute(_ sql: String) throws -> Result {
        var errorPointer: UnsafeMutablePointer<Int8>?
        let result = sqlite3_exec(pointer, sql, nil, nil, &errorPointer)
        if result != SQLITE_OK {
            let message = self.message
            if let errorPointer { sqlite3_free(errorPointer) }
            let code = self.errorCode
            let extended = self.extendedErrorCode
            throw SQLError(
                SQLite.Error(sqlite: code),
                SQLite.Error.extended(sqlite: extended),
                message: message
            )
        }
        if let errorPointer {
            defer { sqlite3_free(errorPointer) }
            throw SQLError(message: String(cString: errorPointer))
        }
        return Result(sqlite: result)
    }
    
    @discardableResult nonisolated public func execute(
        _ sql: String,
        row: (([String?], [String]) -> Bool)? = nil,
        errorMessageOutput: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>? = nil
    ) -> Result? {
        final class Box {
            let callback: ([String?], [String]) -> Bool
            init(_ callback: @escaping ([String?], [String]) -> Bool) {
                self.callback = callback
            }
        }
        guard let row else {
            return .init(rawValue: sqlite3_exec(pointer, sql, nil, nil, errorMessageOutput))
        }
        let box = Box(row)
        let context = Unmanaged.passRetained(box).toOpaque()
        defer { Unmanaged<AnyObject>.fromOpaque(context).release() }
        let callback: @convention(c) (
            _ context: UnsafeMutableRawPointer?,
            _ numberOfColumns: Int32,
            _ columnValues: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?,
            _ columnNames: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
        ) -> Int32 = { context, numberOfColumns, columnValues, columnNames in
            guard let context, let columnValues, let columnNames else {
                return 1
            }
            let box = Unmanaged<Box>.fromOpaque(context).takeUnretainedValue()
            let number = Int(numberOfColumns)
            var values = [String?](repeating: nil, count: number)
            var names = [String](repeating: "", count: number)
            for index in 0..<number {
                names[index] = columnNames[index].map { String(cString: $0) } ?? ""
                values[index] = columnValues[index].map { String(cString: $0) }
            }
            return box.callback(values, names) ? 0 : 1
        }
        return .init(rawValue: sqlite3_exec(pointer, sql, callback, context, errorMessageOutput))
    }
    
    nonisolated public func fetch<Result>(
        _ sql: String,
        bindings: [any Sendable] = [],
        into result: consuming Result,
        body: @Sendable (inout Result, ResultRows.Element) -> Void
    ) throws -> Result where Result: Collection {
        let statement = try PreparedStatement(sql: sql, bindings: bindings, handle: self)
        for row in statement.rows { body(&result, row) }
        return result
    }
    
    nonisolated public func fetch(_ sql: String, bindings: [any Sendable] = [])
    throws -> [[any Sendable]] {
        let statement = try PreparedStatement(sql: sql, bindings: bindings, handle: self)
        var resultSet = [[any Sendable]]()
        for row in statement.rows {
            var values = [any Sendable]()
            values.reserveCapacity(Int(statement.columnCount))
            for column in row.columns {
                values.append(column.value)
            }
            resultSet.append(values)
        }
        try statement.finalize()
        return resultSet
    }
    
    nonisolated public func query(_ sql: String, bindings: [any Sendable] = [])
    throws -> [[String: any Sendable]] {
        var resultSet = [[String: any Sendable]]()
        try PreparedStatement(sql: sql, bindings: bindings, handle: self).results { row in
            var result = [String: any Sendable]()
            result.reserveCapacity(Int(row.count))
            for column in row.columns {
                result[column.name] = column.value
            }
            resultSet.append(result)
        }
        return resultSet
    }
    
    @discardableResult nonisolated public final func withPreparedStatement<T>(
        _ sql: String,
        bindings: consuming [any Sendable] = [],
        body: (borrowing PreparedStatement) throws -> sending T
    ) throws/*(SQLError)*/ -> sending T {
        let statement = try PreparedStatement(sql: sql, bindings: bindings, handle: self)
        do {
            let results = try body(statement)
            try statement.finalize()
            return results
        } catch let error as Error {
            throw error
        } catch let error as SQLError {
            throw error
        } catch {
            fatalError()
        }
    }
}

extension SQLite {
    nonisolated public final class var sqliteVersion: String {
        .init(cString: sqlite3_libversion())
    }
}

extension SQLite {
    nonisolated public final class var keywordCount: Int32 {
        sqlite3_keyword_count()
    }
    
    nonisolated public final class func listAllKeywords() -> [String] {
        (0..<keywordCount).compactMap { keywordName(at: $0) }
    }
    
    nonisolated public final class func isKeyword(_ token: String) -> Bool {
        sqlite3_keyword_check(token, Int32(token.utf8.count)) == 1
    }
    
    nonisolated public final class func keywordName(at index: Int32) -> String? {
        guard index >= 0 && index < Self.keywordCount else {
            return nil
        }
        var namePointer: UnsafePointer<CChar>?
        var length: Int32 = 0
        guard Result(sqlite: sqlite3_keyword_name(index, &namePointer, &length)) == .ok,
              let namePointer, length > 0 else {
            return nil
        }
        let pointer = UnsafeRawPointer(namePointer).assumingMemoryBound(to: UInt8.self)
        let buffer = UnsafeBufferPointer(start: pointer, count: Int(length))
        return .init(decoding: buffer, as: UTF8.self)
    }
}

extension SQLite {
    nonisolated public final class var auxiliaryFileExtensions: [String] {
        ["shm", "wal", "journal"]
    }
    
    nonisolated public final class func remove(storeURL: URL) throws {
        if FileManager.default.fileExists(atPath: storeURL.path) {
            try FileManager.default.removeItem(at: storeURL)
            logger.info("The data store file has been deleted: \(storeURL.path)")
        } else {
            logger.debug("The data store file cannot be found: \(storeURL.path)")
        }
        for suffix in Self.auxiliaryFileExtensions {
            let url = URL(filePath: storeURL.path + "-\(suffix)")
            guard FileManager.default.fileExists(atPath: url.path) else {
                logger.debug("The data store auxiliary file cannot be found: \(url)")
                continue
            }
            try FileManager.default.removeItem(at: url)
            logger.info("The data store auxiliary file has been deleted: \(storeURL.path)")
        }
    }
}
