//
//  PreparedStatement.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreSQL
import Foundation
import Logging
import SQLite3
import SQLiteStatement
import SQLSupport
import Synchronization
import System

extension PreparedStatement {
    nonisolated public init<T: DatabaseProtocol>(
        sql: String,
        bindings: [any Sendable],
        byteCount: Int32 = -1,
        prepareFlags: PrepareFlags = [],
        connection: consuming DatabaseConnection<T>
    ) throws(SQLError) where T.Handle == SQLite {
        let handle = connection.release()
        #if swift(>=6.2) && !SwiftPlaygrounds
        try self.init(
            sql: sql,
            bindings: bindings,
            byteCount: byteCount,
            prepareFlags: prepareFlags,
            handle: handle
        )
        #else
        self.storage = .init(0)
        self.handle = handle
        let statement = try PreparedStatement(
            sql: sql,
            bindings: bindings,
            byteCount: byteCount,
            prepareFlags: prepareFlags,
            handle: handle
        )
        self.pointer = statement.pointer
        #endif
    }
}

nonisolated private let logger: Logger = .init(label: "com.asymbas.sqlite")

public struct PreparedStatement: ~Copyable, Sendable {
    nonisolated private let storage: Atomic<Int>
    nonisolated private let handle: SQLite?
    nonisolated private var columnFieldBackingData: ColumnFieldBackingData?
    nonisolated internal var id: UUID = .init()
    
    nonisolated internal var pointer: OpaquePointer? {
        get { .init(bitPattern: storage.load(ordering: .sequentiallyConsistent)) }
        nonmutating set {
            storage.store(
                newValue.map(Int.init(bitPattern:)) ?? 0,
                ordering: .sequentiallyConsistent
            )
        }
    }
    
    nonisolated internal var handlePointer: OpaquePointer {
        sqlite3_db_handle(pointer)
    }
    
    /// Creates a prepared statement using `sqlite3_prepare_v2(_:_:_:_:_:)`.
    nonisolated public init(
        sql: consuming String,
        bindings: consuming [any Sendable] = [],
        byteCount: Int32 = -1,
        handle: SQLite
    ) throws(SQLError) {
        self.storage = .init(0)
        self.handle = handle
        var pointer: OpaquePointer?
        let resultCode = sqlite3_prepare_v2(handle.pointer, sql, byteCount, &pointer, nil)
        guard SQLite.Result(sqlite: resultCode) == .ok else {
            throw SQLError(pointer: handle.pointer.unsafelyUnwrapped)
        }
        guard let pointer else {
            throw SQLError(pointer: handle.pointer.unsafelyUnwrapped)
        }
        self.pointer = pointer
        if !bindings.isEmpty {
            precondition(
                self.bindParameterCount == Int32(bindings.count),
                "Mismatch in number of bindings: \(bindParameterCount) != \(bindings.count)"
            )
            self.bind(bindings)
        }
    }
    
    /// Creates a prepared statement using `sqlite3_prepare_v3(_:_:_:_:_:_:)`.
    nonisolated public init(
        sql: consuming String,
        bindings: consuming [any Sendable] = [],
        byteCount: Int32 = -1,
        prepareFlags: PrepareFlags,
        handle: SQLite
    ) throws(SQLError) {
        self.storage = .init(0)
        self.handle = handle
        let flags = UInt32(prepareFlags.rawValue)
        var pointer: OpaquePointer?
        let resultCode = sqlite3_prepare_v3(handle.pointer, sql, byteCount, flags, &pointer, nil)
        guard SQLite.Result(sqlite: resultCode) == .ok else {
            throw SQLError(pointer: handle.pointer.unsafelyUnwrapped)
        }
        guard let pointer else {
            throw SQLError(pointer: handle.pointer.unsafelyUnwrapped)
        }
        self.pointer = pointer
        if !bindings.isEmpty {
            precondition(
                self.bindParameterCount == Int32(bindings.count),
                "Mismatch in number of bindings: \(bindParameterCount) != \(bindings.count)"
            )
            self.bind(bindings)
        }
    }
    
    deinit {
        sqlite3_finalize(pointer)
        if pointer != nil {
            self.pointer = nil
        }
    }
    
    /// A single-pass sequence over result rows that resets the statement when iteration begins.
    ///
    /// - Important: Call `finalize()` for deterministic cleanup and error reporting.
    nonisolated public var rows: ResultRows {
        .init(pointer: pointer!)
    }
    
    /// Finalizes the statement and releases SQLite resources for the underlying pointer.
    ///
    /// - Returns: The SQLite result code from `sqlite3_finalize`.
    /// - Throws: `SQLite.Error` if finalization reports an error.
    @discardableResult nonisolated
    public consuming func finalize() throws(SQLite.Error) -> SQLite.Result {
        defer { self.pointer = nil }
        let code = sqlite3_finalize(pointer)
        switch SQLite.Result(rawValue: code) {
        case .ok: return .ok
        case _ where self.pointer == nil: return .ok
        default: throw SQLite.Error(sqlite: code)
        }
    }
    
    /// Steps the statement once and returns the SQLite result code for the step.
    ///
    /// - Returns: The SQLite result for the step, such as `.row` or `.done`.
    /// - Throws: `SQLite.Error` if SQLite returns an unknown or invalid result code.
    @inline(__always) nonisolated public var step: SQLite.Result {
        get throws {
            let result = sqlite3_step(pointer)
            guard let result = SQLite.Result(rawValue: result) else {
                throw SQLite.Error(sqlite: result)
            }
            return result
        }
    }
    
    @inline(__always) nonisolated public var result: ResultRow? {
        ResultRow(pointer: pointer.unsafelyUnwrapped)
    }
    
    @inline(__always) nonisolated public func next() throws -> ResultRow? {
        switch try self.step {
        case .row:
            return result
        case .done:
            return nil
        default:
            let code = sqlite3_finalize(pointer)
            self.pointer = nil
            throw SQLite.Error(sqlite: code)
        }
    }
    
    @discardableResult nonisolated public consuming
    func results(body: (ResultRow) throws -> Void) throws -> SQLite.Result {
        while true {
            if let row = try next() {
                try body(row)
            } else {
                return try finalize()
            }
        }
    }
    
    /// Evaluates the statement by calling `next()` until completion and then finalizes.
    ///
    /// - Throws: The first error thrown by stepping or finalization.
    nonisolated public consuming func run() throws {
        while true {
            if let _ = try next() { continue }
            try finalize()
            return
        }
    }
    
    private struct ColumnFieldBackingData {
        nonisolated internal var names: [String]
        nonisolated internal var mapExact: [String: Int32]
    }
    
    nonisolated private var columnsStorage: ColumnFieldBackingData? {
        get { columnFieldBackingData }
        set { self.columnFieldBackingData = newValue }
    }
    
    /// The number of columns in the current result set.
    nonisolated public var columnCount: Int32 {
        sqlite3_column_count(pointer)
    }
    
    /// Build or return cached columns.
    nonisolated public var columns: [String] {
        mutating get {
            if let cachedColumns = self.columnsStorage {
                return cachedColumns.names
            }
            var names = [String]()
            names.reserveCapacity(Int(columnCount))
            var exact = [String: Int32]()
            for index in 0..<columnCount {
                guard let name = self.columnName(at: index) else {
                    preconditionFailure("There should be a valid column name at this index.")
                }
                names.append(name)
                if exact[name] == nil { exact[name] = index }
            }
            let builtCachedColumns = ColumnFieldBackingData(names: names, mapExact: exact)
            self.columnsStorage = builtCachedColumns
            return builtCachedColumns.names
        }
    }
    
    /// Indicates whether the statement is currently busy.
    nonisolated public var isBusy: Bool {
        sqlite3_stmt_busy(pointer) != 0
    }
    
    /// Indicates whether the statement is read-only.
    nonisolated public var isReadOnly: Bool {
        sqlite3_stmt_readonly(pointer) != 0
    }
    
    /// Indicates whether the statement is an `EXPLAIN` statement.
    nonisolated public var isExplain: Bool {
        sqlite3_stmt_isexplain(pointer) != 0
    }
    
    /// The original SQL text used to prepare this statement, if available.
    nonisolated public var sql: String? {
        if let result = sqlite3_sql(pointer) {
            return .init(cString: result)
        } else {
            return nil
        }
    }
    
    /// The expanded SQL text with bound values substituted, if available.
    nonisolated public var expandedSQL: String? {
        guard let sql = sqlite3_expanded_sql(pointer) else {
            return nil
        }
        defer { sqlite3_free(sql) }
        return .init(cString: sql)
    }
    
    /// The number of parameter placeholders in this statement.
    nonisolated public var bindParameterCount: Int32 {
        sqlite3_bind_parameter_count(pointer)
    }
    
    nonisolated public func bindParameterName(at index: Int32) -> String? {
        guard let result = sqlite3_bind_parameter_name(pointer, index) else {
            return nil
        }
        return .init(cString: result)
    }
    
    nonisolated public func bindParameterIndex(for column: String) -> Int32? {
        switch sqlite3_bind_parameter_index(pointer, column) {
        case 0: nil
        case let value: value
        }
    }
    
    nonisolated public func columnName(at index: Int32) -> String? {
        guard let name = sqlite3_column_name(pointer, index) else {
            return nil
        }
        return .init(cString: name)
    }
    
    nonisolated public mutating func columnIndex(named name: String) -> Int32? {
        if columnsStorage == nil { _ = self.columns }
        if let index = self.columnsStorage?.mapExact[name] { return index }
        return nil
    }
    
    /// Clears any currently bound parameter values.
    nonisolated public mutating func clearBindings() {
        sqlite3_clear_bindings(pointer)
    }
    
    /// Resets the statement to its initial state without clearing bindings.
    nonisolated public mutating func reset() {
        sqlite3_reset(pointer)
    }
    
    /// Clears bindings, resets the statement, and clears cached column metadata.
    nonisolated public mutating func resetAll() {
        clearBindings()
        reset()
        self.columnsStorage = nil
    }
    
    /// Binds an `Int` value to a 1-based parameter index.
    ///
    /// - Parameters:
    ///   - value: The integer value to bind.
    ///   - offset: The 1-based parameter index.
    nonisolated public func bind(asInt value: Int, at offset: Int32) {
        sqlite3_bind_int(pointer, offset, Int32(value))
    }
    
    /// Binds positional values to parameters starting at index 1.
    nonisolated internal func bind(_ values: consuming [any Sendable]) {
        let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        sqlite3_clear_bindings(pointer)
        for (index, value) in values.enumerated() {
            let offset = Int32(index + 1)
            #if DEBUG
            logger.trace("Binding value to position: \(value) as \(type(of: value)).self (position: \(offset))")
            #endif
            let bind: SQLValue
            if let value = value as? SQLValue {
                bind = value
            } else {
                bind = SQLValue(any: value)
            }
            switch bind.sqlType {
            case .null:
                sqlite3_bind_null(pointer, offset)
            case .integer:
                sqlite3_bind_int64(pointer, offset, bind.base as! Int64)
            case .real:
                sqlite3_bind_double(pointer, offset, bind.base as! Double)
            case .text:
                sqlite3_bind_text(pointer, offset, bind.base as! String, -1, SQLITE_TRANSIENT)
            case .blob:
                _ = (bind.base as! Data).withUnsafeBytes { rawBufferPointer in
                    sqlite3_bind_blob(
                        pointer,
                        offset,
                        rawBufferPointer.baseAddress,
                        Int32(rawBufferPointer.count),
                        SQLITE_TRANSIENT
                    )
                }
            }
        }
    }
    
    public struct PrepareFlags: OptionSet, Sendable {
        nonisolated public let rawValue: Int32
        
        nonisolated public init(rawValue: Int32) {
            self.rawValue = rawValue
        }
        
        nonisolated public init(sqlite rawValue: Int32) {
            self.rawValue = rawValue
        }
        
        nonisolated public static var persistent: Self {
            .init(sqlite: SQLITE_PREPARE_PERSISTENT)
        }
        
        nonisolated public static var normalize: Self {
            .init(sqlite: SQLITE_PREPARE_NORMALIZE)
        }
        
        nonisolated public static var noVTab: Self {
            .init(sqlite: SQLITE_PREPARE_NO_VTAB)
        }
        
        nonisolated public static var dontLog: Self {
            .init(sqlite: /*SQLITE_PREPARE_DONT_LOG*/ 0) // Not on available on Swift Playground.
        }
        /// Uses no flags.
        nonisolated public static var none: Self {
            []
        }
    }
}

public struct ResultRows: AsyncSequence, Sendable, Sequence {
    /// Inherited from `AsyncSequence.AsyncIterator`.
    public typealias AsyncIterator = ResultAsyncIterator
    /// Inherited from `Sequence.Iterator`.
    public typealias Iterator = ResultIterator
    nonisolated(unsafe) internal let pointer: OpaquePointer
    
    nonisolated internal init(pointer: OpaquePointer) {
        self.pointer = pointer
    }
    
    /// Inherited from `Sequence.makeIterator()`.
    nonisolated public func makeIterator() -> Iterator {
        sqlite3_reset(pointer)
        return Iterator(pointer: pointer)
    }
    
    /// Inherited from `AsyncSequence.makeAsyncIterator()`.
    nonisolated public func makeAsyncIterator() -> AsyncIterator {
        sqlite3_reset(pointer)
        return AsyncIterator(pointer: pointer)
    }
    
    public struct ResultIterator: IteratorProtocol, Sendable {
        /// Inherited from `IteratorProtocol.Element`.
        public typealias Element = ResultRow
        nonisolated(unsafe) private let pointer: OpaquePointer
        nonisolated private var isFinished: Bool = false
        
        nonisolated internal init(pointer: OpaquePointer) {
            self.pointer = pointer
        }
        
        /// Inherited from `IteratorProtocol.next()`.
        nonisolated public mutating func next() -> Element? {
            if isFinished { return nil }
            let resultCode = sqlite3_step(pointer)
            if resultCode == SQLITE_ROW {
                return ResultRow(pointer: pointer)
            }
            self.isFinished = true
            return nil
        }
    }
    
    public struct ResultAsyncIterator: AsyncIteratorProtocol, Sendable {
        /// Inherited from `AsyncIteratorProtocol.Element`.
        public typealias Element = ResultRow
        nonisolated(unsafe) private let pointer: OpaquePointer
        nonisolated private var isFinished: Bool = false
        
        nonisolated internal init(pointer: OpaquePointer) {
            self.pointer = pointer
        }
        
        /// Inherited from `AsyncIteratorProtocol.next()`.
        nonisolated public mutating func next() async throws -> Element? {
            if isFinished { return nil }
            let pointer = Mutex<OpaquePointer>(pointer)
            let result: (
                row: ResultRow?,
                terminal: Bool
            ) = try await withCheckedThrowingContinuation { @Sendable continuation in
                Task {
                    if Task.isCancelled {
                        continuation.resume(throwing: CancellationError())
                        return
                    }
                    let resultCode = sqlite3_step(pointer.withLock(\.self))
                    if Task.isCancelled {
                        continuation.resume(throwing: CancellationError())
                        return
                    }
                    switch resultCode {
                    case SQLITE_ROW:
                        continuation.resume(returning: (ResultRow(pointer: pointer.withLock(\.self)), false))
                    case SQLITE_DONE:
                        continuation.resume(returning: (nil, true))
                    default:
                        sqlite3_reset(pointer.withLock(\.self))
                        let handle = sqlite3_db_handle(pointer.withLock(\.self))
                        continuation.resume(throwing: SQLError(pointer: handle!))
                    }
                }
            }
            if result.terminal { self.isFinished = true }
            return result.row
        }
    }
}

public struct ResultRow: Sendable {
    nonisolated(unsafe) internal let pointer: OpaquePointer
    nonisolated internal let count: Int32
    
    nonisolated public var columns: ResultColumns {
        .init(row: self, count: count)
    }
    
    nonisolated fileprivate init(pointer: OpaquePointer) {
        self.pointer = pointer
        self.count = sqlite3_column_count(pointer)
    }
    
    nonisolated public func columnType(at index: Int32) -> SQLType? {
        SQLType(sqlite: sqlite3_column_type(pointer, index))
    }
    
    @Sendable nonisolated private func value(at columnIndex: Int32) -> any Sendable {
        switch sqlite3_column_type(pointer, columnIndex) {
        case SQLITE_NULL: NSNull()
        case SQLITE_INTEGER: sqlite3_column_int64(pointer, columnIndex)
        case SQLITE_FLOAT: sqlite3_column_double(pointer, columnIndex)
        case SQLITE_TEXT: String(cString: sqlite3_column_text(pointer, columnIndex).unsafelyUnwrapped)
        case SQLITE_BLOB: Data(
            bytes: sqlite3_column_blob(pointer, columnIndex).unsafelyUnwrapped,
            count: Int(sqlite3_column_bytes(pointer, columnIndex))
        )
        default: fatalError("Unsupported column type at index \(columnIndex).")
        }
    }
    
    nonisolated public subscript(_ index: Int32) -> (any Sendable)? {
        switch columnType(at: index) {
        case .null: NSNull()
        case .integer: self.int64(index)
        case .real: self.double(index)
        case .text: self.text(index)
        case .blob: self.blob(index)
        default: nil
        }
    }
    
    nonisolated public subscript<T>(_ index: Int32, as type: T.Type) -> T? {
        switch type {
        case is Int32.Type: self.int(index) as? T
        case is Int64.Type, is sqlite_int64.Type: self.int64(index) as? T
        case is Double.Type: self.double(index) as? T
        case is String.Type: self.text(index) as? T
        case is Data.Type: self.blob(index) as? T
        default: self[index] as? T
        }
    }
    
    nonisolated public func isNull(_ index: Int32) -> Bool {
        sqlite3_column_type(pointer, index) == SQLITE_NULL
    }
    
    nonisolated public func int(_ index: Int32) -> Int32 {
        sqlite3_column_int(pointer, index)
    }
    
    nonisolated public func int64(_ index: Int32) -> Int64 {
        sqlite3_column_int64(pointer, index)
    }
    
    nonisolated public func double(_ index: Int32) -> Double {
        sqlite3_column_double(pointer, index)
    }
    
    nonisolated public func text(_ index: Int32) -> String {
        guard let value = sqlite3_column_text(pointer, index) else {
            return ""
        }
        return String(cString: value)
    }
    
    nonisolated public func blob(_ index: Int32) -> Data {
        let bytes = sqlite3_column_blob(pointer, index)
        let count = sqlite3_column_bytes(pointer, index)
        return bytes != nil ? Data(bytes: bytes!, count: Int(count)) : Data()
    }
}

public struct ResultColumns: AsyncSequence, Sendable, Sequence {
    /// Inherited from `AsyncSequence.AsyncIterator`.
    public typealias AsyncIterator = ResultAsyncIterator
    /// Inherited from `Sequence.Iterator`.
    public typealias Iterator = ResultIterator
    nonisolated private let row: ResultRow
    nonisolated private let count: Int32
    
    nonisolated internal init(row: ResultRow, count: Int32) {
        self.row = row
        self.count = count
    }
    
    /// Inherited from `Sequence.makeIterator()`.
    nonisolated public func makeIterator() -> Iterator {
        Iterator(row: row, count: count)
    }
    
    /// Inherited from `AsyncSequence.makeAsyncIterator()`.
    nonisolated public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(row: row, count: count)
    }
    
    public struct ResultIterator: IteratorProtocol {
        /// Inherited from `IteratorProtocol.Element`.
        public typealias Element = ResultColumn
        nonisolated private let row: ResultRow
        nonisolated private let count: Int32
        private var index: Int32 = 0
        
        nonisolated internal init(row: ResultRow, count: Int32) {
            self.row = row
            self.count = count
        }
        
        /// Inherited from `IteratorProtocol.next()`.
        nonisolated public mutating func next() -> Element? {
            guard index < count else {
                return nil
            }
            let index = self.index
            defer { self.index += 1 }
            let name = sqlite3_column_name(row.pointer, index).map {
                String(cString: $0)
            } ?? ""
            let value = self.row[index]
            return ResultColumn(index: index, name: name, value: value)
        }
    }
    
    public struct ResultAsyncIterator: AsyncIteratorProtocol {
        /// Inherited from `AsyncIteratorProtocol.Element`.
        public typealias Element = ResultColumn
        nonisolated private let row: ResultRow
        nonisolated private let count: Int32
        private var index: Int32 = 0
        
        nonisolated fileprivate init(row: ResultRow, count: Int32) {
            self.row = row
            self.count = count
        }
        
        /// Inherited from `AsyncIteratorProtocol.next()`.
        nonisolated public mutating func next() async throws -> Element? {
            guard index < count else {
                return nil
            }
            let index = self.index
            defer { self.index += 1 }
            let name = sqlite3_column_name(row.pointer, index).map {
                String(cString: $0)
            } ?? ""
            let value = self.row[index]
            return ResultColumn(index: index, name: name, value: value)
        }
    }
}

public struct ResultColumn: Sendable {
    nonisolated public var index: Int32
    nonisolated public var name: String
    nonisolated public var value: (any Sendable)?
}
