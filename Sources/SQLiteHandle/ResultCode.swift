//
//  SQLiteResultCode.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import SQLite3

extension SQLError {
    nonisolated package init(pointer: OpaquePointer) {
        let extendedCode = sqlite3_extended_errcode(pointer)
        self.init(message: String(cString: sqlite3_errmsg(pointer)) + " (extended: \(extendedCode))")
    }
}

public protocol ResultCode:
    CaseIterable,
    CustomStringConvertible,
    Equatable,
    Hashable,
    RawRepresentable,
    Sendable {
}

extension SQLite {
    @frozen public struct Result: ResultCode {
        nonisolated public let rawValue: Int32
        
        nonisolated public init?(rawValue: RawValue) {
            guard Self.allCases.contains(where: { $0.rawValue == rawValue }) else {
                return nil
            }
            self.rawValue = rawValue
        }
    }
}

extension SQLite.Result {
    nonisolated internal init(sqlite rawValue: RawValue) {
        self.rawValue = rawValue
    }
    
    /// `SQLITE_OK` or `0` raw value: Successful result.
    nonisolated public static var ok: Self {
        .init(sqlite: SQLITE_OK)
    }
    
    /// `SQLITE_ROW` or `100` raw value: `sqlite3_step()` has another row ready.
    nonisolated public static var row: Self {
        .init(sqlite: SQLITE_ROW)
    }
    
    /// `SQLITE_DONE` or `101` raw value: `sqlite3_step()` has finished executing.
    nonisolated public static var done: Self {
        .init(sqlite: SQLITE_DONE)
    }
}

extension SQLite.Result: CaseIterable {
    nonisolated public static var allCases: Set<Self> {
        [.ok, .row, .done]
    }
}

extension SQLite.Result: CustomStringConvertible {
    nonisolated public var description: String {
        switch self {
        case .ok: "SQLITE_OK"
        case .row: "SQLITE_ROW"
        case .done: "SQLITE_DONE"
        default: "SQLResultCode.Unknown(\(rawValue))"
        }
    }
}

extension SQLite {
    @frozen public struct Error: ResultCode, Swift.Error {
        nonisolated public let rawValue: Int32
        
        nonisolated public init?(rawValue: RawValue) {
            guard Self.allCases.contains(where: { $0.rawValue == rawValue }) else {
                return nil
            }
            self.rawValue = rawValue
        }
    }
}

extension SQLite.Error {
    nonisolated internal init(sqlite rawValue: RawValue) {
        self.rawValue = rawValue
    }
    
    nonisolated internal static func extended(sqlite rawValue: RawValue) -> Self {
        self.init(sqlite: rawValue)
    }
    
    nonisolated public static var error: Self {
        .init(sqlite: SQLITE_ERROR)
    }
    
    nonisolated public static var internalError: Self {
        .init(sqlite: SQLITE_INTERNAL)
    }
    
    nonisolated public static var permissionDenied: Self {
        .init(sqlite: SQLITE_PERM)
    }
    
    nonisolated public static var abort: Self {
        .init(sqlite: SQLITE_ABORT)
    }
    
    nonisolated public static var busy: Self {
        .init(sqlite: SQLITE_BUSY)
    }
    
    nonisolated public static var locked: Self {
        .init(sqlite: SQLITE_LOCKED)
    }
    
    nonisolated public static var noMemory: Self {
        .init(sqlite: SQLITE_NOMEM)
    }
    
    nonisolated public static var readOnly: Self {
        .init(sqlite: SQLITE_READONLY)
    }
    
    nonisolated public static var interrupt: Self {
        .init(sqlite: SQLITE_INTERRUPT)
    }
    
    nonisolated public static var ioError: Self {
        .init(sqlite: SQLITE_IOERR)
    }
    
    nonisolated public static var corrupt: Self {
        .init(sqlite: SQLITE_CORRUPT)
    }
    
    nonisolated public static var notFound: Self {
        .init(sqlite: SQLITE_NOTFOUND)
    }
    
    nonisolated public static var full: Self {
        .init(sqlite: SQLITE_FULL)
    }
    
    nonisolated public static var cantOpen: Self {
        .init(sqlite: SQLITE_CANTOPEN)
    }
    
    nonisolated public static var protocolError: Self {
        .init(sqlite: SQLITE_PROTOCOL)
    }
    
    nonisolated public static var empty: Self {
        .init(sqlite: SQLITE_EMPTY)
    }
    
    nonisolated public static var schema: Self {
        .init(sqlite: SQLITE_SCHEMA)
    }
    
    nonisolated public static var tooBig: Self {
        .init(sqlite: SQLITE_TOOBIG)
    }
    
    nonisolated public static var constraint: Self {
        .init(sqlite: SQLITE_CONSTRAINT)
    }
    
    nonisolated public static var mismatch: Self {
        .init(sqlite: SQLITE_MISMATCH)
    }
    
    nonisolated public static var misuse: Self {
        .init(sqlite: SQLITE_MISUSE)
    }
    
    nonisolated public static var nolfs: Self {
        .init(sqlite: SQLITE_NOLFS)
    }
    
    nonisolated public static var authorizationDenied: Self {
        .init(sqlite: SQLITE_AUTH)
    }
    
    nonisolated public static var format: Self {
        .init(sqlite: SQLITE_FORMAT)
    }
    
    nonisolated public static var range: Self {
        .init(sqlite: SQLITE_RANGE)
    }
    
    nonisolated public static var notADatabaseFile: Self {
        .init(sqlite: SQLITE_NOTADB)
    }
    
    nonisolated public static var notice: Self {
        .init(sqlite: SQLITE_NOTICE)
    }
    
    nonisolated public static var warning: Self {
        .init(sqlite: SQLITE_WARNING)
    }
}

extension SQLite.Error: CaseIterable {
    nonisolated public static var allCases: Set<Self> {
        [
            .error,
            .internalError,
            .permissionDenied,
            .abort,
            .busy,
            .locked,
            .noMemory,
            .readOnly,
            .interrupt,
            .ioError,
            .corrupt,
            .notFound,
            .full,
            .cantOpen,
            .protocolError,
            .empty,
            .schema,
            .tooBig,
            .constraint,
            .mismatch,
            .misuse,
            .nolfs,
            .authorizationDenied,
            .format,
            .range,
            .notADatabaseFile,
            .notice,
            .warning
        ]
    }
}

extension SQLite.Error: CustomStringConvertible {
    nonisolated public var description: String {
        switch self {
        case .error: "SQLITE_ERROR"
        case .internalError: "SQLITE_INTERNAL"
        case .permissionDenied: "SQLITE_PERM"
        case .abort: "SQLITE_ABORT"
        case .busy: "SQLITE_BUSY"
        case .locked: "SQLITE_LOCKED"
        case .noMemory: "SQLITE_NOMEM"
        case .readOnly: "SQLITE_READONLY"
        case .interrupt: "SQLITE_INTERRUPT"
        case .ioError: "SQLITE_IOERR"
        case .corrupt: "SQLITE_CORRUPT"
        case .notFound: "SQLITE_NOTFOUND"
        case .full: "SQLITE_FULL"
        case .cantOpen: "SQLITE_CANTOPEN"
        case .protocolError: "SQLITE_PROTOCOL"
        case .empty: "SQLITE_EMPTY"
        case .schema: "SQLITE_SCHEMA"
        case .tooBig: "SQLITE_TOOBIG"
        case .constraint: "SQLITE_CONSTRAINT"
        case .mismatch: "SQLITE_MISMATCH"
        case .misuse: "SQLITE_MISUSE"
        case .nolfs: "SQLITE_NOLFS"
        case .authorizationDenied: "SQLITE_AUTH"
        case .format: "SQLITE_FORMAT"
        case .range: "SQLITE_RANGE"
        case .notADatabaseFile: "SQLITE_NOTADB"
        case .notice: "SQLITE_NOTICE"
        case .warning: "SQLITE_WARNING"
        default: "SQLite.Unknown(\(rawValue))"
        }
    }
}
