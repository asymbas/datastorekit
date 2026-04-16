//
//  DataChangeNotificationContext.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreCore
private import Logging
private import SQLite3
private import Synchronization
internal import DataStoreSQL

nonisolated private let logger: Logger = .init(label: "com.asymbas.sqlite")

extension SQLite {
    internal struct DataChangeNotificationContext: ~Copyable, Sendable {
        nonisolated private let storage: Atomic<Int> = .init(0)
        
        nonisolated private var pointer: UnsafeMutableRawPointer? {
            get { .init(bitPattern: storage.load(ordering: .sequentiallyConsistent)) }
            nonmutating set {
                self.storage.store(
                    newValue.map(Int.init(bitPattern:)) ?? 0,
                    ordering: .sequentiallyConsistent
                )
            }
        }
        
        nonisolated internal init?(
            handle pointer: OpaquePointer?,
            onChange: @escaping DataChangeNotificationCallback
        ) {
            guard self.storage.load(ordering: .sequentiallyConsistent) == 0 else {
                logger.trace("SQLite data change notification is already installed.")
                return
            }
            let callback: @convention(c) (
                UnsafeMutableRawPointer?,
                Int32,
                UnsafePointer<CChar>?,
                UnsafePointer<CChar>?,
                sqlite3_int64
            ) -> Void = {
                guard let context = $0 else { return }
                let unmanaged = Unmanaged<AnyObject>.fromOpaque(context)
                let closure = unmanaged.takeUnretainedValue() as! DataChangeNotificationCallback
                let operation = DataStoreOperation(sqlite: $1)
                let databaseName = $2.map { String(cString: $0) } ?? "<unknown>"
                let tableName = $3.map { String(cString: $0) } ?? "<unknown>"
                let rowID = $4
                closure(operation, databaseName, tableName, rowID)
            }
            if let previous = self.pointer {
                Unmanaged<AnyObject>.fromOpaque(previous).release()
            }
            let retained = Unmanaged.passRetained(onChange as AnyObject).toOpaque()
            self.pointer = retained
            sqlite3_update_hook(pointer, callback, retained)
            logger.trace("Installed SQLite data change notification.")
        }
        
        nonisolated public func remove(handle: SQLite) {
            guard let pointer = handle.pointer else {
                logger.debug("No SQLite data change notification to close.")
                return
            }
            sqlite3_update_hook(pointer, nil, nil)
            if let previous = self.pointer {
                Unmanaged<AnyObject>.fromOpaque(previous).release()
                self.pointer = nil
            }
        }
    }
}

extension DataStoreOperation {
    nonisolated internal init(sqlite operationCode: Int32) {
        switch operationCode {
        case SQLITE_INSERT: self = .insert
        case SQLITE_UPDATE: self = .update
        case SQLITE_DELETE: self = .delete
        default: preconditionFailure("The raw value is not a valid SQLite operation code: \(operationCode)")
        }
    }
}
