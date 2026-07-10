//
//  DatabaseHandle.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public protocol DatabaseHandle: AnyObject & Identifiable & Sendable {
    associatedtype Result: Sendable
    associatedtype Count: FixedWidthInteger & Sendable
    nonisolated var id: Self.ID { get }
    nonisolated var role: DataStoreRole? { get }
    nonisolated var rowChanges: Count { get }
    nonisolated var totalRowChanges: Count { get }
    nonisolated func close() throws
    @discardableResult nonisolated func execute(_ sql: String) throws -> Result
    nonisolated func fetch(_ sql: String, bindings: [any Sendable])
    throws -> [[any Sendable]]
    nonisolated func query(_ sql: String, bindings: [any Sendable])
    throws -> [[String: any Sendable]]
}
