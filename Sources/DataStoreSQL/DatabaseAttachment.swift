//
//  DatabaseAttachment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import Logging
import SwiftData

public protocol DatabaseAttachment: AnyObject & Sendable {
    associatedtype ObjectContext: ObjectContextProtocol
    nonisolated var storeIdentifier: String { get }
    nonisolated func makeObjectContext(editingState: some EditingStateProviding) -> ObjectContext?
}

public protocol ObjectContextProtocol: AnyObject & Identifiable & Sendable {
    associatedtype Snapshot: DataStoreSnapshot
    nonisolated func snapshot(for persistentIdentifier: PersistentIdentifier) throws -> Snapshot?
}
