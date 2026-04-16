//
//  DatabaseAttachment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import DataStoreCore
public import SwiftData

public protocol DataStoreSnapshotProvider: AnyObject, Sendable {
    associatedtype Snapshot: DataStoreSnapshot
    func resolvedPersistentIdentifier(for persistentIdentifier: PersistentIdentifier) -> PersistentIdentifier?
    func primaryKey<PrimaryKey>(for persistentIdentifier: PersistentIdentifier, as type: PrimaryKey.Type) -> PrimaryKey
    where PrimaryKey: LosslessStringConvertible & Sendable
    func snapshot(for persistentIdentifier: PersistentIdentifier) -> Snapshot?
}

public protocol DatabaseAttachment: AnyObject & Sendable, DataStoreSnapshotProvider {
    associatedtype Context: DatabaseContext
    nonisolated func makeObjectContext(editingState: some EditingStateProviding) -> Context?
}

public protocol DatabaseContext: AnyObject & Identifiable & Sendable, DataStoreSnapshotProvider {
    nonisolated func snapshot(for persistentIdentifier: PersistentIdentifier) -> Snapshot?
}
