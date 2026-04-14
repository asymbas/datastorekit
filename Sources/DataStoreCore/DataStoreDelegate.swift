//
//  DataStoreDelegate.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import SwiftData

// TODO: Change name for `DataStoreDelegate`, because its purpose has changed.

public protocol DataStoreDelegate: AnyObject, Sendable {
    nonisolated func storeWillSave()
    nonisolated func storeDidSave(
        inserted: [PersistentIdentifier],
        updated: [PersistentIdentifier],
        deleted: [PersistentIdentifier]
    )
}
