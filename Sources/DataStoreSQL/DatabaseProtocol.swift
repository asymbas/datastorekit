//
//  DatabaseProtocol.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import SwiftData

public protocol DatabaseProtocol: DataStore, Sendable where Self.Configuration: Sendable {
    associatedtype Handle: DatabaseHandle
    associatedtype Attachment: DatabaseAttachment where Attachment.Context == Self.Context
    associatedtype Context: DatabaseContext
    associatedtype Transaction: DatabaseTransaction where Transaction.Store == Self
}

package protocol StoreBound: Sendable {
    associatedtype Store: DatabaseProtocol
}
