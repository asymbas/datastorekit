//
//  DatabaseProtocol.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import SwiftData

public protocol DatabaseProtocol: DataStore, Sendable {
    associatedtype Handle: DatabaseHandle
    associatedtype Attachment: DatabaseAttachment where Attachment.ObjectContext == Self.Context
    associatedtype Context: ObjectContextProtocol
    associatedtype Transaction: DatabaseTransaction where Transaction.Store == Self
}

public protocol DatabaseContext: Sendable {
    associatedtype Store: DatabaseProtocol
}
