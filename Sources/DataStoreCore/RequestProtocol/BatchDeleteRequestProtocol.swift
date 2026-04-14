//
//  BatchDeleteRequestProtocol.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import DataStoreSupport
public import Foundation
public import SwiftData

public protocol BatchDeleteRequest: Sendable, SendableMetatype {
    associatedtype ModelType: PersistentModel
    associatedtype EditingStateType: EditingStateProviding
    nonisolated var editingState: EditingStateType { get }
//    nonisolated var includeSubclasses: Bool { get }
    nonisolated var predicate: Predicate<ModelType>? { get }
}

extension DataStoreBatchDeleteRequest: BatchDeleteRequest {}
