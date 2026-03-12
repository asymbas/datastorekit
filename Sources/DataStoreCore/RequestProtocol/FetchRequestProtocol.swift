//
//  FetchRequestProtocol.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import Foundation
import SwiftData

public protocol FetchRequest<ModelType>: Sendable, SendableMetatype
where ModelType: PersistentModel, EditingStateType: EditingStateProviding {
    associatedtype ModelType
    associatedtype EditingStateType
    nonisolated var descriptor: FetchDescriptor<ModelType> { get }
    nonisolated var editingState: EditingStateType { get }
}

extension DataStoreFetchRequest: FetchRequest {}

public struct DatabaseFetchRequest<T>: FetchRequest where T: PersistentModel {
    nonisolated public var descriptor: FetchDescriptor<T>
    nonisolated public var editingState: DatabaseEditingState
    
    nonisolated public init(
        descriptor: FetchDescriptor<T>,
        editingState: DatabaseEditingState
    ) {
        self.descriptor = descriptor
        self.editingState = editingState
    }
}

public struct PreloadFetchRequest<T>: FetchRequest where T: PersistentModel {
    nonisolated public var isUnchecked: Bool
    nonisolated public var modifier: (any Hashable & Sendable)?
    nonisolated public var descriptor: FetchDescriptor<T>
    nonisolated public var editingState: DatabaseEditingState
    
    nonisolated public init(
        isUnchecked: Bool,
        modifier: (any Hashable & Sendable)?,
        descriptor: FetchDescriptor<T>,
        editingState: DatabaseEditingState
    ) {
        self.isUnchecked = isUnchecked
        self.modifier = modifier
        self.descriptor = descriptor
        self.editingState = editingState
    }
}
