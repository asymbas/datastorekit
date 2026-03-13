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
