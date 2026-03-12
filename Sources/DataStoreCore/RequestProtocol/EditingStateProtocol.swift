//
//  EditingStateProtocol.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import Foundation
import SwiftData

public protocol EditingStateProviding: Identifiable, Sendable, SendableMetatype
where ID == UUID {
    nonisolated var author: String? { get }
}

extension EditingState: EditingStateProviding {}

public struct DatabaseEditingState: EditingStateProviding {
    nonisolated public let id: UUID
    nonisolated public var author: String?
    
    nonisolated public init(id: UUID = .init(), author: String? = nil) {
        self.id = id
        self.author = author
    }
}
