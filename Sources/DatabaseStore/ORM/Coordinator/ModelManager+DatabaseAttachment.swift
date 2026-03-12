//
//  ModelManager+DatabaseAttachment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import DataStoreSQL
import Foundation
import Logging
import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.transaction")

extension ModelManager: DatabaseAttachment {
    public typealias ObjectContext = SnapshotRegistry
    
    nonisolated public func makeObjectContext(editingState: some EditingStateProviding) -> ObjectContext? {
        registry(for: editingState)
    }
}
