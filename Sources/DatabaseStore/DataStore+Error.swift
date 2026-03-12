//
//  DataStore+Error.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import Foundation

extension DatabaseStore {
    public enum Error: Swift.Error, LocalizedError {
        /// The snapshot repeatedly failed to meet expectations prior to an insert during the save operation.
        case exceededMaximumInsertAttempts
        /// The data store cannot be opened, because the configuration has incorrect metadata.
        case invalidStoreConfiguration
        case invalidStoreIdentifier
        case storeFileTypeIsNotCompatible
        case storeFileVersionIsNotCompatible
        
        
    }
}
