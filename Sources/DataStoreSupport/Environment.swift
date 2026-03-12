//
//  SQLFragment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation

nonisolated package func getEnvironmentValue(for key: String) -> String? {
    if let value = ProcessInfo.processInfo.environment[key] {
        return value
    } else {
        return nil
    }
}

#if false

nonisolated package func _getEnvironmentValue(for key: String) -> String? {
    if let value = getenv(key) {
        return String(cString: value)
    } else {
        return nil
    }
}

#endif
