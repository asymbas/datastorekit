//
//  Environment.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Foundation

nonisolated package let shouldDisableLogging: Bool = {
    if let value = ProcessInfo.processInfo.environment["DATASTOREKIT_DISABLE_LOGGING"]?.lowercased() {
        Set(["1", "true", "yes"]).contains(value)
    } else {
        false
    }
}()

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
