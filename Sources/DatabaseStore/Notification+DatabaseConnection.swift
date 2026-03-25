//
//  Notification+DatabaseConnection.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation

extension Notification.Name {
    nonisolated public static let databaseConnection: Self = .init("DatabaseConnection")
}

extension Notification.Name {
    nonisolated public static let dataStoreDidSave: Self = .init("DataStoreDidSave")
}
