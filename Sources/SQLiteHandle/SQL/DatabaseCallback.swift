//
//  DatabaseCallback.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import DataStoreCore

public typealias DataChangeNotificationCallback = (
    _ operation: DataStoreOperation,
    _ database: String,
    _ table: String,
    _ rowID: Int64
) -> Void
