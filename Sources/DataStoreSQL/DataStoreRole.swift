//
//  DataStoreRole.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public enum DataStoreRole: UInt8, Sendable {
    case reader
    case writer
}

extension DataStoreRole: CustomStringConvertible {
    nonisolated public var description: String {
        switch self {
        case .reader: "reader"
        case .writer: "writer"
        }
    }
}
