//
//  SQLite-StoreType.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

extension SQLite {
    public enum StoreType: CustomStringConvertible, Sendable {
        case inMemory
        case sharedMemory(name: String)
        case temporary
        case file(path: String)
        case uri(_ uriString: String)
        
        nonisolated public var requiresURI: Bool {
            switch self {
            case .inMemory, .temporary: false
            case .sharedMemory, .uri: true
            case .file(let path): path.starts(with: "file:")
            }
        }
        
        nonisolated public var description: String {
            switch self {
            case .inMemory: ":memory:"
            case .sharedMemory(let name): "file:\(name)?mode=memory&cache=shared"
            case .temporary: ""
            case .file(let path): path
            case .uri(let uriString): uriString
            }
        }
    }
}
