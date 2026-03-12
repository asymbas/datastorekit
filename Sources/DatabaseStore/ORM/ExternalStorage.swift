//
//  ExternalStorage.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation
import Logging

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

public struct ExternalStoragePath: Sendable {
    /// The relative path is structured as `entity-name/attribute-name/primary-key`.
    nonisolated public let relativePath: String
    nonisolated public let component: String
    nonisolated public let data: Data?
    nonisolated public var storeType: StoreType = .redirect
    
    public enum StoreType: Sendable {
        case inline
        case redirect
    }
}

internal struct ExternalStorageTransaction: Sendable {
    nonisolated private let baseURL: URL
    nonisolated private let transactionRootURL: URL
    nonisolated private var backupURLByTargetPath: [String: URL] = [:]
    nonisolated private var createdTargetPaths: Set<String> = []
    nonisolated private var updatedTargetPaths: Set<String> = []
    nonisolated private var deletedTargetPaths: Set<String> = []
    
    nonisolated internal init(baseURL: URL) throws {
        self.baseURL = baseURL
        self.transactionRootURL = baseURL
            .appending(path: ".external-storage-transaction", directoryHint: .isDirectory)
            .appending(path: UUID().uuidString, directoryHint: .isDirectory)
        try FileManager.default.createDirectory(
            at: transactionRootURL,
            withIntermediateDirectories: true
        )
    }
    
    nonisolated package mutating func apply(_ metadata: [ExternalStoragePath]) throws {
        let baseURL = self.baseURL
        for external in metadata where external.storeType == .redirect {
            let targetFileURL = baseURL.appending(path: external.relativePath)
            let targetKey = targetFileURL.path
            switch external.data {
            case let data?:
                let relativePath = external.relativePath + ".staged." + UUID().uuidString
                let stagedFileURL = self.transactionRootURL.appending(path: relativePath)
                let stagedDirectoryURL = stagedFileURL.deletingLastPathComponent()
                if !FileManager.default.fileExists(atPath: stagedDirectoryURL.path) {
                    try FileManager.default.createDirectory(
                        at: stagedDirectoryURL,
                        withIntermediateDirectories: true
                    )
                }
                try data.write(to: stagedFileURL, options: .atomic)
                let targetDirectoryURL = targetFileURL.deletingLastPathComponent()
                if !FileManager.default.fileExists(atPath: targetDirectoryURL.path) {
                    try FileManager.default.createDirectory(
                        at: targetDirectoryURL,
                        withIntermediateDirectories: true
                    )
                }
                let targetExistedBefore = FileManager.default.fileExists(atPath: targetFileURL.path)
                let hadBackupAlready = (backupURLByTargetPath[targetKey] != nil)
                if targetExistedBefore {
                    if createdTargetPaths.contains(targetKey) {
                        try FileManager.default.removeItem(at: targetFileURL)
                        updatedTargetPaths.remove(targetKey)
                        deletedTargetPaths.remove(targetKey)
                    } else if !hadBackupAlready {
                        let relativePath = external.relativePath + ".backup"
                        let backupFileURL = self.transactionRootURL.appending(path: relativePath)
                        let backupDirectoryURL = backupFileURL.deletingLastPathComponent()
                        if !FileManager.default.fileExists(atPath: backupDirectoryURL.path) {
                            try FileManager.default.createDirectory(
                                at: backupDirectoryURL,
                                withIntermediateDirectories: true
                            )
                        }
                        try FileManager.default.moveItem(at: targetFileURL, to: backupFileURL)
                        backupURLByTargetPath[targetKey] = backupFileURL
                    } else {
                        try FileManager.default.removeItem(at: targetFileURL)
                    }
                } else if backupURLByTargetPath[targetKey] == nil {
                    createdTargetPaths.insert(targetKey)
                    updatedTargetPaths.remove(targetKey)
                    deletedTargetPaths.remove(targetKey)
                }
                if hadBackupAlready || targetExistedBefore {
                    if !createdTargetPaths.contains(targetKey) {
                        updatedTargetPaths.insert(targetKey)
                    }
                }
                deletedTargetPaths.remove(targetKey)
                try FileManager.default.moveItem(at: stagedFileURL, to: targetFileURL)
            case nil:
                guard FileManager.default.fileExists(atPath: targetFileURL.path) else {
                    continue
                }
                if createdTargetPaths.contains(targetKey) {
                    try FileManager.default.removeItem(at: targetFileURL)
                    createdTargetPaths.remove(targetKey)
                    updatedTargetPaths.remove(targetKey)
                    deletedTargetPaths.remove(targetKey)
                    continue
                }
                if backupURLByTargetPath[targetKey] == nil {
                    let relativePath = external.relativePath + ".backup"
                    let backupFileURL = self.transactionRootURL.appending(path: relativePath)
                    let backupDirectoryURL = backupFileURL.deletingLastPathComponent()
                    if !FileManager.default.fileExists(atPath: backupDirectoryURL.path) {
                        try FileManager.default.createDirectory(
                            at: backupDirectoryURL,
                            withIntermediateDirectories: true
                        )
                    }
                    try FileManager.default.moveItem(at: targetFileURL, to: backupFileURL)
                    backupURLByTargetPath[targetKey] = backupFileURL
                } else {
                    try FileManager.default.removeItem(at: targetFileURL)
                }
                deletedTargetPaths.insert(targetKey)
                updatedTargetPaths.remove(targetKey)
            }
        }
    }
    
    nonisolated package func rollback() {
        for targetPath in createdTargetPaths {
            let url = URL(fileURLWithPath: targetPath)
            try? FileManager.default.removeItem(at: url)
        }
        for (targetPath, backupURL) in backupURLByTargetPath {
            let targetURL = URL(fileURLWithPath: targetPath)
            try? FileManager.default.removeItem(at: targetURL)
            try? FileManager.default.moveItem(at: backupURL, to: targetURL)
        }
        try? FileManager.default.removeItem(at: transactionRootURL)
    }
    
    nonisolated package func commit() throws {
        try FileManager.default.removeItem(at: transactionRootURL)
        let created = createdTargetPaths.count
        let updated = updatedTargetPaths.count
        let deleted = deletedTargetPaths.count
        let total = created + updated + deleted
        if total != 0 {
            logger.info(
                "Committed external storage transaction: created \(created), updated \(updated), deleted \(deleted).",
                metadata: ["total": "\(total)"]
            )
        }
    }
}
