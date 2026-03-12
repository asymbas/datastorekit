//
//  ParseRowData.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import Foundation
import Logging
import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

nonisolated package func parseRowData(
    row: [String: any Sendable],
    entity: Schema.Entity,
    storeIdentifier: String
) throws -> (
    rowData: [String: any Sendable],
    relatedRowData: [PersistentIdentifier: [String: any Sendable]]
) {
    var rowData = [String: any Sendable]()
    var aliasedRelatedRowData = [String: [String: any Sendable]]()
    for (key, value) in row {
        let parts = key.split(separator: ".", maxSplits: 1).map(String.init)
        switch parts.count {
        case 1:
            rowData[parts[0]] = value
            logger.debug("Parsed dictionary result as (Table).(Column): \(entity.name).\(parts[0])")
        case 2:
            let aliasField = parts[0]
            let propertyField = parts[1]
            let description = "\(entity.name) -> \(aliasField).\(propertyField) -> \(key) = \(value)"
            if aliasField == entity.name || aliasField.hasSuffix("_\(entity.name)") {
                rowData[propertyField] = value
                logger.debug("Parsed dictionary result as (Alias)_(Table).(Column): \(description)")
            } else {
                aliasedRelatedRowData[aliasField, default: [:]][propertyField] = value
                logger.debug("Parsed related dictionary result as (Alias)_(Table).(Column): \(description)")
            }
        default:
            logger.warning("Parsing dictionary result received unexpected values: \(entity.name) \(key) \(value)")
            break
        }
    }
    var relatedRowData = [PersistentIdentifier: [String: any Sendable]]()
    for (alias, fields) in aliasedRelatedRowData {
        guard let referencedPrimaryKey = fields[pk] as? String else {
            continue
        }
        let relatedEntityName = alias.split(separator: "_").last.map(String.init) ?? alias
        let relatedPersistentIdentifier = try PersistentIdentifier.identifier(
            for: storeIdentifier,
            entityName: relatedEntityName,
            primaryKey: referencedPrimaryKey
        )
        relatedRowData[relatedPersistentIdentifier] = fields
        logger.debug("Created related PersistentIdentifier for \(entity.name): \(alias) \(fields)")
    }
    return (rowData, relatedRowData)
}
