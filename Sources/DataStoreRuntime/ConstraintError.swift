//
//  ConstraintError.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public import Foundation

@preconcurrency public import SwiftData

public struct ConstraintError: LocalizedError {
    nonisolated public let code: Code
    nonisolated public let message: String?
    nonisolated public let persistentIdentifier: PersistentIdentifier?
    nonisolated public let property: PropertyMetadata?
    
    nonisolated public var entityName: String? {
        persistentIdentifier?.entityName
    }
    
    nonisolated private var description: String? {
        switch (entityName, property?.name) {
        case let (entityName?, propertyName?): "\(entityName).\(propertyName)"
        case let (_, propertyName?): propertyName
        case let (entityName?, _): entityName
        case (nil, nil): nil
        }
    }
    
    nonisolated public var errorDescription: String? {
        if let message = self.message {
            return "\(code): \(message)"
        }
        switch code {
        case .unique:
            if let description = self.description {
                return "Unique constraint error: \(description)"
            } else {
                return "Unique constraint error."
            }
        case .referentialIntegrityViolated:
            if let description = self.description {
                return "Relationship constraint violation: \(description)"
            } else {
                return "Relationship constraint violation."
            }
        case .cardinalityViolation(let reason):
            if let description = self.description {
                return "Cardinality violation: \(description) - \(reason)"
            } else {
                return "Cardinality violation: \(reason)"
            }
        case .requiredRelationshipNotFound:
            if let description = self.description {
                return "Required relationship not found: \(description)"
            } else {
                return "Required relationship not found."
            }
        case .deleteRuleViolation(let deleteRule, let destination, let references):
            let count = references.count
            let keys = references.map { $0.primaryKey() }.joined(separator: ", ")
            let item = description ?? entityName ?? "model"
            switch deleteRule {
            case .deny:
                return """
                    Delete Rule Violation - Deny
                    \(item) is still referenced by \(count) models in \(destination).
                    Referencing foreign keys: \(keys)
                    """
            case .cascade:
                return nil
            case .nullify:
                return """
                    Delete Rule Violation - Nullify
                    Cannot delete \(item) because \(count) models in \(destination) would need to be nullified.
                    Referencing foreign keys: \(keys)
                    """
            case .noAction:
                return """
                    Delete Rule Violation - No Action
                    Cannot delete \(item): Delete rule leaves \(count) existing references in \(destination).
                    Referencing foreign keys: \(keys)
                    """
            @unknown default:
                fatalError(DataStoreError.unsupportedFeature.localizedDescription)
            }
        }
    }
    
    nonisolated public init(
        _ code: Code,
        message: String? = nil,
        property: PropertyMetadata? = nil,
        persistentIdentifier: PersistentIdentifier? = nil
    ) {
        self.code = code
        self.message = message
        self.persistentIdentifier = persistentIdentifier
        self.property = property
    }
    
    nonisolated public init(
        for persistentIdentifier: PersistentIdentifier? = nil,
        references: [PersistentIdentifier]? = nil,
        deleteRule code: Schema.Relationship.DeleteRule,
        destination: String = ""
    ) {
        self.code = .deleteRuleViolation(
            code,
            destination: destination,
            references: references ?? []
        )
        self.persistentIdentifier = persistentIdentifier
        self.property = nil
        self.message = nil
    }
    
    public enum Code: CustomStringConvertible, Sendable {
        case unique
        case referentialIntegrityViolated
        case cardinalityViolation(CardinalityViolationReason)
        case requiredRelationshipNotFound
        case deleteRuleViolation(
            Schema.Relationship.DeleteRule,
            destination: String,
            references: [PersistentIdentifier]
        )
        
        nonisolated public var description: String {
            switch self {
            case .unique:
                "Unique constraint error"
            case .referentialIntegrityViolated:
                "Relationship constraint violation"
            case .cardinalityViolation:
                "Cardinality violation"
            case .requiredRelationshipNotFound:
                "Required relationship not found"
            case .deleteRuleViolation(let deleteRule, _, _):
                "Delete rule violation (\(deleteRule.rawValue))"
            }
        }
    }
    
    public enum CardinalityViolationReason: CustomStringConvertible, Equatable, Sendable {
        case minimumModelCountRequired
        case maximumModelCountExceeded
        
        nonisolated public var description: String {
            switch self {
            case .minimumModelCountRequired:
                "Minimum required count not met."
            case .maximumModelCountExceeded:
                "Maximum allowed count exceeded."
            }
        }
    }
}
