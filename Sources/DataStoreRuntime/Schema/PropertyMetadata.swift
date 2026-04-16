//
//  PropertyMetadata.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreCore
private import DataStoreSupport
private import Logging
private import SQLiteHandle
private import Synchronization
public import DataStoreSQL

@preconcurrency public import SwiftData

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

extension Array where Element == PropertyMetadata {
    nonisolated public var columns: [String] {
        [pk] + compactMap(\.column)
    }
}

public struct PropertyMetadata: Equatable, Hashable, Sendable {
    nonisolated package var flags: Flags
    /// The key path for binding the property to a column.
    nonisolated public var keyPath: AnyKeyPath & Sendable
    /// The element index of a `SchemaProperty` as positioned in `Schema.storedProperties`.
    nonisolated public var index: Int
    /// The property's name.
    nonisolated public var name: String
    /// Metadata dependencies required to include with a query.
    nonisolated public var reference: [TableReference]?
    /// References the `SchemaProperty` that this metadata is based on.
    nonisolated public let metadata: any SchemaProperty
    /// References a `SchemaProperty` that this instance is being accessed from.
    nonisolated(unsafe) package var enclosing: (any SchemaProperty)?
    
    package struct Flags: OptionSet {
        /// A flag to include this property's mapped column as part of the SQL's `SELECT` and result columns.
        nonisolated package static let isSelected: Self = .init(rawValue: 1 << 0)
        /// A flag to include this property's referencing table as part of SQL's `SELECT` and result columns.
        nonisolated package static let prefetch: Self = .init(rawValue: 1 << 1)
        
        nonisolated package static let isExternal: Self = .init(rawValue: 1 << 2)
        
        nonisolated package static let isInherited: Self = .init(rawValue: 1 << 3)
        
        nonisolated package static let hasSubentities: Self = .init(rawValue: 1 << 4)
        
        nonisolated package static let isUnique: Self = .init(rawValue: 1 << 5)
        
        nonisolated package static let isOptional: Self = .init(rawValue: 1 << 6)
        
        nonisolated package let rawValue: UInt8
        
        nonisolated package init(rawValue: RawValue) {
            self.rawValue = rawValue
        }
    }
    
    /// Specifies whether this property's mapped column should be included in the SQL statement.
    ///
    /// The default flag is determined by the `SchemaProperty`.
    /// - `Schema.Attribute` defaults to `true`.
    /// - `Schema.Relationship` defaults to `true` if it's a to-one relationship.
    ///   - This provides the foreign key without additional queries.
    ///   - This does not mean it will be prefetched.
    /// - `Schema.Relationship` is `false` for to-many relationships.
    ///   - This cardinality has no foreign key column. Foreign keys are found that reference the owning-side.
    ///   - This should not be applicable.
    nonisolated package var isSelected: Bool {
        get { flags.contains(.isSelected) }
        mutating set {
            #if DEBUG
            if newValue, let relationship = self.metadata as? Schema.Relationship {
                assert(
                    relationship.isToOneRelationship,
                    "A to-many relationship has no foreign key column to select."
                )
            }
            #endif
            switch newValue {
            case true: flags.insert(.isSelected)
            case false: flags.remove(.isSelected)
            }
        }
    }
    
    /// The property value's metatype as described by `SchemaProperty`.
    nonisolated public var valueType: Any.Type {
        metadata.valueType
    }
    
    /// The attribute value's default value as provided in `Schema.Attribute`.
    nonisolated public var defaultValue: (any Sendable)? {
        assert(metadata is Schema.Attribute)
        if let attribute = (metadata as? Schema.Attribute)?.defaultValue,
           let defaultValue: (any Sendable)? = sendable(cast: attribute) {
            return defaultValue
        } else {
            return nil
        }
    }
    
    nonisolated public var column: String? {
        switch metadata {
        case let attribute as Schema.Attribute:
            attribute.name
        case let relationship as Schema.Relationship
            where relationship.isToOneRelationship: relationship.name + "_pk"
        default:
            nil
        }
    }
    
    nonisolated public var key: DataStoreSnapshotCodingKey {
        .modeledProperty(name)
    }
    
    nonisolated public var isUnique: Bool {
        flags.contains(.isUnique)
    }
    
    nonisolated public var isOptional: Bool {
        flags.contains(.isOptional)
    }
    
    // TODO: struct/enum raw values are implemented differently. Ensure consistency.
    
    nonisolated public var isRawRepresentable: Bool {
        valueType is any RawRepresentable.Type
    }
    
    /// Determines whether the entity's property is inherited from a superentity.
    nonisolated public var isInherited: Bool {
        flags.contains(.isInherited)
    }
    
    nonisolated public var hasSubentities: Bool {
        flags.contains(.hasSubentities)
    }
    
    /// Determines whether the entity's property is an attribute (inherited or composite).
    nonisolated public var isAttribute: Bool {
        guard metadata is Schema.Attribute else { return false }
        return reference == nil
    }
    
    /// Determines whether the entity's property is a composite attribute.
    nonisolated public var isCompositeAttribute: Bool {
        guard metadata is Schema.CompositeAttribute else { return false }
        return reference == nil && valueType is any Codable
    }
    
    /// Determines whether the entity's property is a relationship (any cardinality).
    nonisolated public var isRelationship: Bool {
        guard metadata is Schema.Relationship else { return false }
        return reference != nil
    }
    
    /// Determines whether the entity's property is a to-one relationship.
    nonisolated public var isToOneRelationship: Bool {
        guard metadata is Schema.Relationship else { return false }
        return reference?.count == 1
    }
    
    /// Determines whether the entity's property is a many-to-many relationship.
    nonisolated public var isManyToManyRelationship: Bool {
        guard metadata is Schema.Relationship else { return false }
        return reference?.count == 2
    }
    
    nonisolated public init(
        index: Int,
        name: String? = nil,
        keyPath: AnyKeyPath & Sendable,
        defaultValue: Any? = nil,
        metadata: any SchemaProperty,
        enclosing: (any SchemaProperty)? = nil,
        reference: [TableReference]? = nil
    ) {
        // Default value is unused.
        self.index = index
        self.name = name ?? metadata.name
        self.keyPath = keyPath
        self.metadata = metadata
        self.enclosing = enclosing
        self.reference = reference
        self.flags =
        (metadata is Schema.Attribute) ||
        (metadata as? Schema.Relationship)?.isToOneRelationship == true
        ? .isSelected : []
        if metadata.isUnique { flags.insert(.isUnique) }
        if metadata.isOptional { flags.insert(.isOptional) }
    }
    
    nonisolated internal init(
        index: Int,
        name: String? = nil,
        keyPath: AnyKeyPath & Sendable,
        defaultValue: Any? = nil,
        metadata: any SchemaProperty,
        enclosing: (any SchemaProperty)? = nil,
        reference: [TableReference]? = nil,
        flags: Flags? = nil
    ) {
        self.index = index
        self.name = name ?? metadata.name
        self.keyPath = keyPath
        self.metadata = metadata
        self.enclosing = enclosing
        self.reference = reference
        if let flags {
            self.flags = flags
        } else {
            self.flags =
            (metadata is Schema.Attribute) ||
            (metadata as? Schema.Relationship)?.isToOneRelationship == true
            ? .isSelected : []
            if metadata.isUnique { self.flags.insert(.isUnique) }
            if metadata.isOptional { self.flags.insert(.isOptional) }
        }
    }
    
    nonisolated internal func copy(
        index: Int? = nil,
        name: String? = nil,
        keyPath: (AnyKeyPath & Sendable)? = nil,
        defaultValue: Any? = nil,
        metadata: (any SchemaProperty)? = nil,
        enclosing: (any SchemaProperty)? = nil,
        reference: [TableReference]? = nil,
        flags: Flags? = nil
    ) -> Self {
        .init(
            index: index ?? self.index,
            name: name ?? self.name,
            keyPath: keyPath ?? self.keyPath,
            defaultValue: defaultValue ?? self.defaultValue,
            metadata: metadata ?? self.metadata,
            enclosing: enclosing ?? self.enclosing,
            reference: reference ?? self.reference,
            flags: flags ?? self.flags
        )
    }
    
    nonisolated public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.index == rhs.index &&
        lhs.name == rhs.name &&
        lhs.keyPath == rhs.keyPath
    }
    
    nonisolated public func hash(into hasher: inout Hasher) {
        hasher.combine(index)
        hasher.combine(name)
        hasher.combine(keyPath)
    }
    
    /// Creates a temporary `PropertyMetadata` that is not part of the model's schema.
    nonisolated public static func discriminator<T>(for type: T.Type) -> Self
    where T: PersistentModel {
        PropertyMetadata(
            index: -1,
            keyPath: \T.persistentModelID,
            metadata: Schema.Attribute(name: pk, valueType: T.self)
        )
    }
}

extension PropertyMetadata: CustomStringConvertible {
    nonisolated public var description: String {
        "\(index)-\(name) as \(valueType).self"
    }
}
