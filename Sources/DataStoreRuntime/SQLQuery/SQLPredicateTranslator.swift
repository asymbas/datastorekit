//
//  SQLPredicateTranslator.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Collections
import DataStoreCore
import DataStoreSQL
import DataStoreSupport
import Foundation
import Logging
import SQLiteHandle
import SQLiteStatement
import SwiftUI
import Synchronization

#if swift(>=6.2)
import SwiftData
#else
@preconcurrency import SwiftData
#endif

private typealias ForEach = SQLForEach

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.query")

@inline(__always) nonisolated internal func createAlias(
    _ key: PredicateExpressions.VariableID?,
    _ table: String
) -> String {
    key == nil ? table : "\(key.unsafelyUnwrapped)_\(table)"
}

extension SQLPredicateTranslator {
    nonisolated internal var isCachingPredicates: Bool {
        options.contains(.isCachingPredicates)
    }
    nonisolated internal var allowKeyPathVariantsForPropertyLookup: Bool {
        options.contains(.allowKeyPathVariantsForPropertyLookup)
    }
    
    nonisolated internal var useFallbackOnCompositeAttributes: Bool {
        options.contains(.useFallbackOnCompositeAttributes)
    }
    
    nonisolated internal var shouldMarkStartOfPredicateExpression: Bool {
        options.contains(.shouldMarkStartOfPredicateExpression)
    }
    
    nonisolated internal var shouldLogInformation: Bool {
        options.contains(.useVerboseLogging)
    }
}

// TODO: Learn how to increase precendence with parenthesis.
// TODO: Using `Hasher` may be unnecessary, try using the description for `Predicate`.
// FIXME: Unable to match to key paths due to generic and protocol constraints.
// FIXME: Unable to match to key paths related to inheritance.
// FIXME: Unable to access a relationship's attribute to sort without a predicate.
// FIXME: When the fragment returns mismatching entity at root, it has relationship destination issues.

/// An object that translates a `FetchDescriptor` into an SQL DQL statement.
public struct SQLPredicateTranslator<T>: ~Copyable, Sendable
where T: PersistentModel & SendableMetatype {
    /// The stable identity of the translator.
    nonisolated public var id: UUID = .init()
    /// Restricts logging for the translator.
    nonisolated internal var minimumLogLevel: Logger.Level
    /// Filters logging by PredicateExpressions where `nil` is has no filter or `[]` to hide all.
    nonisolated internal var tags: Set<String>?
    /// A debug tag for the current PredicateExressions nested type.
    nonisolated internal var tag: String?
    /// Accumulated logs for each access of `PredicateExpressions` nested types.
    nonisolated internal var nodes: [PredicateTree.Node] = []
    /// Cached `KeyPath` instances for loaded variables.
    nonisolated private var keyPaths: [AnyKeyPath & Sendable: PropertyMetadata] = [:]
    /// The schema from the store's configuration.
    nonisolated internal let schema: Schema
    /// The attachment assigned to the `DataStore`.
    nonisolated internal var attachment: (any DataStoreObservable)?
    /// Flags configured for this translator.
    nonisolated internal let options: SQLPredicateTranslatorOptions
    /// The SQL query to use and skip anything else.
    nonisolated internal var sqlQueryPassthrough: SQL?
    /// The current scope of a closure.
    nonisolated internal var key: PredicateExpressions.VariableID?
    /// The first occurrence of a closure.
    nonisolated internal var root: PredicateExpressions.VariableID?
    /// Tracks the recursive entries of nested predicate expressions.
    nonisolated internal var level: Int = 0
    /// Tracks the total of new predicate expressions made.
    nonisolated internal var counter: Int = 0
    /// The current entity in a `SELECT` clause.
    nonisolated internal var resultIndex: Int = 0
    /// Accumulates results to be hashed for caching purposes.
    nonisolated internal var hasher: Hasher = .init()
    /// Requested `PeristentIdentifier` instances given by SwiftData directly.
    nonisolated internal var requestedIdentifiers: Set<PersistentIdentifier>?
    /// Maps the `VariableID` to a type name in the closure.
    nonisolated internal var aliases: [PredicateExpressions.VariableID: String] = [:]
    /// Type names mapped to metatypes.
    nonisolated internal var types: [String: any SendableMetatype.Type] = [:]
    /// Manages dependencies of referenced columns.
    nonisolated internal var references: [PredicateExpressions.VariableID: OrderedSet<TableReference>] = [:]
    /// Adds related entities into the result set due to dependency requirements.
    nonisolated internal var implicitReferences: OrderedSet<TableReference> = []
    /// Tracks the number of all CTEs.
    nonisolated internal var cteIndex: Int = 0
    /// Used for debugging purposes to track what CTE is associated to a VariableID.
    nonisolated internal var ctesMap: [PredicateExpressions.VariableID: [CommonTableExpression]] = [:]
    /// All CTEs that will be inserted after walking through the predicate tree.
    nonisolated internal var ctes: [CommonTableExpression] = [] {
        didSet {
            if let key = self.path.last, let cte = self.ctes.last {
                ctesMap[key, default: []].append(cte)
            }
        }
    }
    /// A last-in-first-out stack that appends when a fragment is being evaluated (diverges). The LHS shifts to the next on append.
    nonisolated internal var path: [PredicateExpressions.VariableID] = [] {
        didSet(newValue) {}
    }
    
    /// Total number of `PersistentIdentifier` bindings.
    internal lazy var bindingsCount: Int? = {
        requestedIdentifiers?.count
    }()
    
    nonisolated public init(
        schema: Schema,
        attachment: (any DataStoreObservable)? = nil,
        options: consuming SQLPredicateTranslatorOptions,
        minimumLogLevel: consuming Logger.Level = .notice,
        tags: consuming Set<String>? = []
    ) {
        if false {
            options.insert(.preferStandardOutput)
        }
        if attachment != nil {
            options.insert(.useVerboseLogging)
            minimumLogLevel = .trace
        }
        if let values = SQLPredicateTranslatorOptions.tags {
            tags = values
            minimumLogLevel = .trace
        }
        if options.contains(.logAllPredicateExpressions) ||
            getEnvironmentValue(for: "PREDICATE_TRACE") == "TRUE" {
            options.insert(.useVerboseLogging)
            minimumLogLevel = .trace
            tags = nil
        }
        self.schema = schema
        self.attachment = attachment
        self.options = options
        self.minimumLogLevel = minimumLogLevel
        self.tags = tags
    }
    
    nonisolated public mutating func translate(
        _ descriptor: FetchDescriptor<T>,
        select: String? = nil
    ) throws -> SQLPredicateResult {
        _ = descriptor.includePendingChanges
        loadSchemaMetadata(for: T.self)
        var fragment = (descriptor.predicate?.expression as? SQLPredicateExpression)?.query(&self)
        if let passthrough = self.sqlQueryPassthrough {
            fragment = .init(clause: passthrough.sql, bindings: passthrough.bindings)
        }
        let clause = fragment?.clause
        let bindings = fragment?.bindings ?? []
        if let entity = fragment?.entity, entity.name != Schema.entityName(for: T.self) {
            log(.notice, "Fragment does not match the requested type: \(entity.name), \(T.self)")
        }
        var references = [self.root, self.key].reduce(into: OrderedSet<TableReference>()) {
            if let key = $1, let references = self.references[key].take() {
                $0.formUnion(references)
            }
        }
        guard let baseEntity = self.schema.entity(for: T.self) else {
            throw SchemaError.entityNotRegistered
        }
        let baseKey = self.root ?? self.key
        let baseAlias = createTableAlias(baseKey, baseEntity.name)
        let order = try translateSortDescriptors(
            key: baseKey,
            alias: baseAlias,
            entity: baseEntity,
            sortBy: descriptor.sortBy
        )
        let (selectedColumns, selectedProperties) = selectResultColumns(
            key: baseKey,
            alias: baseAlias,
            entity: baseEntity,
            type: T.self,
            propertiesToFetch: Set(descriptor
                .propertiesToFetch
                .lazy
                .compactMap(sendable(cast:))),
            relationshipKeyPathsForPrefetching: Set(descriptor
                .relationshipKeyPathsForPrefetching
                .lazy
                .compactMap(sendable(cast:)))
        )
        if !self.references.isEmpty {
            // FIXME: Handle non-consumed references, issue occurs on real devices.
            for (_, remaining) in self.references {
                references.formUnion(remaining)
            }
        }
        let statement = SQL {
            if options.contains(.explainQueryPlan) { "\nEXPLAIN QUERY PLAN\n" }
            if !ctes.isEmpty { With { SQLForEach(ctes) { $0 } } }
            Select(
                select == nil ? selectedColumns : [select.unsafelyUnwrapped],
                qualified: true
            )
            From(baseEntity.name, as: baseAlias)
            if !references.isEmpty {
                ForEach(references) {
                    let lhsAlias = $0.lhsAlias ?? $0.lhsTable
                    let rhsAlias = $0.rhsAlias ?? $0.rhsTable
                    """
                    JOIN "\($0.rhsTable)" AS "\(rhsAlias)"
                    ON "\(rhsAlias)"."\($0.rhsColumn)" = "\(lhsAlias)"."\($0.lhsColumn)"
                    """
                }
            }
            if !implicitReferences.isEmpty {
                ForEach(implicitReferences) {
                    Join.left(
                        $0.rhsTable,
                        as: $0.rhsAlias!,
                        on: ($0.lhsAlias, $0.lhsTable, $0.lhsColumn),
                        equals: ($0.rhsAlias, $0.rhsTable, $0.rhsColumn)
                    )
                }
            }
            if let clause, !clause.isEmpty {
                Where(clause, bindings: bindings.compactMap(sendable(cast:)))
            }
            if let order, !order.isEmpty {
                OrderBy(order)
            }
            if bindingsCount != nil || descriptor.fetchLimit != nil || descriptor.fetchOffset != nil {
                Limit(descriptor.fetchLimit ?? bindingsCount ?? -1)
                Offset(descriptor.fetchOffset ?? 0)
            }
        }
        #if DEBUG
        do {
            let placeholders = statement.sql.filter { $0 == "?" }.count
            let bindings = statement.bindings.count
            assert(
                placeholders == bindings,
                "Translation produced mismatched placeholders and bindings: \(placeholders) != \(bindings)"
            )
        }
        #endif
        if !self.references.isEmpty {
            print("References not fully consumed: \(references) \(self.references)")
        }
        if let predicate = descriptor.predicate {
            hasher.combine(predicate.description)
        }
        hasher.combine(baseEntity.name)
        hasher.combine(bindingsCount)
        hasher.combine(descriptor.fetchLimit)
        hasher.combine(descriptor.fetchOffset)
        let combinedHash = self.hasher.finalize()
        #if DEBUG
        if (true || minimumLogLevel <= .debug || tags == nil),
           requestedIdentifiers == nil && clause != nil,
           options.contains(.useVerboseLogging) {
            logger.log(
                level: .info,
                "\n“\(baseEntity.name)” translated predicate (\(combinedHash)):\n\(statement)"
            )
            fflush(stdout)
        }
        #endif
        if (shouldLogInformation || attachment != nil), let view = self.attachment {
            let statementArray = [statement.sql, "Bindings: \(statement.bindings)"]
            let joins = self.references
                .sorted { "\($0.key)" < "\($1.key)" }
                .map { "\($0.key): \($0.value)" }
            nodes.append(.init(
                key: baseKey,
                title: "Generated SQL",
                content: statementArray + (references.isEmpty ? [] : ["Remaining Joins: \(joins)"]),
                level: level,
                isComplete: true
            ))
            let id = self.id
            let nodes = self.nodes
            let bindingsCount = self.bindingsCount
            let sql = statement.sql
            let placeholdersCount = statement.sql.filter { $0 == "?" }.count
            Task { @DatabaseActor in
                let predicateString = descriptor.predicate.map { String(describing: $0) }
                let predicateHash = predicateString?.hashValue
                let translation = SQLPredicateTranslation(
                    id: id,
                    predicateDescription: predicateString,
                    predicateHash: predicateHash,
                    sql: sql,
                    placeholdersCount: placeholdersCount,
                    bindingsCount: bindingsCount,
                    tree: PredicateTree(id: id, path: nodes)
                )
                await MainActor.run { view.resolveTranslation(translation) }
            }
        }
        #if RELEASE
        precondition(path.isEmpty)
        #endif
        return .init(
            hash: combinedHash,
            statement: statement,
            properties: selectedProperties,
            requestedIdentifiers: requestedIdentifiers
        )
    }
    
    nonisolated internal mutating func node(
        atTerminal: Bool,
        in expression: Any.Type?,
        title: String,
        content: [String]
    ) {
        self.nodes.append(.init(
            path: path,
            key: key,
            expression: expression,
            title: title,
            content: content,
            level: level,
            isComplete: atTerminal
        ))
    }
    
    nonisolated internal mutating func log(
        _ type: Logger.Level?,
        _ message: @autoclosure () -> String,
        metadata: [Logger.Metadata.Key: Logger.MetadataValue]? = nil,
        function: String = #function
    ) {
        #if DEBUG
        log(as: type, input: message(), function: function)
        #endif
    }
    
    nonisolated internal mutating func log(
        as logLevel: Logger.Level?,
        input messages: Any...,
        metadata: [Logger.Metadata.Key: Logger.MetadataValue]? = nil,
        function: String = #function
    ) {
        // Skip logging that assumes SwiftData is resolving a fault.
        guard requestedIdentifiers == nil else {
            return
        }
        // Skip if no log level is specified and is below the minimum.
        guard let logLevel, shouldLogInformation || logLevel >= minimumLogLevel else {
            return
        }
        // Skip if a tag filter exists and the current tag is not in the allowed set.
        if let tagFilter = self.tags,
           let tag = self.tag, !tagFilter.contains(tag.lowercased()) {
            return
        }
        let output = messages.map(String.init(describing:)).joined(separator: " ")
        if shouldLogInformation || logLevel >= minimumLogLevel {
            let tag = self.tag ?? "<nil>"
            if options.contains(.preferStandardOutput) {
                let position = "[Predicate #\(counter) @ lvl-\(level)]"
                let context = "\(tag).\(function)"
                print(
                    "\(logLevel) \(position) \(context) \(output)",
                    "metadata: \(metadata)"
                )
            } else {
                var metadata = metadata ?? .init()
                metadata["position"] = "predicate #\(counter) (level \(level))"
                metadata["context"] = "\(tag).\(function)"
                if !path.isEmpty { metadata["path"] = .array(path.map { "\($0)" }) }
                logger.log(level: logLevel, "\(output)", metadata: metadata)
            }
        }
    }
    
    /// Disambiguates the names of result columns.
    nonisolated internal mutating func selectResultColumns<Model>(
        key: PredicateExpressions.VariableID?,
        alias: String,
        entity: Schema.Entity,
        type: Model.Type,
        propertiesToFetch: Set<AnyKeyPath & Sendable> = [],
        relationshipKeyPathsForPrefetching: Set<AnyKeyPath & Sendable> = [],
        includeJoins: Bool = true
    ) -> (columns: [String], properties: [PropertyMetadata]) where Model: PersistentModel {
        hasher.combine(propertiesToFetch)
        hasher.combine(relationshipKeyPathsForPrefetching)
        var columns = [String]()
        var foreignKeyColumns = [PropertyMetadata]()
        let entityAlias = alias
        loadSchemaMetadata(for: Model.self)
        guard var primaryKeyColumn = self.keyPaths[\Model.persistentModelID] else {
            fatalError("Primary key was not registered in context: \(type)")
        }
        let schemaMetadata = type.databaseSchemaMetadata
        primaryKeyColumn.index = self.resultIndex
        columns.append(clause(entityAlias, primaryKeyColumn.name))
        var properties = [PropertyMetadata]()
        properties.reserveCapacity(schemaMetadata.count + 1)
        properties.append(primaryKeyColumn)
        for var property in schemaMetadata {
            defer { properties.append(property) }
            switch property.metadata {
            case is Schema.Attribute:
                if !propertiesToFetch.isEmpty && !propertiesToFetch.contains(property.keyPath) {
                    property.isSelected = false
                } else {
                    columns.append(clause(entityAlias, property.name))
                }
            case let relationship as Schema.Relationship:
                if relationshipKeyPathsForPrefetching.contains(property.keyPath) {
                    property.flags.insert(.prefetch)
                }
                if relationship.isToOneRelationship {
                    columns.append(clause(entityAlias, relationship.name + "_pk"))
                    foreignKeyColumns.append(property)
                }
            default:
                fatalError()
            }
        }
        guard includeJoins else {
            return (columns, properties)
        }
        if let key, let references = self.references[key], !references.isEmpty {
            for (index, reference) in references.enumerated() {
                guard let property = foreignKeyColumns.first(where: {
                    reference.sourceColumn.hasPrefix($0.name)
                }) else {
                    continue
                }
                guard !relationshipKeyPathsForPrefetching.contains(property.keyPath) else {
                    self.references[key]?.remove(at: index)
                    log(.debug, "Removing join for prefetching: \(reference)")
                    continue
                }
                guard let destinationAlias = reference.destinationAlias,
                      let joinEntity = self.schema.entitiesByName[reference.destinationTable] else {
                    log(.trace, "Unknown entity referenced in JOIN if not intermediary: \(reference)")
                    continue
                }
                guard let entityType = (self.types[joinEntity.name] ?? joinEntity.type),
                      let type = entityType as? any PersistentModel.Type else {
                    fatalError(SwiftDataError.modelValidationFailure.localizedDescription)
                }
                self.resultIndex += 1
                log(.debug, "Selecting columns from JOIN reference: \(joinEntity.name)")
                let (resultColumns, resultProperties) = selectResultColumns(
                    key: key,
                    alias: destinationAlias,
                    entity: joinEntity,
                    type: type,
                    includeJoins: false
                )
                columns += consume resultColumns
                properties += consume resultProperties
            }
        }
        for keyPath in relationshipKeyPathsForPrefetching {
            log(.debug, "Processing key path for prefetching: \(keyPath)")
            guard let property = self.keyPaths[keyPath] else {
                log(.warning, "KeyPath for prefetching not found: \(keyPath)")
                continue
            }
            guard let relationship = property.metadata as? Schema.Relationship else {
                fatalError("Expected property metadata to reference a Schema.Relationship.")
            }
            guard let destinationEntity = self.schema.entitiesByName[relationship.destination] else {
                fatalError(SwiftDataError.unknownSchema.localizedDescription)
            }
            if relationship.isToOneRelationship,
               let type = unwrapOptionalMetatype(relationship.valueType) as? any PersistentModel.Type {
                self.resultIndex += 1
                let destinationAlias = createTableAlias(key, destinationEntity.name)
                implicitReferences.append(.init(
                    sourceAlias: entityAlias,
                    sourceTable: entity.name,
                    sourceColumn: relationship.self.name + "_pk",
                    destinationAlias: destinationAlias,
                    destinationTable: destinationEntity.name,
                    destinationColumn: pk
                ))
                let (resultColumns, resultProperties) = selectResultColumns(
                    key: key,
                    alias: destinationAlias,
                    entity: destinationEntity,
                    type: type,
                    includeJoins: false
                )
                columns += consume resultColumns
                properties += consume resultProperties
            }
        }
        /// Formats the previously aliased table-column pairs to a valid alias in the `SELECT` clause.
        func clause(_ table: String, _ column: String) -> String {
            "\(quote(table)).\(quote(column)) AS \(quote("\(table).\(column)"))"
        }
        return (columns, properties)
    }
    
    /// - SwiftData always includes the `PersistentIdentifier` with the `SortDescriptor` as the default.
    nonisolated private mutating func translateSortDescriptors<Model>(
        key: PredicateExpressions.VariableID?,
        alias: String,
        entity: Schema.Entity,
        sortBy descriptors: [SortDescriptor<Model>],
        randomize: Bool = false,
        excludeDefaultPrimaryKeySorting: Bool = false
    ) throws -> [SQLSort]? where Model: PersistentModel & SendableMetatype {
        guard !randomize else { return [.random] }
        return try descriptors.compactMap { descriptor -> SQLSort? in
            guard let keyPath = descriptor.keyPath,
                  let keyPath: (PartialKeyPath<Model> & Sendable) = sendable(cast: keyPath) else {
                throw SwiftDataError.unsupportedSortDescriptor
            }
            defer {
                hasher.combine(descriptor.order)
                hasher.combine(descriptor.keyPath)
            }
            guard let property = try self[keyPath] else {
                if let relationship = entity.relationships.first(where: { $0.keypath == keyPath }) {
                    let qualifiedName = "\(quote(alias)).\(quote(relationship.name + "_pk"))"
                    hasher.combine(relationship.name + "_pk")
                    log(.notice, "SortDescriptor<\(Model.self)> resolved using schema as fallback: \(qualifiedName)")
                    return .init(
                        qualifiedName,
                        isOptional: relationship.isOptional,
                        order: descriptor.order
                    )
                } else {
                    #if DEBUG
                    for property in Model.databaseSchemaMetadata where property.metadata is Schema.Relationship {
                        if let _ = unwrapOptionalMetatype(property.valueType) as? any PersistentModel.Type {
                            logger.debug("Found a relationship keypath (\(keyPath)): \(property)")
                        } else {
                            logger.debug("Unable to infer model type for relationship: \(property)")
                        }
                    }
                    #endif
                    log(.notice, "Omitting SortDescriptor<\(Model.self)> that failed to resolve: \(keyPath)")
                    return nil
                }
            }
            let clause: String
            defer { hasher.combine(property.name + "_pk") }
            switch property.metadata {
            case is Schema.Attribute where property.enclosing is Schema.Relationship:
                guard let relationship = property.enclosing as? Schema.Relationship,
                      let type = relationship.valueType as? any PersistentModel.Type else {
                    log(.warning, "Missing accessible relationship for attribute: \(property)")
                    return nil
                }
                let destinationAlias = createTableAlias(key, relationship.destination)
                clause = "\(quote(destinationAlias)).\(quote(property.name))"
                loadSchemaMetadata(for: type, key: key)
                let reference = TableReference(
                    sourceAlias: alias,
                    sourceTable: entity.name,
                    sourceColumn: relationship.name + "_pk",
                    destinationAlias: destinationAlias,
                    destinationTable: relationship.destination,
                    destinationColumn: pk
                )
                if let key {
                    self.references[key, default: []].append(reference)
                    log(.debug, "Inserted JOIN clause reference (SortDescriptor): \(reference)")
                } else {
                    implicitReferences.append(reference)
                }
            case is Schema.Attribute where property.enclosing is Schema.CompositeAttribute:
                guard let composite = property.enclosing as? Schema.CompositeAttribute else {
                    log(.warning, "Missing accessible composite attribute for attribute: \(property)")
                    return nil
                }
                guard !(unwrapOptionalMetatype(composite.valueType) is any RawRepresentable.Type) else {
                    log(.trace, "Composite attribute conforms to RawRepresentable: \(property) \(keyPath)")
                    clause = "\(quote(alias)).\(quote(property.enclosing!.name))"
                    break
                }
                let qualifiedName = "\(quote(alias)).\(quote(composite.name))"
                clause = useFallbackOnCompositeAttributes
                ?
                    """
                    COALESCE (
                        json_extract(CAST(\(qualifiedName) AS TEXT), '$."\(property.name)"'),
                        json_extract(CAST(\(qualifiedName) AS TEXT), '$')
                    )
                    """
                : composite.valueType is (any ExpressibleByArrayLiteral.Type) == false
                ? "json_extract(\(qualifiedName), '$.\(quote(property.name))')"
                : "json_extract(\(qualifiedName), '$')"
            default:
                if excludeDefaultPrimaryKeySorting, property.name == pk {
                    log(.debug, "Removed default SortDescriptor: \(property) \(keyPath)")
                    return nil
                }
                clause = "\(quote(alias)).\(quote(property.name))"
            }
            return .init(
                clause,
                isOptional: property.metadata.isOptional,
                order: descriptor.order
            )
        }
    }
    
    /// Parses the description of a key path to gather enough metadata to resolve for a `PropertyMetadata`.
    nonisolated private mutating
    func parseKeyPathForProperty<Model>(_ keyPath: PartialKeyPath<Model> & Sendable)
    throws -> PropertyMetadata? where Model: PersistentModel {
        guard !options.contains(.disableKeyPathPropertyLookupFallbacks) else {
            return nil
        }
        guard let metadata = KeyPathDescription.parse(keyPath: keyPath) else {
            log(.warning, "Failed to parse key path metadata: \(keyPath)")
            return nil
        }
        log(.trace, "Parsed KeyPath metadata: \(keyPath) -> \(metadata)")
        guard let rootEntity = self.schema.entitiesByName[metadata.rootType] else {
            log(.warning, "Parsed key path metadata did not match to any entity: \(metadata.rootType)")
            return nil
        }
        let rootTable = rootEntity.name
        var currentTable = rootTable
        var currentColumn: String?
        var currentProperty: (any SchemaProperty)?
        var currentProperties = rootEntity.storedPropertiesByName
        var pathSegments = [String]()
        for component in metadata.components {
            log(nil, "Processing key path metadata component: \(component)")
            if component.isComputed, let typeName = component.unwrappedType {
                if component.isArray {
                    log(.error, "Traversing arrays for key path metadata is not supported.")
                    throw DataStoreError.unsupportedFeature
                }
                if let destinationEntity = self.schema.entitiesByName[typeName] {
                    log(nil, "Entering destination entity: \(destinationEntity.name)")
                    currentTable = destinationEntity.name
                    currentProperties = destinationEntity.storedPropertiesByName
                    currentProperty = nil
                    currentColumn = nil
                    pathSegments = []
                    continue
                }
                if let compositeType = component.computedType,
                   let composite = currentProperties.values
                    .compactMap({
                        $0 as? Schema.CompositeAttribute
                    })
                        .first(where: {
                            let attributeType = String(describing: $0.valueType)
                            return attributeType == compositeType ||
                            attributeType.hasSuffix(".\(compositeType)")
                        })
                {
                    log(nil, "Entering composite attribute: \(composite.name)")
                    pathSegments = []
                    currentColumn = composite.name
                    currentProperty = composite
                    currentProperties = .init(uniqueKeysWithValues: composite.properties.map {
                        ($0.name, $0)
                    })
                    continue
                }
                log(nil, "Key path metadata component could not be resolved: \(typeName)")
                return nil
            }
            guard let name = component.property, let property = currentProperties[name] else {
                log(.warning, "Property not found using key path metadata: \(currentColumn ?? "nil")")
                return nil
            }
            let qualifiedName: String
            if let intermediateColumn = currentColumn {
                qualifiedName = "\(quote(rootTable)).\(quote(intermediateColumn)).\(quote(name))"
            } else {
                qualifiedName = "\(quote(currentTable)).\(quote(name))"
            }
            switch property {
            case let relationship as Schema.Relationship:
                log(nil, "Entering relationship: \(qualifiedName) -> \(relationship.destination)")
                currentTable = relationship.destination
                currentColumn = relationship.name
                currentProperty = relationship
                currentProperties = self.schema.entitiesByName[currentTable]?.storedPropertiesByName ?? [:]
                pathSegments = []
                continue
            case let composite as Schema.CompositeAttribute:
                log(nil, "Entering composite attribute: \(qualifiedName)")
                if currentColumn == nil {
                    currentColumn = name
                    currentProperty = composite
                    pathSegments = []
                } else {
                    pathSegments.append(name)
                }
                currentProperties = .init(uniqueKeysWithValues: composite.properties.map { ($0.name, $0) })
                continue
            case let attribute as Schema.Attribute where currentProperty is Schema.Relationship:
                guard let relationship = currentProperty as? Schema.Relationship,
                      let type = relationship.valueType as? any (PersistentModel & SendableMetatype).Type else {
                    log(.warning, "Cannot extract metadata from relationship: \(keyPath)")
                    continue
                }
                loadSchemaMetadata(for: type)
                switch type.databaseSchemaMetadata.first(where: { $0.name == attribute.name }) {
                case var property?:
                    property.enclosing = relationship
                    property.keyPath = keyPath
                    self.keyPaths[keyPath] = property
                    log(nil, "Caching extracted relationship metadata: \(keyPath) -> \(property)")
                    return property
                case nil:
                    return nil
                }
            case let attribute as Schema.Attribute where currentProperty is Schema.CompositeAttribute:
                guard let jsonColumn = currentColumn,
                      let composite = currentProperty as? Schema.CompositeAttribute,
                      let index = composite.properties.firstIndex(of: attribute) else {
                    log(.warning, "Cannot extract metadata from composite attribute: \(keyPath)")
                    continue
                }
                pathSegments.append(name)
                let jsonPath = pathSegments.joined(separator: ".")
                log(nil, "Final JSON extract: \(qualifiedName) -> \(jsonColumn).\(jsonPath)")
                let property = PropertyMetadata(
                    index: index,
                    name: jsonPath,
                    keyPath: keyPath,
                    metadata: attribute,
                    enclosing: composite
                )
                self.keyPaths[keyPath] = property
                log(.trace, "Caching extracted composite attribute metadata: \(keyPath) -> \(property)")
                return property
            case let attribute as Schema.Attribute:
                log(nil, "Entering attribute: \(qualifiedName)")
                currentColumn = attribute.name
                currentProperty = attribute
                pathSegments = []
                continue
            default:
                let qualifiedName = "\(quote(currentTable)).\(quote(name))"
                log(.warning, "Final column access: \(qualifiedName)")
            }
        }
        if let metadata = currentProperty,
           let type = self.types[currentTable] as? any (PersistentModel & SendableMetatype).Type,
           let property = type.databaseSchemaMetadata.first(where: { $0.name == metadata.name }) {
            log(.notice, "Resorting to slowest path lookup for PropertyMetadata: \(currentTable).\(property.name)")
            #if DEBUG
            print(
                """
                Affected key path: \(keyPath)
                The parsed key path could not match to any PropertyMetadata in the schema.
                It might be present with an identical description with mismatching Equatable/Hashable.
                Using a protocol or generic constraint can affect matching to a key path.
                Consider constraining to only PersistentModel when possible.
                """
            )
            #endif
            return property
        }
        log(.warning, "Traversed entire path but found no terminal value: \(keyPath)")
        return nil
    }
    
    nonisolated internal mutating func bridgeAsRelationship<Base, Root, Value>(
        _ baseType: Base.Type,
        from lhsKeyPath: AnyKeyPath & Sendable,
        to rhsKeyPath: any KeyPath<Root, Value> & Sendable
    ) -> (
        entity: Schema.Entity,
        property: PropertyMetadata,
        keyPath: AnyKeyPath & Sendable
    )? where Base: PersistentModel {
        assert(
            lhsKeyPath is PartialKeyPath<Base>,
            "\(lhsKeyPath) LHS key path root type is not \(Base.self)."
        )
        let description = "\(Base.self)-\(Root.self).\(Value.self).self \(lhsKeyPath) -> \(rhsKeyPath)"
        log(.trace, "Bridging relationship: \(description)")
        guard let fullKeyPath = appendKeyPath(from: lhsKeyPath, to: rhsKeyPath) else {
            log(.warning, "Key path could not be appended: \(description)")
            return nil
        }
        var type = unwrapOptionalMetatype(Root.self)
        if let relationshipType = type as? any RelationshipCollection.Type {
            type = unwrapArrayMetatype(relationshipType)
        }
        guard let rhsType = type as? any (PersistentModel & SendableMetatype).Type,
              let rhsEntity = self.schema.entity(for: rhsType) else {
            fatalError(SwiftDataError.unknownSchema.localizedDescription)
        }
        guard let lhsKeyPath: PartialKeyPath<Base> & Sendable = sendable(cast: lhsKeyPath),
              let lhsProperty = Base.schemaMetadata(for: lhsKeyPath) else {
            log(.warning, "No metadata found for LHS model: \(description)")
            return nil
        }
        guard var rhsProperty = rhsType.schemaMetadata(for: rhsKeyPath) else {
            log(.warning, "No metadata found for RHS relationship: \(description)")
            return nil
        }
        if allowKeyPathVariantsForPropertyLookup == true {
            rhsType.addKeyPathVariantToPropertyMetadata(fullKeyPath, for: rhsProperty)
        }
        if types[Schema.entityName(for: rhsType)] == nil {
            log(.warning, "Entity was not loaded to allow for key path bridging: \(rhsType)")
            loadSchemaMetadata(for: rhsType)
        }
        rhsProperty.enclosing = lhsProperty.metadata
        rhsProperty.keyPath = fullKeyPath
        log(.debug, "Key path bridged relationship: \(lhsProperty.name) + \(rhsProperty.name)")
        self.keyPaths[fullKeyPath] = rhsProperty
        return (rhsEntity, rhsProperty, fullKeyPath)
    }
    
    nonisolated internal mutating func bridgeAsCompositeAttribute<Root, Value>(
        from lhsKeyPath: AnyKeyPath & Sendable,
        to rhsKeyPath: any KeyPath<Root, Value> & Sendable
    ) -> (property: PropertyMetadata, keyPath: AnyKeyPath & Sendable)? {
        let description = "\(T.self)-\(Root.self).\(Value.self).self \(lhsKeyPath) -> \(rhsKeyPath)"
        log(.trace, "Bridging composite attribute: \(description)")
        guard let fullKeyPath = appendKeyPath(from: lhsKeyPath, to: rhsKeyPath) else {
            log(.warning, "Key path could not be appended: \(description)")
            return nil
        }
        let components = String(describing: rhsKeyPath).split(separator: ".").map(String.init)
        guard let lhsKeyPath: PartialKeyPath<T> & Sendable = sendable(cast: lhsKeyPath),
              let property = T.schemaMetadata(for: lhsKeyPath),
              let composite = property.metadata as? Schema.CompositeAttribute else {
            log(.warning, "No metadata found for LHS composite attribute: \(description)")
            return nil
        }
        guard composite.valueType is Root.Type else {
            fatalError("Root type of key path does not match the type in the schema: \(description)")
        }
        guard composite.valueType is any RawRepresentable.Type else {
            fatalError("Root type of key path does not match the type in the schema: \(description)")
        }
        guard !composite.properties.isEmpty else {
            fatalError("Sub-attributes not found for composite attribute: \(description)")
        }
        guard !components.isEmpty else {
            fatalError("Unable to extract sub-attribute from composite attribute: \(description)")
        }
        let subAttributeIndex: Int
        switch composite.properties.firstIndex(where: { components.contains($0.name) }) {
        case let index?:
            subAttributeIndex = index
            log(.debug, "Composite attribute inferred as struct-based value type: \(description)")
        case nil where composite.properties.count == 1:
            subAttributeIndex = 0
            log(.debug, "Composite attribute inferred as enum-based value type: \(description)")
        default:
            log(.warning, "No metadata found for RHS attribute: \(description)",
                metadata: [
                    "composite_attribute": "\(composite.properties.map(\.name))",
                    "components": "\(components)"
                ]
            )
            return nil
        }
        let attribute = composite.properties[subAttributeIndex]
        guard attribute.valueType is Value.Type else {
            fatalError("Value type of key path does not match the type in the schema: \(description)")
        }
        let subProperty = PropertyMetadata(
            index: subAttributeIndex,
            name: attribute.name,
            keyPath: fullKeyPath,
            metadata: attribute,
            enclosing: composite
        )
        log(.debug, "Key path bridged composite attribute: \(property.name) + \(subProperty.name)")
        self.keyPaths[fullKeyPath] = subProperty
        return (subProperty, fullKeyPath)
    }
}

extension SQLPredicateTranslator {
    /// Concatenates the mapped `VariableID` types to represent nested access in `path`.
    nonisolated internal mutating func createTableAlias(
        _ key: PredicateExpressions.VariableID?,
        _ table: String
    ) -> String {
        let parts = (path.compactMap { aliases[$0] } + [table]).joined(separator: "_")
        let alias = createAlias(key, parts)
        log(.debug, "Creating path chain: \(alias)")
        if let key, !aliases.keys.contains(key) { self.aliases[key] = table }
        return alias
    }
    
    /// Registers the model and prepares a discriminator with its schema metadata.
    ///
    /// - Parameters:
    ///   - type: The model type that conforms to `PersistentModel`.
    ///   - variableID: The current variable scope.
    nonisolated internal mutating func loadSchemaMetadata<Model>(
        for type: Model.Type,
        key variableID: PredicateExpressions.VariableID? = nil
    ) where Model: PersistentModel & SendableMetatype {
        let identifier = Schema.entityName(for: type)
        if types[identifier] == nil {
            self.types[identifier] = type
            let property = PropertyMetadata(
                index: -1,
                keyPath: \Model.persistentModelID,
                metadata: Schema.Attribute(
                    name: pk,
                    valueType: Model.self,
                    defaultValue: schema.entity(for: type)
                )
            )
            self.keyPaths[property.keyPath] = property
            self.keyPaths.merge(type.schemaMetadataByKeyPath()) { existing, _ in existing }
        }
        guard let variableID,
              let currentEntity = self.schema.entity(for: type),
              let superentity = currentEntity.superentity,
              let superType = self.types[superentity.name] ?? Schema.type(for: superentity.name),
              let superType = superType as? any PersistentModel.Type else {
            return
        }
        loadSchemaMetadata(for: superType, key: variableID)
        self.references[variableID, default: []].append(TableReference(
            sourceAlias: createTableAlias(variableID, currentEntity.name),
            sourceTable: identifier,
            sourceColumn: pk,
            destinationAlias: createTableAlias(variableID, superentity.name),
            destinationTable: superentity.name,
            destinationColumn: pk
        ))
    }
}

extension SQLPredicateTranslator {
    /// - Important:
    ///   Protocols and generics can affect how key paths can be matched.
    nonisolated fileprivate mutating
    func getProperty<Variable>(at keyPath: PartialKeyPath<Variable> & Sendable)
    throws -> PropertyMetadata? where Variable: PersistentModel & SendableMetatype {
        guard let property = self.keyPaths[keyPath] else {
            if let superclass = class_getSuperclass(Variable.self) as? any PersistentModel.Type {
                loadSchemaMetadata(for: superclass, key: key)
                return try Variable.schemaMetadata(for: keyPath)
                ?? parseKeyPathForProperty(keyPath)
                ?? lookupPropertyMetadata(
                    superclass: superclass,
                    subclass: Variable.self,
                    keyPath: keyPath
                )
            } else {
                loadSchemaMetadata(for: Variable.self, key: key)
                return try Variable.schemaMetadata(for: keyPath)
                ?? parseKeyPathForProperty(keyPath)
            }
        }
        return property
    }
    
    // FIXME: Unable to cast key paths on inherited properties.
    
    nonisolated private func lookupPropertyMetadata<Super, Sub>(
        superclass: Super.Type,
        subclass: Sub.Type,
        keyPath: PartialKeyPath<Sub> & Sendable
    ) -> PropertyMetadata? where Super: PersistentModel, Sub: PersistentModel {
        #if swift(>=6.2)
        guard #available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *) else {
            return nil
        }
        #endif
        return nil
    }
    
    nonisolated internal subscript<Variable>(keyPath: PartialKeyPath<Variable> & Sendable)
    -> PropertyMetadata? where Variable: PersistentModel & SendableMetatype {
        mutating get throws { try getProperty(at: keyPath) }
    }
    
    nonisolated internal subscript<Variable>(
        keyPath: AnyKeyPath & Sendable,
        type: Variable.Type
    ) -> PropertyMetadata? where Variable: PersistentModel & SendableMetatype {
        mutating get throws {
            guard let keyPath = keyPath as? PartialKeyPath<Variable>,
                  let keyPath: (PartialKeyPath<Variable> & Sendable) = sendable(cast: keyPath) else {
                return nil
            }
            return try getProperty(at: keyPath)
        }
    }
    
    nonisolated internal subscript<Model>(type: Model.Type) -> [PropertyMetadata]
    where Model: PersistentModel & SendableMetatype {
        mutating get { Model.databaseSchemaMetadata }
    }
}

nonisolated internal func appendKeyPath<Root, Value>(
    from lhsKeyPath: AnyKeyPath & Sendable,
    to rhsKeyPath: any KeyPath<Root, Value> & Sendable
) -> (AnyKeyPath & Sendable)? {
    let lhsKeyPath = lhsKeyPath as AnyKeyPath
    guard let keyPath = lhsKeyPath.appending(path: rhsKeyPath) else {
        return nil
    }
    guard let keyPath: (AnyKeyPath & Sendable) = sendable(cast: keyPath) else {
        return nil
    }
    return keyPath
}

nonisolated internal func appendKeyPath<LHSRoot, LHSValue, RHSValue>(
    _ lhsKeyPath: KeyPath<LHSRoot, LHSValue>,
    _ rhsKeyPath: KeyPath<LHSValue, RHSValue>
) -> KeyPath<LHSRoot, RHSValue> {
    lhsKeyPath.appending(path: rhsKeyPath)
}

nonisolated internal func compose<Root, Wrapped, Value>(
    _ base: KeyPath<Root, Wrapped?>,
    _ next: KeyPath<Wrapped, Value>
) -> (Root) -> Value? {
    { root in root[keyPath: base].map { $0[keyPath: next] } }
}
