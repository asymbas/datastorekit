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
import Synchronization

#if swift(>=6.2)
import SwiftData
#else
@preconcurrency import SwiftData
#endif

private typealias ForEach = SQLForEach

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.query")

// FIXME: Unable to match to key paths due to generic and protocol constraints.
// FIXME: Unable to match to key paths related to inheritance.
// FIXME: Unable to access a relationship's attribute to sort without a predicate.
// FIXME: When the fragment returns mismatching entity at root, it has relationship destination issues.

/// A type that translates a `FetchDescriptor<T>` into an SQL statement object.
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
    /// The last error set to this instance that may have occurred while translating.
    nonisolated internal var error: Self.Error?
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
    nonisolated internal var sqlPassthrough: SQL?
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
            #if DEBUG
            if let key = self.path.last, let cte = self.ctes.last {
                ctesMap[key, default: []].append(cte)
            }
            #endif
        }
    }
    /// A last-in-first-out stack that appends when a fragment is being evaluated (diverges). The LHS shifts to the next on append.
    nonisolated internal var path: [PredicateExpressions.VariableID] = []
    
    /// Total number of `PersistentIdentifier` bindings.
    internal lazy var bindingsCount: Int? = {
        requestedIdentifiers?.count
    }()
    
    package var evaluatedSnapshots: [PersistentIdentifier: any DataStoreSnapshot]?
    
    package var evaluateEphemeralProperty:
    @Sendable (EphemeralPropertyEvaluate) throws -> [PersistentIdentifier: any DataStoreSnapshot]? = { _ in nil }
    
    package var editingState: (any EditingStateProviding)?
    
    nonisolated public init(
        schema: Schema,
        editingState: (any EditingStateProviding)? = nil,
        attachment: (any DataStoreObservable)? = nil,
        options: consuming SQLPredicateTranslatorOptions = [],
        minimumLogLevel: consuming Logger.Level = .notice,
        tags: consuming Set<String>? = []
    ) {
        #if DEBUG
        if let values = SQLPredicateTranslatorOptions.tags {
            tags = values
            minimumLogLevel = .trace
        }
        if attachment != nil, options.insert(.useVerboseLogging).inserted {
            minimumLogLevel = .trace
        }
        if options.contains(.logAllPredicateExpressions) ||
            getEnvironmentValue(for: "PREDICATE_TRACE") == "TRUE" {
            options.insert(.useVerboseLogging)
            minimumLogLevel = .trace
            tags = nil
        }
        #endif
        self.schema = schema
        self.editingState = editingState
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
        if let passthrough = self.sqlPassthrough {
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
            propertiesToFetch: Set(descriptor.propertiesToFetch.lazy.compactMap(sendable(cast:))),
            relationshipKeyPathsForPrefetching: Set(descriptor.relationshipKeyPathsForPrefetching.lazy.compactMap(sendable(cast:)))
        )
        if !self.references.isEmpty {
            // FIXME: Handle non-consumed references, issue occurs on real devices.
            for (_, remaining) in self.references {
                references.formUnion(remaining)
            }
        }
        let emittedJoinAliases: Set<String> = references.reduce(into: []) {
            $0.insert($1.rhsAlias ?? $1.rhsTable)
        }
        let dedupedImplicitReferences = self.implicitReferences.filter {
            !emittedJoinAliases.contains($0.rhsAlias ?? $0.rhsTable)
        }
        let statement = SQL {
            if options.contains(.explainQueryPlan) { "\nEXPLAIN QUERY PLAN\n" }
            if !ctes.isEmpty { With { SQLForEach(ctes) { $0 } } }
            Select(select == nil ? selectedColumns : [select.unsafelyUnwrapped], qualified: true)
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
            if !dedupedImplicitReferences.isEmpty {
                ForEach(dedupedImplicitReferences) {
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
            assert(placeholders == bindings,
                "Translation produced mismatched placeholders and bindings: \(placeholders) != \(bindings)"
            )
        }
        #endif
        if !self.references.isEmpty {
            logger.notice("References not fully consumed: \(references) \(self.references)")
        }
        if let predicate = descriptor.predicate { hasher.combine(predicate.description) }
        hasher.combine(baseEntity.name)
        hasher.combine(bindingsCount)
        hasher.combine(descriptor.fetchLimit)
        hasher.combine(descriptor.fetchOffset)
        let combinedHash = self.hasher.finalize()
        #if DEBUG
        if (minimumLogLevel <= .debug || tags == nil),
           requestedIdentifiers == nil && clause != nil,
           options.contains(.useVerboseLogging)
            || options.contains(.logAllPredicateExpressions)
            || options.contains(.generateSQLStatement) {
            logger.log(level: .info, "\n“\(baseEntity.name)” translated predicate (\(combinedHash)):\n\(statement)")
            fflush(stdout)
        }
        #endif
        if (options.contains(.useVerboseLogging) || attachment != nil), let view = self.attachment {
            let statementArray = [statement.sql, "Bindings: \(statement.bindings)"]
            let joins = self.references.sorted { "\($0.key)" < "\($1.key)" }.map { "\($0.key): \($0.value)" }
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
                let predicateHash = predicateString.map {
                    var hasher = Hasher()
                    hasher.combine($0)
                    return hasher.finalize()
                }
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
        if let error = self.error {
            throw error
        }
        return .init(
            hash: combinedHash,
            statement: statement,
            properties: selectedProperties,
            requestedIdentifiers: requestedIdentifiers
        )
    }
    
    nonisolated public mutating func translate(
        _ request: some FetchRequest<T>,
        select: String? = nil
    ) throws -> SQLPredicateResult {
        self.editingState = request.editingState
        return try translate(request.descriptor, select: select)
    }
    
    nonisolated public mutating func sql(
        _ descriptor: FetchDescriptor<T>,
        select: String? = nil
    ) throws -> String {
        let result = try translate(descriptor, select: select)
        var sql = result.statement.sql
        for binding in result.statement.bindings.reversed() {
            guard let range = sql.range(of: "?", options: .backwards) else { break }
            let literal = (binding as? SQLValue)?.sql ?? "'\(binding)'"
            sql.replaceSubrange(range, with: literal)
        }
        return sql
    }
    
    nonisolated public struct EphemeralPropertyEvaluate: Sendable {
        nonisolated public let editingState: any EditingStateProviding
        nonisolated public let entityName: String
        nonisolated public let propertyIndex: Int
        nonisolated public let value: any Sendable
    }
    
    nonisolated public enum Error: Swift.Error {
        /// Missing in-memory models for ephemeral property evaluations.
        case cannotEvaluateEphemeralProperties
        case invalidTranslation(String)
    }
}

extension SQLPredicateTranslator {
    internal mutating func translateEphemeralEquality(
        lhs: consuming SQLPredicateFragment,
        rhsValue: any Sendable
    ) throws -> SQLPredicateFragment? {
        guard let editingState = self.editingState,
              let entity = lhs.entity,
              let alias = lhs.alias,
              let property = lhs.property,
              let attribute = property.metadata as? Schema.Attribute,
              attribute.options.contains(.ephemeral) else {
            return nil
        }
        let comparisonValue: any Sendable = (rhsValue as? SQLValue)?.base ?? rhsValue
        assert(comparisonValue is SQLValue == false, "Ephemeral equality should not compare to SQLValue")
        guard let matchingSnapshots = try evaluateEphemeralProperty(.init(
            editingState: editingState,
            entityName: entity.name,
            propertyIndex: property.index,
            value: comparisonValue
        )) else {
            return nil
        }
        if matchingSnapshots.isEmpty {
            return lhs.copy(
                clause: "FALSE",
                bindings: [],
                kind: .setMembership
            )
        }
        let primaryKeys = matchingSnapshots.map { matchingSnapshot in
            SQLValue.text(matchingSnapshot.key.primaryKey(as: String.self))
        }
        let placeholders = Array(repeating: "?", count: primaryKeys.count).joined(separator: ", ")
        hasher.combine(entity.name)
        hasher.combine(property.index)
        hasher.combine(primaryKeys.count)
        if evaluatedSnapshots == nil {
            evaluatedSnapshots = matchingSnapshots
        } else {
            evaluatedSnapshots?.merge(matchingSnapshots) { _, new in new }
        }
        return lhs.copy(
            clause: "(\(quote(alias)).\(quote(pk)) IN (\(placeholders)))",
            bindings: primaryKeys,
            kind: .setMembership
        )
    }
}

extension SQLPredicateTranslator {
    #if true
    // TODO: Temporarily test matching to inherited properties.
    nonisolated private mutating func resolveKeyPathNames<Model: PersistentModel>(
        _ keyPaths: Set<AnyKeyPath & Sendable>,
        for type: Model.Type
    ) -> Set<String> {
        guard !keyPaths.isEmpty else { return [] }
        var names = Set<String>()
        names.reserveCapacity(keyPaths.count)
        for keyPath in keyPaths {
            if let property = self.keyPaths[keyPath] {
                names.insert(property.name)
            } else if let keyPath: PartialKeyPath<Model> & Sendable = sendable(cast: keyPath),
                      let property = Model.schemaMetadata(for: keyPath) {
                names.insert(property.name)
                self.keyPaths[keyPath] = property
            } else {
                log(.warning, "Could not resolve key path to property name: \(keyPath)")
            }
        }
        return names
    }
    #endif
    
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
        #if true
        let resolvedFetchNames = resolveKeyPathNames(propertiesToFetch, for: Model.self)
        let resolvedPrefetchNames = resolveKeyPathNames(relationshipKeyPathsForPrefetching, for: Model.self)
        #endif
        hasher.combine(propertiesToFetch)
        hasher.combine(relationshipKeyPathsForPrefetching)
        var columns = [String]()
        var foreignKeyColumns = [PropertyMetadata]()
        let entityAlias = alias
        loadSchemaMetadata(for: Model.self)
        // TODO: Temporarily removed key to verify that this prevented references from being completely consumed.
//        loadSchemaMetadata(for: Model.self, key: key)
        guard var primaryKeyColumn = self.keyPaths[\Model.persistentModelID] else {
            preconditionFailure("Primary key was not registered in context: \(type)")
        }
        let schemaMetadata = type.databaseSchemaMetadata
        primaryKeyColumn.index = self.resultIndex
        columns.append(clause(entityAlias, primaryKeyColumn.name))
        var properties = [PropertyMetadata]()
        properties.reserveCapacity(schemaMetadata.count + 1)
        properties.append(primaryKeyColumn)
        // TODO: Use the updated `TableReference` instead.
        for var property in schemaMetadata where !property.flags.contains(.isExternal) {
            defer { properties.append(property) }
            switch property.metadata {
            case is Schema.Attribute:
                let columnAlias: String
                if property.isInherited,
                   let ownerEntity = entityOwningProperty(named: property.name, startingAt: entity),
                   let inheritedAlias = createInheritedAlias(key, from: entity, as: entityAlias, to: ownerEntity) {
                    columnAlias = inheritedAlias
                } else {
                    columnAlias = entityAlias
                }
                #if true
                if !resolvedFetchNames.isEmpty && !resolvedFetchNames.contains(property.name) {
                    property.isSelected = false
                } else {
                    columns.append(clause(columnAlias, property.name))
                }
                #else
                if !propertiesToFetch.isEmpty && !propertiesToFetch.contains(property.keyPath) {
                    property.isSelected = false
                } else {
                    columns.append(clause(columnAlias, property.name))
                }
                #endif
            case let relationship as Schema.Relationship:
                #if true
                if resolvedPrefetchNames.contains(property.name) {
                    property.flags.insert(.prefetch)
                }
                #else
                if relationshipKeyPathsForPrefetching.contains(property.keyPath) {
                    property.flags.insert(.prefetch)
                }
                #endif
                if relationship.isToOneRelationship {
                    let columnAlias: String
                    if property.flags.contains(.isInherited),
                       let ownerEntity = entityOwningProperty(named: property.name, startingAt: entity),
                       let inheritedAlias = createInheritedAlias(key, from: entity, as: entityAlias, to: ownerEntity) {
                        columnAlias = inheritedAlias
                    } else {
                        columnAlias = entityAlias
                    }
                    columns.append(clause(columnAlias, relationship.name + "_pk"))
                    foreignKeyColumns.append(property)
                }
            default:
                log(.notice, "Unhandled property metadata", metadata: ["property": "\(property)"])
                continue
            }
        }
        guard includeJoins else {
            return (columns, properties)
        }
        if let key, let references = self.references[key], !references.isEmpty {
            for (index, reference) in references.enumerated() {
                guard let property = foreignKeyColumns.first(where: { reference.sourceColumn.hasPrefix($0.name) }) else {
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
                    preconditionFailure()
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
            #if true
            let property: PropertyMetadata?
            if let resolved = self.keyPaths[keyPath] {
                property = resolved
            } else if let keyPath: PartialKeyPath<Model> & Sendable = sendable(cast: keyPath)  {
                property = Model.schemaMetadata(for: keyPath)
            } else {
                property = nil
            }
            guard let property else {
                log(.warning, "KeyPath for prefetching not found: \(keyPath)")
                continue
            }
            #else
            guard let property = self.keyPaths[keyPath] else {
                log(.warning, "KeyPath for prefetching not found: \(keyPath)")
                continue
            }
            #endif
            guard let relationship = property.metadata as? Schema.Relationship else {
                preconditionFailure("Expected property metadata to reference a Schema.Relationship.")
            }
            guard let destinationEntity = self.schema.entitiesByName[relationship.destination] else {
                preconditionFailure("Expected schema to contain a destination entity for the relationship.")
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
                clause = self.options.contains(.useFallbackOnCompositeAttributes)
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
    nonisolated private mutating func parseKeyPathForProperty<Model>(_ keyPath: PartialKeyPath<Model> & Sendable)
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
                The parsed key path could not match to any PropertyMetadata in the schema.
                It might be present with an identical description with mismatching Equatable/Hashable.
                Using a protocol or generic constraint can affect matching to a key path.
                Consider constraining to only PersistentModel when possible.
                Affected key path: \(keyPath)
                Metadata: \(metadata)
                Type: \(type)
                Property: \(property)
                """
            )
            #endif
            return property
        }
        log(.warning, "Traversed entire path but found no terminal value: \(keyPath)")
        if options.contains(.failOnInvalidTranslations) {
            throw Self.Error.invalidTranslation("\(keyPath)")
        }
        return nil
    }
    
    nonisolated internal mutating func bridgeAsRelationship<Base, Root, Value>(
        _ baseType: Base.Type,
        from lhsKeyPath: AnyKeyPath & Sendable,
        to rhsKeyPath: any KeyPath<Root, Value> & Sendable
    ) -> (entity: Schema.Entity, property: PropertyMetadata, keyPath: AnyKeyPath & Sendable)?
    where Base: PersistentModel {
        assert(lhsKeyPath is PartialKeyPath<Base>, "\(lhsKeyPath) LHS key path root type is not \(Base.self).")
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
            log(.notice, "RHS model is not a valid PersistentModel type or is not in the schema: \(description)")
            return nil
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
        if options.contains(.allowKeyPathVariantsForPropertyLookup) {
            rhsType.addKeyPathVariantToPropertyMetadata(fullKeyPath, for: rhsProperty)
        }
        if types[Schema.entityName(for: rhsType)] == nil {
            log(.warning, "Entity was not loaded to allow for key path bridging: \(rhsType)")
            loadSchemaMetadata(for: rhsType)
        }
        rhsProperty.enclosing = lhsProperty.metadata
        rhsProperty.keyPath = fullKeyPath
        log(.debug, "Key path bridged relationship: \(lhsProperty.name).\(rhsProperty.name)")
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
            preconditionFailure("Root type of key path does not match the type in the schema: \(description)")
        }
        guard composite.valueType is any RawRepresentable.Type else {
            preconditionFailure("Root type of key path does not match the type in the schema: \(description)")
        }
        guard !composite.properties.isEmpty else {
            preconditionFailure("Sub-attributes not found for composite attribute: \(description)")
        }
        guard !components.isEmpty else {
            preconditionFailure("Unable to extract sub-attribute from composite attribute: \(description)")
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
            log(.warning, "No metadata found for RHS attribute: \(description)", metadata: [
                "composite_attribute_properties": "\(composite.properties.map(\.name))",
                "components": "\(components)"
            ])
            return nil
        }
        let subAttribute = composite.properties[subAttributeIndex]
        guard subAttribute.valueType is Value.Type else {
            preconditionFailure("Value type of key path does not match the type in the schema: \(description)")
        }
        let subProperty = PropertyMetadata(
            index: subAttributeIndex,
            name: subAttribute.name,
            keyPath: fullKeyPath,
            metadata: subAttribute,
            enclosing: composite
        )
        log(.debug, "Key path bridged composite attribute: \(property.name).\(subProperty.name)")
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
    
    nonisolated internal mutating func createInheritedAlias(
        _ key: PredicateExpressions.VariableID?,
        from sourceEntity: Schema.Entity,
        as sourceAlias: String,
        to destinationEntity: Schema.Entity
    ) -> String? {
        if sourceEntity.name == destinationEntity.name {
            return sourceAlias
        }
        if let path = inheritancePath(descendingFrom: sourceEntity, to: destinationEntity) {
            return append(path, startingAt: (sourceEntity, sourceAlias))
        }
        if let path = inheritancePath(ascendingFrom: sourceEntity, to: destinationEntity) {
            return append(path, startingAt: (sourceEntity, sourceAlias))
        }
        return nil
        func append(_ path: [Schema.Entity], startingAt current: consuming (entity: Schema.Entity, alias: String)) -> String {
            for entity in path {
                let alias = createTableAlias(key, entity.name)
                if alias == current.alias { current = (entity, alias); continue }
                let reference = TableReference(
                    sourceAlias: current.alias,
                    sourceTable: current.entity.name,
                    sourceColumn: pk,
                    destinationAlias: alias,
                    destinationTable: entity.name,
                    destinationColumn: pk
                )
                if !containsReference(reference, in: implicitReferences),
                   !containsReference(reference, in: key.flatMap { references[$0] }) {
                    implicitReferences.append(reference)
                }
                current = (entity, alias)
            }
            return current.alias
        }
        func containsReference(_ reference: TableReference, in references: OrderedSet<TableReference>?) -> Bool {
            guard let references else { return false }
            return references.contains(where: { existing in
                existing.sourceAlias == reference.sourceAlias &&
                existing.sourceTable == reference.sourceTable &&
                existing.sourceColumn == reference.sourceColumn &&
                existing.destinationAlias == reference.destinationAlias &&
                existing.destinationTable == reference.destinationTable &&
                existing.destinationColumn == reference.destinationColumn
            })
        }
        func inheritancePath(descendingFrom current: Schema.Entity, to target: Schema.Entity) -> [Schema.Entity]? {
            for subentity in current.subentities {
                if subentity.name == target.name { return [subentity] }
                if let path = inheritancePath(descendingFrom: subentity, to: target) { return [subentity] + path }
            }
            return nil
        }
        func inheritancePath(ascendingFrom current: Schema.Entity, to target: Schema.Entity) -> [Schema.Entity]? {
            var path = [Schema.Entity]()
            var currentEntity: Schema.Entity? = current
            while let superentity = currentEntity?.superentity {
                path.append(superentity)
                if superentity.name == target.name { return path }
                currentEntity = superentity
            }
            return nil
        }
    }
    
    nonisolated internal func entityOwningProperty(
        named name: String,
        startingAt entity: Schema.Entity
    ) -> Schema.Entity? {
        var entity: Schema.Entity? = entity
        while let currentEntity = entity {
            if currentEntity.storedPropertiesByName[name] != nil && currentEntity.inheritedPropertiesByName[name] == nil {
                return currentEntity
            }
            entity = currentEntity.superentity
        }
        return nil
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
    nonisolated private mutating func getProperty<Variable>(at keyPath: PartialKeyPath<Variable> & Sendable)
    throws -> PropertyMetadata? where Variable: PersistentModel & SendableMetatype {
        guard let property = self.keyPaths[keyPath] else {
            if let superclass = class_getSuperclass(Variable.self) as? any PersistentModel.Type {
                loadSchemaMetadata(for: superclass, key: key)
                return try Variable.schemaMetadata(for: keyPath)
                ?? lookupPropertyMetadata(superclass: superclass, subclass: Variable.self, keyPath: keyPath)
                ?? parseKeyPathForProperty(keyPath)
            } else {
                loadSchemaMetadata(for: Variable.self, key: key)
                return try Variable.schemaMetadata(for: keyPath)
                ?? parseKeyPathForProperty(keyPath)
            }
        }
        return property
    }
    
    nonisolated private mutating func lookupPropertyMetadata<Super, Sub>(
        superclass: Super.Type,
        subclass: Sub.Type,
        keyPath: PartialKeyPath<Sub> & Sendable
    ) -> PropertyMetadata? where Super: PersistentModel, Sub: PersistentModel {
        #if swift(>=6.2)
        guard #available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *) else {
            return nil
        }
        #endif
        guard let metadata = KeyPathDescription.parse(keyPath: keyPath),
              let propertyName = metadata.components.compactMap(\.property).last else {
            return nil
        }
        var superclass: (any PersistentModel.Type)? = superclass
        while let currentSuperclass = superclass {
            if let property = currentSuperclass.databaseSchemaMetadata.first(where: { $0.name == propertyName }) {
                if options.contains(.allowKeyPathVariantsForPropertyLookup) {
                    subclass.addKeyPathVariantToPropertyMetadata(keyPath, for: property)
                }
                var flags = property.flags
                flags.insert(.isInherited)
                let property = property.copy(keyPath: keyPath, flags: flags)
                self.keyPaths[keyPath] = property
                return property
            }
            superclass = class_getSuperclass(currentSuperclass) as? any PersistentModel.Type
        }
        return nil
    }
    
    nonisolated internal subscript<Variable>(keyPath: PartialKeyPath<Variable> & Sendable)
    -> PropertyMetadata? where Variable: PersistentModel & SendableMetatype {
        mutating get throws { try getProperty(at: keyPath) }
    }
    
    nonisolated internal subscript<Variable>(keyPath: AnyKeyPath & Sendable, type: Variable.Type)
    -> PropertyMetadata? where Variable: PersistentModel & SendableMetatype {
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

extension SQLPredicateTranslator {
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
        log(as: type, input: message(), metadata: metadata, function: function)
        #endif
    }
    
    nonisolated internal mutating func log(
        as logLevel: Logger.Level?,
        input messages: Any...,
        metadata: [Logger.Metadata.Key: Logger.MetadataValue]? = nil,
        function: String = #function
    ) {
        #if DEBUG
        log(logLevel: logLevel, messages: messages, metadata: metadata, function: function)
        #endif
    }
    
    nonisolated private mutating func log(
        logLevel: Logger.Level?,
        messages: Any...,
        metadata: [Logger.Metadata.Key: Logger.MetadataValue]? = nil,
        function: String = #function
    ) {
        // Skip logging that assumes SwiftData is resolving a fault.
        guard requestedIdentifiers == nil else {
            return
        }
        // Skip if no log level is specified and is below the minimum.
        guard let logLevel, options.contains(.useVerboseLogging) || logLevel >= minimumLogLevel else {
            return
        }
        // Skip if a tag filter exists and the current tag is not in the allowed set.
        if let tagFilter = self.tags, let tag = self.tag, !tagFilter.contains(tag.lowercased()) {
            return
        }
        let output = messages.map(String.init(describing:)).joined(separator: " ")
        let tag = self.tag ?? "<nil>"
        if options.contains(.preferStandardOutput) {
            let position = "[Predicate #\(counter) @ lvl-\(level)]"
            let context = "\(tag).\(function)"
            print("\(logLevel) \(position) \(context) \(output)", "metadata: \(String(describing: metadata))")
        } else {
            var metadata = metadata ?? .init()
            metadata["position"] = "predicate #\(counter) (level \(level))"
            metadata["context"] = "\(tag).\(function)"
            if !path.isEmpty { metadata["path"] = .array(path.map { "\($0)" }) }
            logger.log(level: logLevel, "\(output)", metadata: metadata)
        }
    }
}
