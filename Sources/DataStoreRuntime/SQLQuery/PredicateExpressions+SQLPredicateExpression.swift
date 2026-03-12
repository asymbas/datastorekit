//
//  PredicateExpressions+SQLPredicateExpression.swift
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
import SQLiteStatement
import SQLSupport
import Synchronization

#if swift(>=6.2)
import SwiftData
#else
@preconcurrency import SwiftData
#endif

private typealias ForEach = SQLForEach

/// `0`
extension PredicateExpressions.Value: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        context.log(.trace, "Value received binding: \(value) as \(Output.self).self.")
        if let hashValue = self.value as? (any Hashable) {
            context.hasher.combine(hashValue)
        }
        let clause: String
        let bindings: [Any]
        switch value {
        case let persistentIdentifiers as Set<PersistentIdentifier>:
            let count = persistentIdentifiers.count
            context.requestedIdentifiers = persistentIdentifiers
            context.bindingsCount = count
            let primaryKeys = persistentIdentifiers.map { $0.primaryKey(as: String.self) }
            let placeholders = Array(repeating: "?", count: count).joined(separator: ", ")
            clause = "(\(placeholders))"
            bindings = primaryKeys.map(SQLValue.text)
        case let persistentIdentifier as PersistentIdentifier:
            let primaryKey = persistentIdentifier.primaryKey(as: String.self)
            clause = "?"
            bindings = [SQLValue.text(primaryKey)]
        case let model as any PersistentModel:
            clause = "?"
            bindings = [model]
        case let sqlQueryPassthrough as SQL:
            context.sqlQueryPassthrough = sqlQueryPassthrough
            return .invalid
        case let values as any Swift.Collection<Int>:
            let placeholders = Array(repeating: "?", count: values.count).joined(separator: ", ")
            clause = "(\(placeholders))"
            bindings = values.map { SQLValue.integer(Int64($0)) }
        case let values as any Swift.Collection<Double>:
            let placeholders = Array(repeating: "?", count: values.count).joined(separator: ", ")
            clause = "(\(placeholders))"
            bindings = values.map(SQLValue.real)
        case let values as any Swift.Collection<String>:
            let placeholders = Array(repeating: "?", count: values.count).joined(separator: ", ")
            clause = "(\(placeholders))"
            bindings = values.map(SQLValue.text)
        case _ where SQLType(equivalentRawValueType: type(of: value)) == nil:
            context.log(.debug, "Binding as Any: \(type(of: value)) == \(Output.self).self")
            clause = "?"
            bindings = [value]
        default:
            context.log(.debug, "Binding as SQLValue: \(type(of: value)) == \(Output.self).self")
            clause = "?"
            bindings = [SQLValue(any: value)]
        }
        return .init(
            clause: clause,
            bindings: bindings,
            type: Output.self,
            kind: .bindParameter
        )
    }
}

/// `$0`
extension PredicateExpressions.Variable: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        context.log(.trace, "Variable received where Output.Type is \(Output.self).self.")
        switch Output.self as Any.Type {
        case var type where type is any RelationshipCollection.Type:
            type = unwrapArrayMetatype(type)
            context.log(.trace, "Variable conforms to RelationshipCollection.Type: \(type).self")
            fallthrough
        case let type where type is any PersistentModel.Type:
            guard let type = type as? any (PersistentModel & SendableMetatype).Type,
                  let entity = context.schema.entity(for: type) ?? Schema([type]).entity(for: type) else {
                fatalError(SwiftDataError.unknownSchema.localizedDescription)
            }
            context.log(.trace, "Variable conforms to PersistentModel.Type: \(type).self")
            let alias = context.createTableAlias(key, entity.name)
            context.loadSchemaMetadata(for: type, key: key)
            context.hasher.combine(entity.name)
            return .init(clause: alias, key: key, alias: alias, type: Output.self, entity: entity, kind: .scope)
        default:
            context.log(.trace, "Variable is a non-entity value type: \(Output.self).self")
            return .init(clause: "", key: key, type: Output.self, kind: .scope)
        }
    }
}

/// `$0.property`
extension PredicateExpressions.KeyPath: SQLPredicateExpression
where Root: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        var root = self.root.query(&context)
        let description = "\\\(Root.Output.self).\(Output.self) == \(keyPath) -> \(root.description)"
        context.log(.debug, "Evaluating KeyPath for \(T.self).self: \(description)")
        guard kind == nil else { return resolveComputedPropertyFragment(&context, root) }
        switch root.kind {
        case .scope:
            assert(root.entity != nil, "No entity associated to fragment when accessing a property.")
            guard let type = Root.Output.self as? any PersistentModel.Type else {
                fatalError("Root.Output.self is not a PersistentModel.Type: \(description)")
            }
            context.loadSchemaMetadata(for: type)
            guard let property = try? context[keyPath, type] else {
                fatalError("No PropertyMetadata found in schema: \(description)")
            }
            context.log(.trace, "Key path resolved top-level property: \(property.name) \(description)")
            return resolvePropertyFragment(&context, root.copy(property: property))
        case .columnReference:
            assert(root.entity != nil, "No entity associated to fragment when accessing a property.")
            guard let keyPath = root.keyPath else {
                return root.invalid("Root did not provide the root key path", description)
            }
            switch root.property?.metadata {
            case let relationship as Schema.Relationship:
                guard let type = root.type as? any PersistentModel.Type else {
                    return root.invalid("Root.Output.self is not a PersistentModel.Type", description)
                }
                guard let _ = Root.Output.self as? any PersistentModel.Type else {
                    return root.invalid("Root.Output.self is not a PersistentModel.Type", description)
                }
                context.loadSchemaMetadata(for: type)
                guard let result = context.bridgeAsRelationship(type, from: keyPath, to: self.keyPath) else {
                    return root.invalid("Missing relationship", description)
                }
                context.log(.debug, "Resolved bridging relationship: \(description)")
                let destinationAlias = context.createTableAlias(root.key, relationship.destination)
                let reference = TableReference(
                    sourceAlias: root.alias,
                    sourceTable: root.entity?.name ?? "INVALID",
                    sourceColumn: relationship.name + "_pk",
                    destinationAlias: destinationAlias,
                    destinationTable: relationship.destination,
                    destinationColumn: pk
                )
                if let key = root.key, context.references[key, default: []].append(reference).inserted {
                    context.log(.debug, "Inserted JOIN clause reference (to-one): \(reference)")
                }
                return resolvePropertyFragment(&context, root.copy(
                    clause: destinationAlias,
                    alias: destinationAlias,
                    type: type,
                    entity: result.entity,
                    property: result.property,
                    keyPath: result.keyPath
                ))
            case is Schema.CompositeAttribute:
                guard let entity = root.entity else {
                    return root.invalid("Root has no associated entity", description)
                }
                guard let result = context.bridgeAsCompositeAttribute(from: keyPath, to: self.keyPath) else {
                    return .invalid("Missing composite attribute", description)
                }
                context.log(.debug, "Resolved bridging composite attribute: \(description)")
                return resolvePropertyFragment(&context, root.copy(
                    entity: entity,
                    property: result.property,
                    keyPath: result.keyPath
                ))
            default:
                fatalError("Unhandled case for bridging key path: \(description)")
            }
        case .bindParameter where root.bindings.last is Root.Output:
            guard let object = root.bindings.popLast() as? Root.Output else {
                fatalError("Unable to extract Root.Output from bindings: \(root.bindings)")
            }
            let value = object[keyPath: keyPath]
            context.log(.trace, "Extracted value to bind from object: \(description) = \(value)")
            switch Swift.type(of: value) {
            case let type as any PersistentModel.Type:
                guard let entity = context.schema.entity(for: type) else {
                    context.log(.notice, "No model found in schema: \(description)")
                    fallthrough
                }
                context.log(.debug, "Binding \(entity.name) value: \(description) = \(value)")
                return root.copy(bindings: [consume value], entity: entity)
            default:
                context.log(.debug, "Binding extracted value: \(description) = \(value)")
                return root.copy(bindings: [consume value])
            }
        default:
            return .invalid("Reached unexpected case in key path resolution", description)
        }
    }
    
    /// Uses the matched `PropertyMetadata` to convert data into an SQL expression.
    ///
    /// - Only to-one relationships handle a JOIN inline on the same key as they do not enter any closure.
    /// - Self-referencing is handled by `context.path`, which concatenates nested access of types.
    /// - Unidirectional relationships are ignored.
    /// - Creates a `Join` on the current `VariableID` closure and query for relationships.
    private func resolvePropertyFragment<T>(
        _ context: inout Context<T>,
        _ root: consuming Fragment
    ) -> Fragment {
        let description = "\\\(Root.Output.self).\(Output.self) == \(keyPath) -> \(root.description)"
        guard let sourceAlias = root.alias,
              let property = root.property else {
            return .invalid("Incomplete KeyPath.root", description)
        }
        let clause: String?
        switch property.metadata {
        case let relationship as Schema.Relationship where relationship.isToOneRelationship:
            context.log(.debug, "Property is a to-one relationship: \(description)")
            clause = "\(quote(root.clause)).\(quote(relationship.name + "_pk"))"
        case is Schema.Relationship:
            context.log(.debug, "Property is a to-many relationship: \(description)")
            clause = "\(quote(sourceAlias)).\(quote(pk))"
        case is Schema.CompositeAttribute:
            context.log(.debug, "Property is a composite attribute: \(description)")
            clause = "\(quote(sourceAlias)).\(quote(property.name))"
        case is Schema.Attribute where property.enclosing is Schema.Relationship:
            context.log(.debug, "Property is an attribute of a relationship: \(description)")
            clause = "\(quote(root.clause)).\(quote(property.name))"
        case is Schema.Attribute where property.enclosing is Schema.CompositeAttribute:
            context.log(.debug, "Property is an attribute of a composite: \(description)")
            guard !(unwrapOptionalMetatype(property.enclosing!.valueType) is any RawRepresentable.Type) else {
                clause = "\(quote(sourceAlias)).\(quote(property.enclosing!.name))"
                break
            }
            clause = context.useFallbackOnCompositeAttributes ?
                """
                COALESCE (
                    json_extract(CAST(\(root.clause) AS TEXT), '$."\(property.name)"'),
                    json_extract(CAST(\(root.clause) AS TEXT), '$')
                )
                """
            : property.enclosing?.valueType is (any ExpressibleByArrayLiteral.Type) == false
            ? "json_extract(\(root.clause), '$.\(quote(property.name))')"
            : "json_extract(\(root.clause), '$')"
        case is Schema.Attribute:
            context.log(.debug, "Property is an attribute: \(description)")
            clause = "\(quote(sourceAlias)).\(quote(property.name))"
        default:
            return .invalid("Unhandle PropertyMetadata case", description)
        }
        return root.copy(clause: clause, keyPath: keyPath, kind: .columnReference)
    }
    
    private func resolveComputedPropertyFragment<T>(
        _ context: inout Context<T>,
        _ root: consuming Fragment
    ) -> Fragment {
        switch kind {
        case .collectionFirst:
            return resolve { _, _ in
                "NULL"
            } referencingColumn: { lhsAlias, lhsTable, foreignKeyColumn, rhsAlias in
                """
                SELECT "\(lhsAlias)"."\(pk)" -- collectionFirst (reference)
                FROM "\(lhsTable)" AS "\(lhsAlias)"
                WHERE "\(lhsAlias)"."\(foreignKeyColumn)" = "\(rhsAlias)"."\(pk)"
                LIMIT 1
                """
            } referencingIntermediaryTable: { join, lhs, rhs in
                """
                SELECT "\(rhs.alias)"."\(pk)" -- collectionFirst (intermediary)
                FROM "\(join.table)" AS "\(join.alias)"
                JOIN "\(rhs.table)" AS "\(rhs.alias)"
                ON "\(rhs.alias)"."\(pk)" = "\(join.alias)"."\(rhs.column)"
                WHERE "\(join.alias)"."\(lhs.column)" = "\(lhs.alias)"."\(pk)"
                LIMIT 1
                """
            }
        case .bidirectionalCollectionLast:
            return resolve { _, _ in
                "NULL"
            } referencingColumn: { lhsAlias, lhsTable, foreignKeyColumn, rhsAlias in
                """
                SELECT "\(lhsAlias)"."\(pk)" -- bidirectionalCollectionLast (reference)
                FROM "\(lhsTable)" AS "\(lhsAlias)"
                WHERE "\(lhsAlias)"."\(foreignKeyColumn)" = "\(rhsAlias)"."\(pk)"
                ORDER BY "\(lhsAlias)"."\(pk)" DESC
                LIMIT 1
                """
            } referencingIntermediaryTable: { join, lhs, rhs in
                """
                SELECT "\(rhs.alias)"."\(pk)" -- bidirectionalCollectionLast (intermediary)
                FROM "\(join.table)" AS "\(join.alias)"
                JOIN "\(rhs.table)" AS "\(rhs.alias)"
                ON "\(rhs.alias)"."\(pk)" = "\(join.alias)"."\(rhs.column)"
                WHERE "\(join.alias)"."\(lhs.column)" = "\(lhs.alias)"."\(pk)"
                ORDER BY "\(rhs.alias)"."\(pk)" DESC
                LIMIT 1
                """
            }
        case .collectionCount:
            return resolve { sourceAlias, attribute in
                "LENGTH(\(quote(sourceAlias)).\(quote(attribute)))"
            } referencingColumn: { lhsAlias, lhsTable, foreignKeyColumn, rhsAlias in
                """
                SELECT COUNT(*) -- collectionCount (reference)
                FROM "\(lhsTable)" AS "\(lhsAlias)"
                WHERE "\(lhsAlias)"."\(foreignKeyColumn)" = "\(rhsAlias)"."\(pk)"
                """
            } referencingIntermediaryTable: { join, lhs, _ in
                """
                SELECT COUNT(*) -- collectionFirst (intermediary)
                FROM "\(join.table)" AS "\(join.alias)"
                WHERE "\(join.alias)"."\(lhs.column)" = "\(lhs.alias)"."\(pk)"
                """
            }
        case .collectionIsEmpty:
            return resolve { sourceAlias, attribute in
                "\(quote(sourceAlias)).\(quote(attribute)) = ''"
            } referencingColumn: { lhsAlias, lhsTable, foreignKeyColumn, rhsAlias in
                """
                NOT EXISTS ( -- collectionIsEmpty (reference)
                    SELECT 1
                    FROM "\(lhsTable)" AS "\(lhsAlias)"
                    WHERE "\(lhsAlias)"."\(foreignKeyColumn)" = "\(rhsAlias)"."\(pk)"
                )
                """
            } referencingIntermediaryTable: { join, lhs, _ in
                """
                NOT EXISTS ( -- collectionIsEmpty (intermediary)
                    SELECT 1
                    FROM "\(join.table)" AS "\(join.alias)"
                    WHERE "\(join.alias)"."\(lhs.column)" = "\(lhs.alias)"."\(pk)"
                )
                """
            }
        default:
            fatalError("Unknown KeyPath.kind case: \(String(describing: kind))")
        }
        /// - To-many relationship metatypes are wrapped as `Array<Root.Output>.self`.
        func resolve(
            error errorHandler: () -> String = {
                print("Error occurred resolving computed property: \(root.description)")
                return "(1 = 0) /* Error */"
            },
            attribute attributeHandler: (
                _ sourceAlias: String,
                _ sourceColumn: String
            ) -> String,
            referencingColumn referenceHandler: (
                _ lhsAlias: String,
                _ lhsTable: String,
                _ lhsColumn: String,
                _ rhsAlias: String
            ) -> String,
            referencingIntermediaryTable intermediaryHandler: (
                _ join: (alias: String, table: String),
                _ lhs: (alias: String, table: String, column: String),
                _ rhs: (alias: String, table: String, column: String)
            ) -> String
        ) -> Fragment {
            let description = "\\\(Root.Output.self).\(Output.self) == \(keyPath) -> \(root.description)"
            context.log(.debug, "Resolving computed property: \(description)")
            guard Root.Output.self is any RelationshipCollection.Type
                    || Root.Output.self is any PersistentModel.Type else {
                switch root.bindings.popLast() {
                case let value as Root.Output:
                    context.log(.debug, "Binding cast as Root.Output: \(value[keyPath: keyPath])")
                    return shortCircuitValue(value)
                case let value as SQLValue:
                    context.log(.debug, "Binding is wrapped in SQLValue: \(value)")
                    guard let unwrappedValue = value.base as? Root.Output else {
                        context.log(.warning, "Binding failed to cast as Root.Output: \(description)")
                        fallthrough
                    }
                    context.log(.debug, "Unwrapped binding casted as Root.Output: \(unwrappedValue)")
                    return shortCircuitValue(unwrappedValue)
                default:
                    // FIXME: Resolving arrays is not implemented yet.
                    return root.copy(clause: errorHandler(), kind: .functionCall)
                }
                /// Short-circuit values if they can be computed outside of SQL, such as responding to UI changes.
                func shortCircuitValue(_ value: Root.Output) -> Fragment {
                    switch value[keyPath: keyPath] {
                    case let bool as Bool:
                        root.copy(clause: "\(bool ? "TRUE" : "FALSE")", kind: .functionCall)
                    default:
                        root.copy(clause: String(describing: value), kind: .functionCall)
                    }
                }
            }
            guard let alias = root.alias, let _ = root.entity else {
                return .invalid
            }
            if let attribute = root.property?.metadata as? Schema.Attribute {
                guard attribute.valueType is String.Type else {
                    context.log(.warning, "Predicate can only apply to String attributes.")
                    return root.copy(clause: errorHandler(), kind: .functionCall)
                }
                return root.copy(clause: attributeHandler(alias, attribute.name), kind: .functionCall)
            }
            if let _ = root.property?.metadata as? Schema.Relationship {
                switch root.property?.reference {
                case let reference? where reference.count == 1:
                    let reference = reference[0]
                    let sql = referenceHandler(
                        context.createTableAlias(root.key, reference.destinationTable),
                        reference.destinationTable,
                        reference.destinationColumn,
                        context.createTableAlias(root.key, reference.sourceTable)
                    )
                    // MARK: Is now inverted compared to previous implementation.
                    return root.copy(clause: "(\(sql))", kind: .functionCall)
                case let reference?:
                    let joinTuple = (
                        context.createTableAlias(root.key, reference[0].destinationTable),
                        reference[0].destinationTable
                    )
                    let sourceTuple = (
                        context.createTableAlias(root.key, reference[0].sourceTable),
                        reference[0].sourceTable,
                        reference[0].sourceColumn
                    )
                    let destinationTuple = (
                        context.createTableAlias(root.key, reference[1].destinationTable),
                        reference[1].destinationTable,
                        reference[1].destinationColumn
                    )
                    let sql = intermediaryHandler(joinTuple, sourceTuple, destinationTuple)
                    return root.copy(
                        clause: "(\(sql))",
                        kind: .functionCall
                    )
                default:
                    return .invalid
                }
            }
            return root.copy(clause: errorHandler(), kind: .functionCall)
        }
    }
}

/// `#Expression<T, Bool> { $0.property == "a" }`
extension PredicateExpressions.ExpressionEvaluate: SQLPredicateExpression
where repeat each Input: SQLPredicateExpression,
      Transformation: SQLPredicateExpression,
      Transformation.Output == Expression<repeat (each Input).Output, Output> {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        var ctes = [CommonTableExpression]()
        var clauses = [SQL]()
        var bindings = [Any]()
        let outerIndex = context.cteIndex
        context.cteIndex += 1
        for pack in repeat each input {
            let outer = pack.query(&context)
            guard let outerKey = outer.key, let outerAlias = outer.alias else {
                return outer.invalid("Incomplete ExpressionEvaluate.input")
            }
            bindings += outer.bindings
            guard let base = self.expression as? PredicateExpressions.Value<Transformation.Output> else {
                return outer.invalid("Casting value failed", Transformation.self, Output.self)
            }
            for expression in repeat each base.value.variable {
                let inner = expression.query(&context)
                bindings += inner.bindings
                guard let innerAlias = inner.alias, let innerEntity = inner.entity else {
                    return inner.invalid("Incomplete ExpressionEvaluate.variable")
                }
                guard let sql = base.value.expression as? SQLPredicateExpression else {
                    return inner.invalid("Casting value failed", Transformation.self, Output.self)
                }
                var body = sql.query(&context)
                if body.type is Bool.Type, body.clause == "?" {
                    guard let value = body.bindings.popLast() else {
                        fatalError("Expected Boolean value from bindings.")
                    }
                    context.log(.trace, "Short-circuiting Boolean expression.")
                    body = body.copy(clause: "(1 = \(value))")
                } else {
                    bindings += body.bindings
                }
                let select = { () -> (columns: [String], references: OrderedSet<TableReference>) in
                    var columns = [String]()
                    let references = context.references[expression.key].take() ?? []
                    for reference in references {
                        guard let alias = reference.rhsAlias else {
                            context.log(.notice, "No RHS reference specified: \(reference)")
                            continue
                        }
                        guard let entity = context.schema.entitiesByName[reference.rhsTable] else {
                            context.log(.notice, "No entity found for RHS reference: \(reference)")
                            continue
                        }
                        guard let type = context.types[entity.name] as? any PersistentModel.Type else {
                            context.log(.notice, "No type loaded for RHS refernece: \(reference)")
                            continue
                        }
                        let (selectedColumns, _) = context.selectResultColumns(
                            key: expression.key,
                            alias: alias,
                            entity: entity,
                            type: type
                        )
                        columns += consume selectedColumns
                    }
                    return (columns, references)
                }()
                let cte = CommonTableExpression("CTE_\(outerKey)_\(outerIndex)") {
                    #if DEBUG
                    if context.shouldMarkStartOfPredicateExpression {
                        let debug = debugVariableIDs(
                            ("path", context.path.last),
                            ("context", context.key),
                            ("outer", outer.key),
                            ("inner", inner.key),
                            ("body", body.key)
                        )
                        "/* Inner CTE ExpressionEvaluate (\(debug)) */"
                    }
                    #endif
                    Select(["\(quote(innerAlias)).\(quote(pk))"] + select.columns, qualified: true)
                    From(innerEntity.name, as: innerAlias)
                    let destinationAlias = context.createTableAlias(expression.key, innerEntity.name)
                    if innerAlias != destinationAlias {
                        Join(
                            innerEntity.name,
                            as: destinationAlias,
                            on: (innerAlias, innerEntity.name, pk),
                            equals: (destinationAlias, innerEntity.name, pk)
                        )
                    }
                    if !select.references.isEmpty {
                        ForEach(select.references) {
                            let lhsAlias = $0.lhsAlias ?? $0.lhsTable
                            let rhsAlias = $0.rhsAlias ?? $0.rhsTable
                            """
                            JOIN "\($0.rhsTable)" AS "\(rhsAlias)"
                            ON "\(rhsAlias)"."\($0.rhsColumn)" = "\(lhsAlias)"."\($0.lhsColumn)"
                            """
                        }
                    }
                    if !body.clause.isEmpty { Where(body.clause) }
                }
                ctes.append(cte)
                clauses.append(SQL {
                    Exists {
                        if context.shouldMarkStartOfPredicateExpression {
                            "/* Outer CTE ExpressionEvaluate */"
                        }
                        Select(1)
                        From(cte.name)
                        Where((cte.name, pk), equals: (outerAlias, pk))
                    }
                })
            }
        }
        guard !ctes.isEmpty else {
            return .invalid("ExpressionEvaluate is empty")
        }
        context.ctes.append(contentsOf: ctes)
        return .init(
            clause: clauses.map(\.sql).joined(separator: " AND "),
            bindings: bindings,
            kind: .existsClause
        )
    }
}

/// `#Predicate<T> { predicate.evaluate($0) }`
extension PredicateExpressions.PredicateEvaluate: SQLPredicateExpression
where repeat each Input: SQLPredicateExpression,
      Condition: SQLPredicateExpression,
      Condition.Output == Predicate<repeat (each Input).Output>  {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        var ctes = [CommonTableExpression]()
        var clauses = [SQL]()
        var bindings = [Any]()
        let outerIndex = context.cteIndex
        context.cteIndex += 1
        for pack in repeat each input {
            let outer = pack.query(&context)
            guard let outerKey = outer.key, let outerAlias = outer.alias else {
                return outer.invalid("Incomplete PredicateEvaluate.input")
            }
            bindings += outer.bindings
            guard let base = self.predicate as? PredicateExpressions.Value<Condition.Output> else {
                return outer.invalid("Casting value failed", Condition.self, Output.self)
            }
            for predicate in repeat each base.value.variable {
                let inner = predicate.query(&context)
                bindings += inner.bindings
                guard let innerAlias = inner.alias, let innerEntity = inner.entity else {
                    return inner.invalid("Incomplete PredicateEvaluate.variable")
                }
                guard let sql = base.value.expression as? SQLPredicateExpression else {
                    return inner.invalid("Casting value failed", Condition.self, Output.self)
                }
                var body = sql.query(&context)
                if body.type is Bool.Type, body.clause == "?" {
                    guard let value = body.bindings.popLast() else {
                        fatalError("Expected Boolean value from bindings.")
                    }
                    context.log(.trace, "Short-circuiting Boolean expression.")
                    body = body.copy(clause: "(1 = \(value))")
                } else {
                    bindings += body.bindings
                }
                let select = { () -> (columns: [String], references: OrderedSet<TableReference>) in
                    var columns = [String]()
                    let references = context.references[predicate.key].take() ?? []
                    for reference in references {
                        guard let alias = reference.rhsAlias else {
                            context.log(.notice, "No RHS reference specified: \(reference)")
                            continue
                        }
                        guard let entity = context.schema.entitiesByName[reference.rhsTable] else {
                            context.log(.notice, "No entity found for RHS reference: \(reference)")
                            continue
                        }
                        guard let type = context.types[entity.name] as? any PersistentModel.Type else {
                            context.log(.notice, "No type loaded for RHS refernece: \(reference)")
                            continue
                        }
                        let (selectedColumns, _) = context.selectResultColumns(
                            key: predicate.key,
                            alias: alias,
                            entity: entity,
                            type: type
                        )
                        columns += consume selectedColumns
                    }
                    return (columns, references)
                }()
                let cte = CommonTableExpression("CTE_\(outerKey)_\(outerIndex)") {
                    #if DEBUG
                    if context.shouldMarkStartOfPredicateExpression {
                        let debug = debugVariableIDs(
                            ("path", context.path.last),
                            ("context", context.key),
                            ("outer", outer.key),
                            ("inner", inner.key),
                            ("body", body.key)
                        )
                        "/* Inner CTE PredicateEvaluate (\(debug)) */"
                    }
                    #endif
                    Select(["\(quote(innerAlias)).\(quote(pk))"] + select.columns, qualified: true)
                    From(innerEntity.name, as: innerAlias)
                    let destinationAlias = context.createTableAlias(predicate.key, innerEntity.name)
                    if innerAlias != destinationAlias {
                        Join(
                            innerEntity.name,
                            as: destinationAlias,
                            on: (innerAlias, innerEntity.name, pk),
                            equals: (destinationAlias, innerEntity.name, pk)
                        )
                    }
                    if !select.references.isEmpty {
                        ForEach(select.references) {
                            let lhsAlias = $0.lhsAlias ?? $0.lhsTable
                            let rhsAlias = $0.rhsAlias ?? $0.rhsTable
                            """
                            JOIN "\($0.rhsTable)" AS "\(rhsAlias)"
                            ON "\(rhsAlias)"."\($0.rhsColumn)" = "\(lhsAlias)"."\($0.lhsColumn)"
                            """
                        }
                    }
                    if !body.clause.isEmpty { Where(body.clause) }
                }
                ctes.append(cte)
                clauses.append(SQL {
                    Exists {
                        if context.shouldMarkStartOfPredicateExpression {
                            "/* Outer CTE PredicateEvaluate */"
                        }
                        Select(1)
                        From(cte.name)
                        Where((cte.name, pk), equals: (outerAlias, pk))
                    }
                })
            }
        }
        guard !ctes.isEmpty else {
            return .invalid("PredicateEvaluate is empty")
        }
        context.ctes.append(contentsOf: ctes)
        return .init(
            clause: clauses.map(\.sql).joined(separator: " AND "),
            bindings: bindings,
            kind: .existsClause
        )
    }
}

/// `$0.properties.contains(value)`
extension PredicateExpressions.SequenceContains: SQLPredicateExpression
where LHS: SQLPredicateExpression,
      RHS: SQLPredicateExpression,
      LHS.Output: Sequence,
      LHS.Output.Element: Equatable,
      RHS.Output == LHS.Output.Element {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let sequence = self.sequence.query(&context)
        let element = self.element.query(&context)
        guard sequence.kind == .columnReference && sequence.property?.metadata != nil else {
            return sequence.copy(
                clause: "(\(element.clause) IN \(sequence.clause))",
                bindings: element.bindings + sequence.bindings,
                kind: .setMembership
            )
        }
        switch LHS.Output.self {
        case is Array<LHS.Output.Element>.Type:
            break
        default:
            if let type = LHS.Output.Element.self as? any Hashable.Type, hashable(cast: type) {
                break
            } else {
                fatalError("Unable to translate predicate into an SQL query.")
            }
        }
        return sequence.copy(
            clause: """
            EXISTS (
                SELECT 1
                FROM json_each(\(sequence.clause))
                WHERE json_each.value = \(element.clause)
            )
            """,
            bindings: element.bindings + sequence.bindings,
            kind: .setMembership
        )
    }
    
    private func hashable<Value>(cast type: Value.Type) -> Bool where Value: Hashable {
        LHS.Output.self is Set<Value>.Type
    }
}

/// `$0.properties.contains(where: { $0.property == 100 })`
extension PredicateExpressions.SequenceContainsWhere: SQLPredicateExpression
where LHS: SQLPredicateExpression,
      RHS: SQLPredicateExpression,
      LHS.Output: Sequence,
      RHS.Output == Bool {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        context.log(.trace, "SequenceContainsWhere.Element.Type is \(Element.self).self.")
        let sequence = self.sequence.query(&context)
        context.log(.trace, "Sequence fragment: \(sequence.description)")
        guard let sequenceKey = sequence.key,
              let sequenceAlias = sequence.alias else {
            return sequence.invalid("Incomplete SequenceContainsWhere.sequence")
        }
        context.path.append(sequenceKey)
        defer { context.key = context.path.popLast() }
        let element = self.variable.query(&context)
        context.log(.trace, "Element fragment: \(element.description)")
        #if DEBUG
        assert(
            element.key == self.variable.key,
            "Element fragment variable closure is misaligned: \(element.description)"
        )
        assert(
            element.type is Element.Type,
            "Element fragment expected to return \(Element.self).self: \(element.description)"
        )
        #endif
        let conditional = self.test.query(&context)
        context.log(.trace, "Conditional fragment: \(conditional.description)")
        switch element.type {
        case let type as any (PersistentModel & SendableMetatype).Type:
            var isManyToManyRelationship = false
            var filter: TableReference?
            guard let elementKey = element.key,
                  let elementAlias = element.alias,
                  let elementEntity = element.entity else {
                return element.invalid("Incomplete SequenceContainsWhere.variable")
            }
            context.log(.trace, "Referencing \(sequenceKey) from inside \(elementKey).")
            request: if let relationship = sequence.property?.metadata as? Schema.Relationship {
                guard let inverseKeyPath = relationship.inverseKeyPath,
                      let inverseKeyPath: (AnyKeyPath & Sendable) = sendable(cast: inverseKeyPath) else {
                    context.log(.debug, "Relationship is unidirectional: \(sequence.description)")
                    break request
                }
                guard let inverseProperty = type.schemaMetadata(for: inverseKeyPath) else {
                    return element.invalid("Missing inverse relationship metadata")
                }
                guard var reference = sequence.property?.reference else {
                    fatalError("Relationship is missing reference metadata: \(sequence.description)")
                }
                if reference.count == 2 {
                    isManyToManyRelationship = true
                    context.log(.debug, "Relationship is many-to-many: \(sequence.label)")
                    reference[0].lhsAlias = sequenceAlias
                    reference[0].rhsAlias = context.createTableAlias(sequenceKey, reference[0].rhsTable)
                    reference[1].lhsAlias = reference[0].rhsAlias
                    reference[1].rhsAlias = elementAlias
                    if context.references[variable.key, default: []].append(reference[0]).inserted {
                        context.log(.trace, "Inserted JOIN clause reference (many-to-many): \(reference)")
                    }
                    filter = reference[1]
                } else {
                    guard var reference = inverseProperty.reference else {
                        break request
                    }
                    context.log(.debug, "Relationship is one-to-many: \(sequence.label)")
                    reference[0].sourceAlias = elementAlias
                    reference[0].destinationAlias = sequenceAlias
                    filter = reference[0]
                }
            } else {
                assert(element.alias == conditional.alias)
                context.log(.debug, "Returned to evaluate optional to-many relationship: \(conditional.label)")
                if let key = conditional.key, var reference = conditional.property?.reference {
                    reference[0].sourceAlias = elementAlias
                    reference[0].destinationAlias = sequenceAlias
                    if context.references[key, default: []].append(reference[0]).inserted {
                        context.log(.trace, "Inserted JOIN clause reference (optional to-many): \(reference)")
                    }
                }
                context.log(.debug, "Preceding fragment was an optional relationship: \(conditional.label)")
            }
            let clause = SQL {
                Exists {
                    #if DEBUG
                    if context.shouldMarkStartOfPredicateExpression {
                        let debug = debugVariableIDs(
                            ("path", context.path.last),
                            ("context", context.key),
                            ("sequence", sequence.key),
                            ("element", element.key),
                            ("conditional", conditional.key)
                        )
                        let cardinality = isManyToManyRelationship ? "intermediary" : "reference"
                        """
                        /*
                        SequenceContainsWhere (\(cardinality), #\(context.level), \(debug))
                            - sequence alias: \(sequence.alias ?? "n/a")
                            - element alias: \(element.alias ?? "n/a")
                            - conditional alias: \(conditional.alias ?? "n/a")
                        */
                        """
                    }
                    #endif
                    Select(1)
                    From(elementEntity.name, as: elementAlias)
                    if let joins = context.references[variable.key].take(), !joins.isEmpty {
                        ForEach(joins) {
                            let lhsAlias = $0.lhsAlias ?? $0.lhsTable
                            let rhsAlias = $0.rhsAlias ?? $0.rhsTable
                            """
                            JOIN "\($0.rhsTable)" AS "\(rhsAlias)"
                            ON "\(rhsAlias)"."\($0.rhsColumn)" = "\(lhsAlias)"."\($0.lhsColumn)"
                            """
                        }
                    }
                    Where {
                        if let filter, let lhsAlias = filter.lhsAlias, let rhsAlias = filter.rhsAlias {
                            let lhs = "\(quote(lhsAlias)).\(quote(filter.lhsColumn))"
                            let rhs = "\(quote(rhsAlias)).\(quote(filter.rhsColumn))"
                            "\(lhs) = \(rhs)"
                            And { conditional.clause }
                        } else {
                            conditional.clause
                        }
                    }
                }
            }
            return sequence.copy(
                clause: clause.sql,
                bindings: sequence.bindings + element.bindings + conditional.bindings,
                kind: .existsClause
            )
        default:
            context.log(.warning, "SequenceContainsWhere on JSON columns is not yet supported.")
            return sequence.copy(
                clause: """
                EXISTS (
                    SELECT 1
                    FROM json_each(\(sequence.clause))
                    WHERE json_each.value = \(conditional.clause)
                )
                """,
                bindings: sequence.bindings + element.bindings + conditional.bindings,
                kind: .existsClause
            )
        }
    }
}

/// `$0.flatMap { $0.property == 100 } == true`
extension PredicateExpressions.OptionalFlatMap: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        context.log(.trace, "Unwrapping Optional<\(Wrapped.self)>.Type to \(Wrapped.self).Type.")
        context.log(.trace, "Result type expects \(Output.self).self == \(Result.self).self.")
        let wrapped = self.wrapped.query(&context)
        context.log(.trace, "Wrapped fragment: \(wrapped.description)")
        guard let wrappedKey = wrapped.key, let wrappedAlias = wrapped.alias else {
            return wrapped.invalid("Incomplete OptionalFlatMap.wrapped")
        }
        context.path.append(wrappedKey)
        defer { context.key = context.path.popLast() }
        var transform = self.transform.query(&context)
        context.log(.trace, "Transform fragment: \(transform.description)")
        switch transform.type {
        case is any RelationshipCollection.Type, is any PersistentModel.Type:
            var filter: TableReference?
            guard let transformAlias = transform.alias,
                  let transformEntity = transform.entity else {
                return transform.invalid("Incomplete OptionalFlatMap.transform")
            }
            if var destination = wrapped.property?.reference?[0] {
                destination.sourceAlias = wrappedAlias
                destination.destinationAlias = transformAlias
                filter = destination
                context.log(.debug, "Inserted JOIN clause reference (optional): \(destination)")
            }
            if var inverse = transform.property?.reference?[0] {
                inverse.sourceAlias = transformAlias
                inverse.destinationAlias = wrappedAlias
                if context.references[variable.key, default: []].append(inverse).inserted {
                    context.log(.debug, "Inserted JOIN clause reference (optional inverse): \(inverse)")
                }
            }
            let clause = SQL {
                #if DEBUG
                if context.shouldMarkStartOfPredicateExpression {
                    let debug = debugVariableIDs(
                        ("path", context.path.last),
                        ("context", context.key),
                        ("wrapped", wrapped.key),
                        ("transform", transform.key)
                    )
                    """
                    /*
                    OptionalFlatMap (#\(context.level), \(debug))
                        - wrapped alias: \(wrapped.alias ?? "n/a")
                        - transform alias: \(transform.alias ?? "n/a")
                    */
                    """
                }
                #endif
                Select(1)
                From(transformEntity.name, as: transformAlias)
                if let references = context.references[variable.key].take(), !references.isEmpty {
                    ForEach(references) {
                        let lhsAlias = $0.lhsAlias ?? $0.lhsTable
                        let rhsAlias = $0.rhsAlias ?? $0.rhsTable
                        """
                        JOIN "\($0.rhsTable)" AS "\(rhsAlias)"
                        ON "\(rhsAlias)"."\($0.rhsColumn)" = "\(lhsAlias)"."\($0.lhsColumn)"
                        """
                    }
                }
                Where {
                    if let filter, let lhsAlias = filter.lhsAlias, let rhsAlias = filter.rhsAlias {
                        let lhs = "\(quote(lhsAlias)).\(quote(filter.lhsColumn))"
                        let rhs = "\(quote(rhsAlias)).\(quote(filter.rhsColumn))"
                        "\(lhs) = \(rhs)"
                        And { transform.clause }
                    } else {
                        transform.clause
                    }
                }
            }
            return wrapped.copy(
                clause: clause.sql,
                bindings: wrapped.bindings + transform.bindings,
                kind: .existsClause
            )
        default:
            context.log(.trace, "Unwrapping binded parameter value: \(wrapped.description)")
            if let binding = transform.bindings.popLast() {
                guard binding is Wrapped else {
                    return wrapped.invalid("Unhandled OptionalFlatMap case: \(binding)")
                }
                return wrapped.copy(
                    clause: "TRUE",
                    bindings: wrapped.bindings + transform.bindings,
                    kind: .existsClause
                )
            } else {
                return wrapped.copy(
                    clause: "FALSE",
                    bindings: wrapped.bindings + transform.bindings,
                    kind: .existsClause
                )
            }
        }
    }
}

// MARK: Supported `PredicateExpressions`

/// `+`, `-`, ` *`
extension PredicateExpressions.Arithmetic: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = self.lhs.query(&context)
        let rhs = self.rhs.query(&context)
        let op: String
        switch self.op {
        case .add: op = "+"
        case .subtract: op = "-"
        case .multiply: op = "*"
        @unknown default:
            fatalError(DataStoreError.invalidPredicate.localizedDescription)
        }
        return lhs.copy(
            clause: "(\(lhs.clause) \(op) \(rhs.clause))",
            bindings: lhs.bindings + rhs.bindings,
            kind: .expression
        )
    }
}

/// `$0.contains("label")`
extension PredicateExpressions.CollectionContainsCollection: SQLPredicateExpression
where Base: SQLPredicateExpression,
      Other: SQLPredicateExpression,
      Base.Output == String,
      Other.Output == String {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let base = self.base.query(&context)
        let other = self.other.query(&context)
        return base.copy(
            clause: "(\(base.clause) LIKE '%' || \(other.clause) || '%')",
            bindings: base.bindings + other.bindings,
            kind: .patternMatch
        )
    }
}

/// `<`, `<=`, `>`, `>=`
extension PredicateExpressions.Comparison: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = self.lhs.query(&context)
        let rhs = self.rhs.query(&context)
        let op: String
        switch self.op {
        case .lessThan: op = "<"
        case .lessThanOrEqual: op = "<="
        case .greaterThan: op = ">"
        case .greaterThanOrEqual: op = ">="
        @unknown default:
            fatalError(DataStoreError.invalidPredicate.localizedDescription)
        }
        return lhs.copy(
            clause: "((\(lhs.clause)) \(op) (\(rhs.clause)))",
            bindings: lhs.bindings + rhs.bindings,
            kind: .binaryOperation
        )
    }
}

/// `condition ? true : false`
extension PredicateExpressions.Conditional: SQLPredicateExpression
where Test: SQLPredicateExpression,
      If: SQLPredicateExpression,
      Else: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let test = self.test.query(&context)
        switch test.clause {
        case "TRUE":
            let fragment = self.trueBranch.query(&context)
            context.log(.trace, "Short-circuited conditional is true: \(fragment.description)")
            return fragment.copy(kind: .caseExpression)
        case "FALSE":
            let fragment = self.falseBranch.query(&context)
            context.log(.trace, "Short-circuited conditional is false: \(fragment.description)")
            return fragment.copy(kind: .caseExpression)
        default:
            let trueBranch = self.trueBranch.query(&context)
            let falseBranch = self.falseBranch.query(&context)
            context.log(
                as: .trace,
                input: "Reached conditional default case.",
                metadata: [
                    "test": .string(test.description),
                    "true_branch": .string(trueBranch.description),
                    "false_branch": .string(falseBranch.description)
                ]
            )
            return .init(
                clause: """
                (
                    CASE \(test.clause)
                        WHEN TRUE THEN \(trueBranch.clause)
                        WHEN FALSE THEN \(falseBranch.clause)
                        ELSE 0
                    END
                )
                """,
                bindings: test.bindings + trueBranch.bindings + falseBranch.bindings,
                kind: .caseExpression
            )
        }
    }
}

// TODO: Implementation is incomplete and does not fully support inheritance yet.

/// `$0 as? Object`
extension PredicateExpressions.ConditionalCast: SQLPredicateExpression
where Input: SQLPredicateExpression, Desired: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let input = self.input.query(&context)
        guard var inputType = input.type else {
            return .invalid
        }
        var desiredType: Any.Type = Desired.self
        context.log(
            as: .debug,
            input: "ConditionalCast from \(Input.Output.self) as? \(Desired.self).",
            metadata: [
                "input": .string(input.description),
                "input_type": "\(inputType)",
                "desired_type": "\(desiredType)",
                "input_output_type": "\(Input.Output.self)",
                "output_type": "\(Output.self)"
            ]
        )
        switch desiredType {
        case is any RelationshipCollection.Type:
            inputType = unwrapArrayMetatype(inputType)
            desiredType = unwrapArrayMetatype(desiredType)
            fallthrough
        case is any PersistentModel.Type:
            guard let desiredType = desiredType as? Desired.Type else {
                return input.copy(
                    clause: input.clause,
                    bindings: input.bindings,
                    type: nil,
                    kind: .expression
                )
            }
            guard let type = desiredType as? any (PersistentModel & SendableMetatype).Type else {
                fatalError()
            }
            context.loadSchemaMetadata(for: type, key: input.key)
            return input.copy(
                clause: input.clause,
                bindings: input.bindings,
                type: type,
                kind: .expression
            )
        default:
            return input.copy(
                clause: input.clause,
                bindings: input.bindings,
                type: Desired.self,
                kind: .expression
            )
        }
    }
}

/// `$0.flag && $1.flag`
extension PredicateExpressions.Conjunction: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = self.lhs.query(&context)
        let rhs = self.rhs.query(&context)
        return lhs.copy(
            clause: SQL {
                Parenthesis {
                    lhs.clause
                    And { rhs.clause }
                }
            }.sql,
            bindings: lhs.bindings + rhs.bindings,
            kind: .logicalOperator
        )
    }
}

/// `$0.collection[0]`
extension PredicateExpressions.CollectionIndexSubscript: SQLPredicateExpression
where Wrapped: SQLPredicateExpression, Index: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let description = "\(Wrapped.self).self, \(Index.self).self"
        let wrapped = self.wrapped.query(&context)
        let index = self.index.query(&context)
        let wrappedClause = wrapped.clause
        let indexClause = index.clause
        context.log(
            as: .debug,
            input: "CollectionIndexSubscript fragments: \(description)",
            metadata: ["wrapped": .string(wrapped.description), "index": .string(index.description)]
        )
        let normalizedIndex = """
            CASE WHEN (\(indexClause)) < 0
                THEN json_array_length(\(wrappedClause)) + (\(indexClause))
                ELSE (\(indexClause))
            END
            """
        var expression = "json_extract(\(wrapped.clause), '$[' || (\(normalizedIndex)) || ']')"
        if context.shouldMarkStartOfPredicateExpression {
            let debug = debugVariableIDs(("context", context.key))
            expression = "/* CollectionIndexSubscript (#\(context.level), \(debug)) */ " + expression
        }
        return wrapped.copy(
            clause: expression,
            bindings: wrapped.bindings
            + index.bindings
            + wrapped.bindings
            + index.bindings
            + index.bindings,
            kind: .expression
        )
    }
}

/// `$0.collection[1...9]`
extension PredicateExpressions.CollectionRangeSubscript: SQLPredicateExpression
where Wrapped: SQLPredicateExpression, Range: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let wrapped = self.wrapped.query(&context)
        let range = self.range.query(&context)
        let outputType = unwrapOptionalMetatype(Range.Output.self)
        let isClosedRange: Bool
        switch true {
        case outputType == Swift.ClosedRange<Int>.self:
            isClosedRange = true
        case outputType == Swift.Range<Int>.self:
            isClosedRange = false
        default:
            context.log(.warning, "Unsupported CollectionRangeSubscript range type: \(outputType)")
            return .invalid
        }
        let count = range.bindings.count
        guard count == 2 else {
            context.log(
                .warning,
                "CollectionRangeSubscript expected 2 range bindings, got \(count).",
                metadata: ["range": .string(range.description)]
            )
            return .invalid
        }
        let description = "\(Wrapped.self).self, \(outputType).self"
        context.log(
            as: .debug,
            input: "CollectionRangeSubscript fragments: \(description)",
            metadata: [
                "wrapped": .string(wrapped.description),
                "range": .string(range.description),
            ]
        )
        let upperBoundForBetween = isClosedRange ? "(bounds.upperbound)" : "((bounds.upperbound) - 1)"
        var expression = """
            (
                SELECT json_group_array(element.value ORDER BY CAST(element.key AS INTEGER))
                FROM json_each(\(wrapped.clause)) AS element
                CROSS JOIN (
                    SELECT
                    CAST(? AS INTEGER) AS lowerbound,
                    CAST(? AS INTEGER) AS upperbound,
                    json_array_length(\(wrapped.clause)) AS length
                ) AS bounds
                WHERE bounds.lowerbound >= 0
                AND bounds.upperbound >= 0
                AND CAST(element.key AS INTEGER)
                BETWEEN (bounds.lowerbound) AND \(upperBoundForBetween)
            )
            """
        if context.shouldMarkStartOfPredicateExpression {
            let debug = debugVariableIDs(("context", context.key))
            expression = "/* CollectionRangeSubscript<\(outputType)> (#\(context.level), \(debug)) */\n"
            + expression
        }
        return wrapped.copy(
            clause: expression,
            bindings: wrapped.bindings + range.bindings + wrapped.bindings,
            kind: .expression
        )
    }
}

/// `$0.languages["name", default: "Swift"] == "Swift"`
extension PredicateExpressions.DictionaryKeyDefaultValueSubscript: SQLPredicateExpression
where Wrapped: SQLPredicateExpression,
      Key: SQLPredicateExpression,
      Default: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let wrapped = self.wrapped.query(&context)
        let key = self.key.query(&context)
        let `default` = self.default.query(&context)
        guard let wrappedType = wrapped.type else {
            return .invalid
        }
        let wrappedInput = {
            switch SQLType(for: unwrapOptionalMetatype(wrappedType)) {
            case .text, .blob, nil: wrapped.clause
            default: "CAST(\(wrapped.clause) AS TEXT)"
            }
        }()
        guard let keyType = key.type else {
            return .invalid
        }
        let sql: String
        do {
            switch SQLType(for: unwrapOptionalMetatype(keyType)) {
            case .integer:
                sql = "'$[' || (\(cast(key.clause, as: .integer))) || ']'"
            case .text:
                sql = "'$.' || json_quote(\(cast(key.clause, as: .text)))"
            default:
                sql = """
                    (
                        SELECT CASE
                            WHEN typeof(key) = 'integer' THEN '$[' || key || ']'
                            ELSE '$.' || json_quote(CAST(key AS TEXT))
                        END
                        FROM (SELECT \(key.clause) AS key)
                    )
                    """
            }
        }
        let outputType = SQLType(for: unwrapOptionalMetatype(Output.self))
        let extracted = cast("json_extract(\(wrappedInput), \(sql))", as: outputType)
        let defaultExpression = (`default`.clause == "NULL")
        ? `default`.clause
        : cast(`default`.clause, as: outputType)
        var expression = "COALESCE(\(extracted), \(defaultExpression))"
        if context.shouldMarkStartOfPredicateExpression {
            let debug = debugVariableIDs(("context", context.key))
            expression = "/* DictionaryKeyDefaultValueSubscript (#\(context.level), \(debug)) */\n"
            + expression
        }
        func cast(_ clause: String, as sqlType: SQLType?) -> String {
            guard let sqlType, sqlType != .null else { return clause }
            return "CAST(\(clause) AS \(sqlType.description))"
        }
        return wrapped.copy(
            clause: expression,
            bindings: wrapped.bindings + key.bindings + `default`.bindings,
            type: Output.self,
            kind: .expression
        )
    }
}

/// `$0.languages["name"] == "Swift"`
extension PredicateExpressions.DictionaryKeySubscript: SQLPredicateExpression
where Wrapped: SQLPredicateExpression, Key: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let wrapped = self.wrapped.query(&context)
        let key = self.key.query(&context)
        guard let wrappedType = wrapped.type else {
            return .invalid
        }
        let wrappedInput = {
            switch SQLType(for: unwrapOptionalMetatype(wrappedType)) {
            case .text, .blob, nil: wrapped.clause
            default: "CAST(\(wrapped.clause) AS TEXT)"
            }
        }()
        guard let keyType = key.type else {
            return .invalid
        }
        let sql: String
        do {
            switch SQLType(for: unwrapOptionalMetatype(keyType)) {
            case .integer:
                sql = "'$[' || (\(cast(key.clause, as: .integer))) || ']'"
            case .text:
                sql = "'$.' || json_quote(\(cast(key.clause, as: .text)))"
            default:
                sql = """
                    (
                        SELECT CASE
                            WHEN typeof(key) = 'integer' THEN '$[' || key || ']'
                            ELSE '$.' || json_quote(CAST(key AS TEXT))
                        END
                        FROM (SELECT \(key.clause) AS key)
                    )
                    """
            }
        }
        var expression = "json_extract(\(wrappedInput), \(sql))"
        if let outputType = SQLType(for: unwrapOptionalMetatype(Output.self)) {
            switch outputType {
            case .integer, .real, .text, .blob:
                expression = "CAST(\(expression) AS \(outputType.description))"
            case .null: break
            default: fatalError()
            }
        }
        if context.shouldMarkStartOfPredicateExpression {
            let debug = debugVariableIDs(("context", context.key))
            expression = "\n/* DictionaryKeySubscript (#\(context.level), \(debug)) */\n"
            + expression
        }
        func cast(_ clause: String, as sqlType: SQLType?) -> String {
            guard let sqlType, sqlType != .null else { return clause }
            return "CAST(\(clause) AS \(sqlType.description))"
        }
        return wrapped.copy(
            clause: expression,
            bindings: wrapped.bindings + key.bindings,
            type: Output.self,
            kind: .expression
        )
    }
}

/// `$0.flag || $1.flag`
extension PredicateExpressions.Disjunction: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = self.lhs.query(&context)
        let rhs = self.rhs.query(&context)
        return lhs.copy(
            clause: SQL {
                Parenthesis {
                    lhs.clause
                    Or { rhs.clause }
                }
            }.sql,
            bindings: lhs.bindings + rhs.bindings,
            kind: .logicalOperator
        )
    }
}

/// `$0.property == "label"`
extension PredicateExpressions.Equal: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> SQLPredicateFragment {
        var lhs = self.lhs.query(&context)
        var rhs = self.rhs.query(&context)
        var clause = ""
        var removedValue: Any?
        switch (lhs.clause, rhs.clause) {
        case ("?", "?") where lhs.bindings.count == 1 && rhs.bindings.count == 1:
            if lhs.type == rhs.type,
               let lhsValue = lhs.bindings.last as? SQLValue,
               let rhsValue = rhs.bindings.last as? SQLValue {
                _ = lhs.bindings.popLast()
                _ = rhs.bindings.popLast()
                clause = lhsValue == rhsValue ? "TRUE" : "FALSE"
            } else {
                clause = "(? = ?)"
            }
        case ("?", let rhsClause) where lhs.bindings.count == 1:
            guard let lhsValue = lhs.bindings.last else {
                clause = (rhsClause == "NULL") ? "TRUE" : "FALSE"
                break
            }
            if rhsClause == "NULL" {
                if let lhsValue = lhsValue as? SQLValue, lhsValue == .null {
                    clause = "TRUE"
                    removedValue = lhs.bindings.popLast()
                } else {
                    clause = "FALSE"
                    removedValue = lhs.bindings.popLast()
                }
                break
            }
            if let lhsValue = lhsValue as? SQLValue, lhs.type is Bool.Type {
                if lhsValue == SQLValue(any: true) {
                    clause = "(\(rhsClause))"
                    removedValue = lhs.bindings.popLast()
                    break
                }
                if lhsValue == SQLValue(any: false) {
                    clause = "(NOT \(rhsClause))"
                    removedValue = lhs.bindings.popLast()
                    break
                }
            }
            clause = "(? = \(rhsClause))"
        case (let lhsClause, "?") where rhs.bindings.count == 1:
            guard let rhsValue = rhs.bindings.last else {
                clause = (lhsClause == "NULL") ? "TRUE" : "FALSE"
                break
            }
            if lhsClause == "NULL" {
                if let rhsValue = rhsValue as? SQLValue, rhsValue == .null {
                    clause = "TRUE"
                    removedValue = rhs.bindings.popLast()
                } else {
                    clause = "FALSE"
                    removedValue = rhs.bindings.popLast()
                }
                break
            }
            if let rhsValue = rhsValue as? SQLValue, rhs.type is Bool.Type {
                if rhsValue == SQLValue(any: true) {
                    clause = "(\(lhsClause))"
                    removedValue = rhs.bindings.popLast()
                    break
                }
                if rhsValue == SQLValue(any: false) {
                    clause = "(NOT \(lhsClause))"
                    removedValue = rhs.bindings.popLast()
                    break
                }
            }
            clause = "(\(lhsClause) = ?)"
        case ("NULL", "NULL"):
            clause = "TRUE"
        case ("NULL", _):
            clause = "(\(rhs.clause) IS NULL)"
        case (_, "NULL"):
            clause = "(\(lhs.clause) IS NULL)"
        case ("", _):
            context.log(.warning, "Empty LHS fragment is not allowed.")
            clause = "(1 = 1)"
        case (_, ""):
            context.log(.warning, "Empty RHS fragment is not allowed.")
            clause = "(1 = 1)"
        default:
            clause = "(\(lhs.clause) = \(rhs.clause))"
        }
        if let removedValue {
            clause += " /* (removed: \(removedValue)) */"
        }
        return lhs.copy(
            clause: clause,
            bindings: lhs.bindings + rhs.bindings,
            kind: .binaryOperation
        )
    }
}

/// `$0.sequence.filter { $0.property == true }`
extension PredicateExpressions.Filter: SQLPredicateExpression
where LHS: SQLPredicateExpression,
      RHS: SQLPredicateExpression,
      LHS.Output: Sequence,
      RHS.Output == Bool {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        fatalError("Filter has not been implemented.")
    }
}

/// `$0.property / 100.0`
extension PredicateExpressions.FloatDivision: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = self.lhs.query(&context)
        let rhs = self.rhs.query(&context)
        return lhs.copy(
            clause: "(\(lhs.clause) / \(rhs.clause))",
            bindings: lhs.bindings + rhs.bindings,
            kind: .expression
        )
    }
}

/// `$0!.property`
extension PredicateExpressions.ForcedUnwrap: SQLPredicateExpression
where Inner: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        var inner = self.inner.query(&context)
        context.log(.debug, "Force unwrapping value: \(inner.bindings) as! \(Output.self).self")
        switch inner.kind {
        case .bindParameter where inner.bindings.count == 1:
            guard let unwrappedValue = inner.bindings.popLast() as? Output else {
                fatalError("Unexpected bindable value for ForcedUnwrap: \(inner.description)")
            }
            return inner.copy(bindings: [unwrappedValue], type: Output.self, kind: .bindParameter)
        default:
            return inner.invalid("ForcedUnwrap is not supported for non-bindable expressions.")
        }
    }
}

/// `$0.property / 2`
extension PredicateExpressions.IntDivision: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = self.lhs.query(&context)
        let rhs = self.rhs.query(&context)
        return lhs.copy(
            clause: "(CAST(\(lhs.clause) AS INTEGER) / CAST(\(rhs.clause) AS INTEGER))",
            bindings: lhs.bindings + rhs.bindings,
            kind: .expression
        )
    }
}

/// `$0.property % 10`
extension PredicateExpressions.IntRemainder: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = self.lhs.query(&context)
        let rhs = self.rhs.query(&context)
        return lhs.copy(
            clause: "(CAST(\(lhs.clause) AS INTEGER) % CAST(\(rhs.clause) AS INTEGER))",
            bindings: lhs.bindings + rhs.bindings,
            kind: .expression
        )
    }
}

/// `!$0.isEmpty`
extension PredicateExpressions.Negation: SQLPredicateExpression
where Wrapped: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let wrapped = self.wrapped.query(&context)
        return wrapped.copy(clause: "(NOT \(wrapped.clause))", kind: .unaryOperation)
    }
}

/// `$0.property != "label"`
extension PredicateExpressions.NotEqual: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> SQLPredicateFragment {
        var lhs = self.lhs.query(&context)
        var rhs = self.rhs.query(&context)
        var clause = ""
        var removedValue: Any?
        switch (lhs.clause, rhs.clause) {
        case ("?", "?") where lhs.bindings.count == 1 && rhs.bindings.count == 1:
            if lhs.type == rhs.type,
               let lhsValue = lhs.bindings.last as? SQLValue,
               let rhsValue = rhs.bindings.last as? SQLValue {
                _ = lhs.bindings.popLast()
                _ = rhs.bindings.popLast()
                clause = lhsValue == rhsValue ? "FALSE" : "TRUE"
            } else {
                clause = "(? != ?)"
            }
        case ("?", let rhsClause) where lhs.bindings.count == 1:
            guard let lhsValue = lhs.bindings.last else {
                clause = (rhsClause == "NULL") ? "FALSE" : "TRUE"
                break
            }
            if rhsClause == "NULL" {
                if let lhsValue = lhsValue as? SQLValue, lhsValue == .null {
                    clause = "FALSE"
                    removedValue = lhs.bindings.popLast()
                } else {
                    clause = "TRUE"
                    removedValue = lhs.bindings.popLast()
                }
                break
            }
            if let lhsValue = lhsValue as? SQLValue, lhs.type is Bool.Type {
                if lhsValue == SQLValue(any: true) {
                    clause = "(NOT \(rhsClause))"
                    removedValue = lhs.bindings.popLast()
                    break
                }
                if lhsValue == SQLValue(any: false) {
                    clause = "(\(rhsClause))"
                    removedValue = lhs.bindings.popLast()
                    break
                }
            }
            clause = "(? != \(rhsClause))"
        case (let lhsClause, "?") where rhs.bindings.count == 1:
            guard let rhsValue = rhs.bindings.last else {
                clause = (lhsClause == "NULL") ? "FALSE" : "TRUE"
                break
            }
            if lhsClause == "NULL" {
                if let rhsValue = rhsValue as? SQLValue, rhsValue == .null {
                    clause = "FALSE"
                    removedValue = rhs.bindings.popLast()
                    break
                } else {
                    clause = "TRUE"
                    removedValue = rhs.bindings.popLast()
                    break
                }
            }
            if let rhsValue = rhsValue as? SQLValue, rhs.type is Bool.Type {
                if rhsValue == SQLValue(any: true) {
                    clause = "(NOT \(lhsClause))"
                    removedValue = rhs.bindings.popLast()
                    break
                }
                if rhsValue == SQLValue(any: false) {
                    clause = "(\(lhsClause))"
                    removedValue = rhs.bindings.popLast()
                    break
                }
            }
            clause = "(\(lhsClause) != ?)"
        case ("NULL", "NULL"):
            clause = "FALSE"
        case ("NULL", _):
            clause = "(\(rhs.clause) IS NOT NULL)"
        case (_, "NULL"):
            clause = "(\(lhs.clause) IS NOT NULL)"
        case ("", _):
            context.log(.warning, "Empty LHS fragment is not allowed.")
            clause = "(1 = 0)"
        case (_, ""):
            context.log(.warning, "Empty RHS fragment is not allowed.")
            clause = "(1 = 0)"
        default:
            clause = "(\(lhs.clause) != \(rhs.clause))"
        }
        if let removedValue {
            clause += " /* (removed: \(removedValue)) */"
        }
        return lhs.copy(
            clause: clause,
            bindings: lhs.bindings + rhs.bindings,
            kind: .binaryOperation
        )
    }
}

/// `$0.property ?? "other"
extension PredicateExpressions.NilCoalesce: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let lhs = self.lhs.query(&context)
        let rhs = self.rhs.query(&context)
        return lhs.copy(
            clause: "COALESCE(\(lhs.clause), \(rhs.clause))",
            bindings: lhs.bindings + rhs.bindings,
            kind: .functionCall
        )
    }
}

/// `$0.property == nil`
extension PredicateExpressions.NilLiteral: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        return .init(clause: "NULL", kind: .literal)
    }
}

/// `$0.sequence["scores"].max() >= 50`
extension PredicateExpressions.SequenceMaximum: SQLPredicateExpression
where Elements: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let description = "\(Elements.self).self"
        let sequence = self.elements.query(&context)
        let clause = sequence.clause
        let bindings = sequence.bindings
        context.log(.debug, "SequenceMaximum elements: \(description) \(sequence.description)")
        var sql = """
            (
                SELECT COALESCE(
                    (
                        SELECT MAX(CAST(element.value AS REAL))
                        FROM json_each(\(clause)) AS element
                        WHERE json_type(element.value) IN ('integer','real')
                    ),
                        (
                            SELECT MAX(element.value)
                            FROM json_each(\(clause)) AS element
                            WHERE json_type(element.value) = 'text'
                    )
                )
            )
            """
        if context.shouldMarkStartOfPredicateExpression {
            let debug = debugVariableIDs(("context", context.key))
            sql = "/* SequenceMaximum (#\(context.level), \(debug)) */\n" + sql
        }
        return sequence.copy(
            clause: sql,
            bindings: bindings + bindings,
            kind: .expression
        )
    }
}

/// `$0.sequence["scores"].min() <= 50`
extension PredicateExpressions.SequenceMinimum: SQLPredicateExpression
where Elements: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let description = "\(Elements.self).self"
        let sequence = self.elements.query(&context)
        let clause = sequence.clause
        let bindings = sequence.bindings
        context.log(.debug, "SequenceMinimum elements: \(description) \(sequence.description)")
        var sql = """
            (
                SELECT COALESCE(
                    (
                        SELECT MIN(CAST(element.value AS REAL))
                        FROM json_each(\(clause)) AS element
                        WHERE json_type(element.value) IN ('integer','real')
                    ),
                        (
                            SELECT MIN(element.value)
                            FROM json_each(\(clause)) AS element
                            WHERE json_type(element.value) = 'text'
                    )
                )
            )
            """
        if context.shouldMarkStartOfPredicateExpression {
            let debug = debugVariableIDs(("context", context.key))
            sql = "/* SequenceMinimum (#\(context.level), \(debug)) */\n" + sql
        }
        return sequence.copy(
            clause: sql,
            bindings: bindings + bindings,
            kind: .expression
        )
    }
}

/// `$0.starts(with: "a")`
extension PredicateExpressions.SequenceStartsWith: SQLPredicateExpression
where Base: SQLPredicateExpression,
      Prefix: SQLPredicateExpression,
      Base.Output == String,
      Prefix.Output == String {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let base = self.base.query(&context)
        let prefix = self.prefix.query(&context)
        return .init(
            clause: "(\(base.clause) LIKE \(prefix.clause) || '%')",
            bindings: base.bindings + prefix.bindings,
            kind: .patternMatch
        )
    }
}

/// `$0.caseInsensitiveCompare("label")`
extension PredicateExpressions.StringCaseInsensitiveCompare: SQLPredicateExpression
where Root: SQLPredicateExpression, Other: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let root = self.root.query(&context)
        let other = self.other.query(&context)
        return root.copy(
            clause: "(\(root.clause) COLLATE NOCASE = \(other.clause))",
            bindings: root.bindings + other.bindings,
            kind: .binaryOperation
        )
    }
}

/// `$0.localizedStandardCompare("label")`
extension PredicateExpressions.StringLocalizedCompare: SQLPredicateExpression
where Root: SQLPredicateExpression, Other: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let root = self.root.query(&context)
        let other = self.other.query(&context)
        return root.copy(
            clause: "(\(root.clause) COLLATE NOCASE = \(other.clause))",
            bindings: root.bindings + other.bindings,
            kind: .binaryOperation
        )
    }
}

/// `$0.property.localizedStandardContains("label")`
extension PredicateExpressions.StringLocalizedStandardContains: SQLPredicateExpression
where Root: SQLPredicateExpression, Other: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let root = self.root.query(&context)
        let other = self.other.query(&context)
        return root.copy(
            clause: "(\(root.clause) LIKE '%' || \(other.clause) || '%' COLLATE NOCASE)",
            bindings: root.bindings + other.bindings,
            kind: .binaryOperation
        )
    }
}

/// `$0 is T`
extension PredicateExpressions.TypeCheck: SQLPredicateExpression
where Input: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        context.log(as: .trace, input: "TypeCheck", Input.self, Desired.self)
        let input = self.input.query(&context)
        if let rootAlias = input.alias,
           input.type is any PersistentModel.Type,
           let subclass = Desired.self as? any PersistentModel.Type {
            let leafTableName = Schema.entityName(for: subclass)
            let clause = """
                EXISTS (
                    SELECT 1 FROM "\(leafTableName)"
                    WHERE "\(leafTableName)"."\(pk)" = \(quote(rootAlias))."\(pk)"
                )
                """
            return input.copy(clause: clause, kind: .subquery)
        }
        switch input.type {
        case is Desired.Type:
            return input.copy(clause: "TRUE", kind: .binaryOperation)
        default:
            return input.copy(clause: "FALSE", kind: .binaryOperation)
        }
    }
}

/// `-$0.property`
extension PredicateExpressions.UnaryMinus: SQLPredicateExpression
where Wrapped: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let wrapped = self.wrapped.query(&context)
        return wrapped.copy(clause: "(-\(wrapped.clause))", kind: .unaryOperation)
    }
}

// MARK: Unsupported `PredicateExpressions`

/// `(2000...2025).contains($0.year)`
extension PredicateExpressions.ClosedRange: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        fatalError("ClosedRange is not supported.")
    }
}

/// `(2000...2025).contains($0.year)`
extension PredicateExpressions.Range: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        fatalError("Range is not supported.")
    }
}

/// `(2000...2025).contains($0.year)`
extension PredicateExpressions.RangeExpressionContains: SQLPredicateExpression
where RangeExpression: SQLPredicateExpression, Element: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        fatalError("RangeExpressionContains is not supported.")
    }
}

/// `$0.sequence.allSatisfy { $0.contains("value") }`
extension PredicateExpressions.SequenceAllSatisfy: SQLPredicateExpression
where LHS: SQLPredicateExpression, RHS: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        fatalError("SequenceAllSatisfy is not supported.")
    }
}

/// `$0.name.contains(/A[0-9]{10}/)`
extension PredicateExpressions.StringContainsRegex: SQLPredicateExpression
where Subject: SQLPredicateExpression, Regex: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        fatalError("StringContainsRegex is not supported.")
    }
}
