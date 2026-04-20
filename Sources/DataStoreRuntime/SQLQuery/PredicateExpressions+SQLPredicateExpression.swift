//
//  PredicateExpressions+SQLPredicateExpression.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Collections
private import DataStoreCore
private import DataStoreSQL
private import DataStoreSupport
private import Logging
private import SQLiteStatement
private import SQLSupport
private import Synchronization
internal import Foundation

#if swift(>=6.2)
private import SwiftData
#else
@preconcurrency private import SwiftData
#endif

private typealias ForEach = SQLForEach

/// `0`
///
/// The input value referenced in a predicate expression.
/// - `value` is `Output` type.
extension PredicateExpressions.Value: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        context.log(.trace, "Received bindable value: \(value) as \(Output.self).self")
        if let value = self.value as? (any Hashable) {
            context.hasher.combine(value)
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
        case let sqlPassthrough as SQL:
            context.sqlPassthrough = sqlPassthrough
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
        case let model as any PersistentModel:
            // Receives a model to use with a key path or other expression.
            clause = "?"
            bindings = [model]
        case _ where SQLType(equivalentRawValueType: type(of: value)) == nil:
            // This path is followed by the key path expression to extract its bindable value.
            context.log(.debug, "Binding as Any: \(type(of: value)) == \(Output.self)")
            clause = "?"
            bindings = [value]
        default:
            // This path defaults to being the bindable value.
            context.log(.debug, "Binding as SQLValue: \(type(of: value)) == \(Output.self)")
            clause = "?"
            bindings = [SQLValue(any: value)]
        }
        return .init(clause: clause, bindings: bindings, type: Output.self, kind: .bindParameter)
    }
}

/// `$0`
///
/// The closure-scoped parameter that is uniquely identified by its `VariableID`.
extension PredicateExpressions.Variable: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        context.log(.trace, "Entering closure in variable: Output.Type is \(Output.self).self")
        switch Output.self as Any.Type {
        case var type where type is any RelationshipCollection.Type:
            type = unwrapArrayMetatype(type)
            context.log(.trace, "Variable conforms to RelationshipCollection.Type: \(type).self")
            fallthrough
        case let type where type is any PersistentModel.Type:
            guard let type = type as? any (PersistentModel & SendableMetatype).Type,
                  let entity = context.schema.entity(for: type) ?? Schema([type]).entity(for: type) else {
                preconditionFailure("Schema could not resolve entity for \(type).self variable.")
            }
            context.log(.trace, "Variable conforms to PersistentModel.Type: \(type).self")
            let alias = context.createTableAlias(key, entity.name)
            context.loadSchemaMetadata(for: type, key: key)
            context.hasher.combine(entity.name)
            return .init(clause: alias, key: key, alias: alias, type: Output.self, entity: entity, kind: .scope)
        default:
            /*
             This path is for entering a closure of a collection where the element is a value type.
             e.g. `model.values.contains(where: { $0.rawValue == rawValue })`
             */
            context.log(.trace, "Variable is a value type: \(Output.self).self")
            return .init(clause: "json_each.value", key: key, type: Output.self, kind: .scope)
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
        guard kind == nil else { return resolveComputedProperty(&context, root) }
        switch root.kind {
        case .scope:
            // Applies the key path on the closure parameter.
            guard let type = Root.Output.self as? any PersistentModel.Type else {
                context.log(.notice, "Root.Output.self is not a PersistentModel.Type: \(description)")
                // Fragment is an element value in `SequenceContainsWhere`.
                return root
            }
            context.loadSchemaMetadata(for: type)
            guard let property = try? context[keyPath, type] else {
                preconditionFailure("No PropertyMetadata found in schema: \(description)")
            }
            context.log(.trace, "Key path resolved top-level property: \(property.name) \(description)")
            return resolveStoredProperty(&context, root.copy(property: property))
        case .columnReference:
            assert(root.entity != nil, "No entity associated to fragment when accessing a property.")
            guard let keyPath = root.keyPath else {
                return root.invalid("Root did not provide the root key path", description)
            }
            switch root.property?.metadata {
            case let relationship as Schema.Relationship:
                var resolvedType: Any.Type = root.type ?? Root.Output.self
                resolvedType = unwrapOptionalMetatype(resolvedType)
                if let collectionType = resolvedType as? any RelationshipCollection.Type {
                    resolvedType = unwrapArrayMetatype(collectionType)
                }
                guard let type = resolvedType as? any PersistentModel.Type else {
                    return root.invalid("Root.Output.self is not a PersistentModel.Type", description)
                }
                assert(Root.Output.self is any PersistentModel.Type)
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
                return resolveStoredProperty(&context, root.copy(
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
                return resolveStoredProperty(&context, root.copy(
                    entity: entity,
                    property: result.property,
                    keyPath: result.keyPath
                ))
            default:
                fatalError("Unhandled case for bridging key path: \(description)")
            }
        case .bindParameter where root.bindings.last is Root.Output:
            guard let object = root.bindings.popLast() as? Root.Output else {
                preconditionFailure("Expected Root.Output.Type binding: \(root.bindings)")
            }
            let value = object[keyPath: keyPath]
            context.log(.trace, "Extracted bindable value from object: \(description) = \(value)")
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
    
    private func resolveStoredProperty<T>(_ context: inout Context<T>, _ root: consuming Fragment) -> Fragment {
        let description = "\\\(Root.Output.self).\(Output.self) == \(keyPath) -> \(root.description)"
        guard let sourceAlias = root.alias, let property = root.property else {
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
        case is Schema.Attribute where property.isInherited:
            context.log(.debug, "Property is an inherited attribute: \(description)")
            guard let entity = root.entity,
                  let ownerEntity = context.entityOwningProperty(named: property.name, startingAt: entity),
                  let inheritedAlias = context.createInheritedAlias(root.key, from: entity, as: sourceAlias, to: ownerEntity) else {
                return .invalid("Unable to resolve inherited attribute", description)
            }
            return root.copy(
                clause: "\(quote(inheritedAlias)).\(quote(property.name))",
                alias: inheritedAlias,
                keyPath: keyPath,
                kind: .columnReference
            )
        case is Schema.Attribute where property.enclosing is Schema.Relationship:
            context.log(.debug, "Property is an attribute of a relationship: \(description)")
            clause = "\(quote(root.clause)).\(quote(property.name))"
        case is Schema.Attribute where property.enclosing is Schema.CompositeAttribute:
            context.log(.debug, "Property is an attribute of a composite: \(description)")
            guard !(unwrapOptionalMetatype(property.enclosing!.valueType) is any RawRepresentable.Type) else {
                clause = "\(quote(sourceAlias)).\(quote(property.enclosing!.name))"
                break
            }
            clause = context.options.contains(.useFallbackOnCompositeAttributes) ?
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
    
    private func resolveComputedProperty<T>(_ context: inout Context<T>, _ root: consuming Fragment) -> Fragment {
        let isFiltered = root.kind == .subquery
        let filteredAlias = isFiltered ? root.alias : nil
        let filterSQL = isFiltered && !root.clause.isEmpty && root.clause != "[INVALID]"
        ? " AND (\(root.clause))"
        : ""
        switch kind {
        case .collectionFirst:
            return resolve { _, _ in
                "NULL"
            } referencingColumn: { lhsAlias, lhsTable, foreignKeyColumn, rhsAlias in
                let alias = filteredAlias ?? lhsAlias
                return """
                SELECT "\(alias)"."\(pk)" -- collectionFirst (reference)
                FROM "\(lhsTable)" AS "\(alias)"
                WHERE "\(alias)"."\(foreignKeyColumn)" = "\(rhsAlias)"."\(pk)"\(filterSQL)
                LIMIT 1
                """
            } referencingIntermediaryTable: { join, lhs, rhs in
                let elementAlias = filteredAlias ?? rhs.alias
                let elementJoin = filterSQL.isEmpty ? "" : """
                    \nJOIN "\(rhs.table)" AS "\(elementAlias)"
                    ON "\(elementAlias)"."\(pk)" = "\(join.alias)"."\(rhs.column)"
                    """
                return """
                SELECT "\(elementAlias)"."\(pk)" -- collectionFirst (intermediary)
                FROM "\(join.table)" AS "\(join.alias)"\(elementJoin)
                WHERE "\(join.alias)"."\(lhs.column)" = "\(lhs.alias)"."\(pk)"\(filterSQL)
                LIMIT 1
                """
            }
        case .bidirectionalCollectionLast:
            return resolve { _, _ in
                "NULL"
            } referencingColumn: { lhsAlias, lhsTable, foreignKeyColumn, rhsAlias in
                let alias = filteredAlias ?? lhsAlias
                return """
                SELECT "\(alias)"."\(pk)" -- bidirectionalCollectionLast (reference)
                FROM "\(lhsTable)" AS "\(alias)"
                WHERE "\(alias)"."\(foreignKeyColumn)" = "\(rhsAlias)"."\(pk)"\(filterSQL)
                ORDER BY "\(alias)"."\(pk)" DESC
                LIMIT 1
                """
            } referencingIntermediaryTable: { join, lhs, rhs in
                let elementAlias = filteredAlias ?? rhs.alias
                let elementJoin = filterSQL.isEmpty ? "" : """
                    \nJOIN "\(rhs.table)" AS "\(elementAlias)"
                    ON "\(elementAlias)"."\(pk)" = "\(join.alias)"."\(rhs.column)"
                    """
                return """
                SELECT "\(elementAlias)"."\(pk)" -- bidirectionalCollectionLast (intermediary)
                FROM "\(join.table)" AS "\(join.alias)"\(elementJoin)
                WHERE "\(join.alias)"."\(lhs.column)" = "\(lhs.alias)"."\(pk)"\(filterSQL)
                ORDER BY "\(elementAlias)"."\(pk)" DESC
                LIMIT 1
                """
            }
        case .collectionCount:
            return resolve { _, _ in
                "NULL"
            } referencingColumn: { lhsAlias, lhsTable, foreignKeyColumn, rhsAlias in
                let alias = filteredAlias ?? lhsAlias
                return """
                SELECT COUNT(*) -- collectionCount (reference)
                FROM "\(lhsTable)" AS "\(alias)"
                WHERE "\(alias)"."\(foreignKeyColumn)" = "\(rhsAlias)"."\(pk)"\(filterSQL)
                """
            } referencingIntermediaryTable: { join, lhs, rhs in
                let elementAlias = filteredAlias ?? rhs.alias
                let elementJoin = filterSQL.isEmpty ? "" : """
                    \nJOIN "\(rhs.table)" AS "\(elementAlias)"
                    ON "\(elementAlias)"."\(pk)" = "\(join.alias)"."\(rhs.column)"
                    """
                return """
                SELECT COUNT(*) -- collectionCount (intermediary)
                FROM "\(join.table)" AS "\(join.alias)"\(elementJoin)
                WHERE "\(join.alias)"."\(lhs.column)" = "\(lhs.alias)"."\(pk)"\(filterSQL)
                """
            }
        case .collectionIsEmpty:
            return resolve { sourceAlias, attribute in
                "\(quote(sourceAlias)).\(quote(attribute)) = ''"
            } referencingColumn: { lhsAlias, lhsTable, foreignKeyColumn, rhsAlias in
                let alias = filteredAlias ?? lhsAlias
                return """
                NOT EXISTS ( -- collectionIsEmpty (reference)
                    SELECT 1
                    FROM "\(lhsTable)" AS "\(alias)"
                    WHERE "\(alias)"."\(foreignKeyColumn)" = "\(rhsAlias)"."\(pk)"\(filterSQL)
                )
                """
            } referencingIntermediaryTable: { join, lhs, rhs in
                let elementAlias = filteredAlias ?? rhs.alias
                let elementJoin = filterSQL.isEmpty ? "" : """
                    \nJOIN "\(rhs.table)" AS "\(elementAlias)"
                    ON "\(elementAlias)"."\(pk)" = "\(join.alias)"."\(rhs.column)"
                    """
                return """
                NOT EXISTS ( -- collectionIsEmpty (intermediary)
                    SELECT 1
                    FROM "\(join.table)" AS "\(join.alias)"\(elementJoin)
                    WHERE "\(join.alias)"."\(lhs.column)" = "\(lhs.alias)"."\(pk)"\(filterSQL)
                )
                """
            }
        default:
            fatalError("Unknown KeyPath.kind case: \(String(describing: kind))")
        }
        // To-many relationship types are wrapped as `Array<Root.Output>.self`.
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
            guard Root.Output.self is any RelationshipCollection.Type || Root.Output.self is any PersistentModel.Type else {
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
                    return root.copy(clause: "(\(sql))", kind: .functionCall)
                case let reference?:
                    let joinTuple = (
                        context.createTableAlias(root.key, reference[0].destinationTable),
                        reference[0].destinationTable
                    )
                    #if true
                    let sourceTuple = (
                        context.createTableAlias(root.key, reference[0].sourceTable),
                        reference[0].sourceTable,
                        reference[0].destinationColumn
                    )
                    let destinationTuple = (
                        context.createTableAlias(root.key, reference[1].destinationTable),
                        reference[1].destinationTable,
                        reference[1].sourceColumn
                    )
                    #else
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
                    #endif
                    let sql = intermediaryHandler(joinTuple, sourceTuple, destinationTuple)
                    return root.copy(clause: "(\(sql))", kind: .functionCall)
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
                        preconditionFailure("Expected Boolean value from bindings.")
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
                    if context.options.contains(.shouldMarkStartOfPredicateExpression) {
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
                        if context.options.contains(.shouldMarkStartOfPredicateExpression) {
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
                        preconditionFailure("Expected Boolean value from bindings.")
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
                    if context.options.contains(.shouldMarkStartOfPredicateExpression) {
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
                        if context.options.contains(.shouldMarkStartOfPredicateExpression) {
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
                preconditionFailure("Unable to translate predicate into an SQL query.")
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
        assert(element.type is Element.Type, "Element fragment expected to return \(Element.self).self: \(element.description)")
        assert(element.key == self.variable.key, "Element fragment variable closure is misaligned: \(element.description)")
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
                    preconditionFailure("Relationship is missing reference metadata: \(sequence.description)")
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
                    if context.options.contains(.shouldMarkStartOfPredicateExpression) {
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
                            - sequence alias: \(sequence.alias ?? "nil")
                            - element alias: \(element.alias ?? "nil")
                            - conditional alias: \(conditional.alias ?? "nil")
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
            context.log(.debug, "SequenceContainsWhere on JSON columns.")
            return sequence.copy(
                clause: """
                EXISTS (
                    SELECT 1
                    FROM json_each(\(sequence.clause))
                    WHERE \(conditional.clause)
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
        context.log(.trace, "Unwrapping Optional<\(Wrapped.self)>.self to \(Wrapped.self).Type.")
        context.log(.trace, "Result type expects \(Output.self).self == \(Result.self).Type.")
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
                if context.options.contains(.shouldMarkStartOfPredicateExpression) {
                    let debug = debugVariableIDs(
                        ("path", context.path.last),
                        ("context", context.key),
                        ("wrapped", wrapped.key),
                        ("transform", transform.key)
                    )
                    """
                    /*
                    OptionalFlatMap (#\(context.level), \(debug))
                        - wrapped alias: \(wrapped.alias ?? "nil")
                        - transform alias: \(transform.alias ?? "nil")
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
            return wrapped.copy(clause: clause.sql, bindings: wrapped.bindings + transform.bindings, kind: .existsClause)
        default:
            context.log(.trace, "Unwrapping binded parameter value: \(wrapped.description)")
            if let binding = transform.bindings.popLast() {
                guard binding is Wrapped else {
                    return wrapped.invalid("Unhandled OptionalFlatMap case: \(binding)")
                }
                return wrapped.copy(clause: "TRUE", bindings: wrapped.bindings + transform.bindings, kind: .existsClause)
            } else {
                return wrapped.copy(clause: "FALSE", bindings: wrapped.bindings + transform.bindings, kind: .existsClause)
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
            context.log(.trace, "Reached conditional default case.", metadata: [
                "test": .string(test.description),
                "true_branch": .string(trueBranch.description),
                "false_branch": .string(falseBranch.description)
            ])
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

/// `$0 as? Object`
extension PredicateExpressions.ConditionalCast: SQLPredicateExpression
where Input: SQLPredicateExpression {
    func evaluate<T>(_ context: inout Context<T>) -> Fragment {
        let input = self.input.query(&context)
        var desiredType: Any.Type = Desired.self
        context.log(.debug, "ConditionalCast from \(Input.Output.self) as? \(Desired.self).", metadata: [
            "input": .string(input.description),
            "input_type": "\(input.type, default: "nil")",
            "desired_type": "\(desiredType)",
            "input_output_type": "\(Input.Output.self)",
            "output_type": "\(Output.self)"
        ])
        if desiredType is any RelationshipCollection.Type {
            desiredType = unwrapArrayMetatype(desiredType)
        }
        guard let desiredType = desiredType as? any (PersistentModel & SendableMetatype).Type else {
            return input.copy(clause: input.clause, bindings: input.bindings, type: Desired.self, kind: .expression)
        }
        // Do not use `input.key` because it will emit an extra inheritance `JOIN` and can make the generated alias ambiguous.
        context.loadSchemaMetadata(for: desiredType)
        guard let inputEntity = input.entity,
              let inputAlias = input.alias,
              let desiredEntity = context.schema.entity(for: desiredType) ?? Schema([desiredType]).entity(for: desiredType) else {
            return input.copy(clause: input.clause, bindings: input.bindings, type: desiredType, kind: .expression)
        }
        guard let alias = context.createInheritedAlias(input.key, from: inputEntity, as: inputAlias, to: desiredEntity) else {
            return input.copy(clause: "NULL", bindings: input.bindings, type: desiredType, entity: desiredEntity, kind: .expression)
        }
        context.log(.debug, "Created inheritance alias: \(desiredType)")
        return input.copy(
            clause: "\(quote(alias)).\(quote(pk))",
            bindings: input.bindings,
            alias: alias,
            type: desiredType,
            entity: desiredEntity,
            property: nil,
            keyPath: nil,
            kind: .scope
        )
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
        context.log(.debug, "CollectionIndexSubscript fragments: \(description)", metadata: [
            "wrapped": .string(wrapped.description),
            "index": .string(index.description)
        ])
        let normalizedIndex = """
            CASE WHEN (\(indexClause)) < 0
                THEN json_array_length(\(wrappedClause)) + (\(indexClause))
                ELSE (\(indexClause))
            END
            """
        var expression = "json_extract(\(wrapped.clause), '$[' || (\(normalizedIndex)) || ']')"
        if context.options.contains(.shouldMarkStartOfPredicateExpression) {
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
            context.log(.warning, "CollectionRangeSubscript expected 2 range bindings, got \(count).", metadata: [
                "range": .string(range.description)
            ])
            return .invalid
        }
        let description = "\(Wrapped.self).self, \(outputType).self"
        context.log(.debug, "CollectionRangeSubscript fragments: \(description)", metadata: [
            "wrapped": .string(wrapped.description),
            "range": .string(range.description)
        ])
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
        if context.options.contains(.shouldMarkStartOfPredicateExpression) {
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
        if context.options.contains(.shouldMarkStartOfPredicateExpression) {
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
            case .null:
                break
            }
        }
        if context.options.contains(.shouldMarkStartOfPredicateExpression) {
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
            do {
                // TODO: Apply to `PredicateExpressions.NotEqual`.
                if let rhsValue: any Sendable = sendable(cast: rhsValue),
                   let fragment = try context.translateEphemeralEquality(lhs: lhs.copy(), rhsValue: rhsValue) {
                    _ = rhs.bindings.popLast()
                    return fragment
                }
            } catch {
                return .invalid("Error: \(error)")
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
        context.log(.trace, "Filter.Element.Type is \(Element.self).self.")
        let sequence = self.sequence.query(&context)
        context.log(.trace, "Sequence fragment: \(sequence.description)")
        guard let sequenceKey = sequence.key,
              let sequenceAlias = sequence.alias else {
            return sequence.invalid("Incomplete Filter.sequence")
        }
        guard sequence.property?.metadata is Schema.Relationship else {
            return sequence.invalid("Filter requires a relationship sequence")
        }
        context.path.append(sequenceKey)
        defer { context.key = context.path.popLast() }
        let element = self.variable.query(&context)
        context.log(.trace, "Element fragment: \(element.description)")
        guard let elementKey = element.key,
              let elementAlias = element.alias,
              let elementEntity = element.entity else {
            return element.invalid("Incomplete Filter.variable")
        }
        let condition = self.filter.query(&context)
        context.log(.trace, "Condition fragment: \(condition.description)")
        switch element.type {
        case let type as any (PersistentModel & SendableMetatype).Type:
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
                    preconditionFailure("Relationship is missing reference metadata: \(sequence.description)")
                }
                if reference.count == 2 {
                    context.log(.debug, "Relationship is many-to-many: \(sequence.label)")
                    reference[0].lhsAlias = sequenceAlias
                    reference[0].rhsAlias = context.createTableAlias(sequenceKey, reference[0].rhsTable)
                    reference[1].lhsAlias = reference[0].rhsAlias
                    reference[1].rhsAlias = elementAlias
                    if context.references[elementKey, default: []].append(reference[0]).inserted {
                        context.log(.trace, "Inserted JOIN clause reference (filter many-to-many): \(reference[0])")
                    }
                } else {
                    guard var inverseReference = inverseProperty.reference else {
                        break request
                    }
                    context.log(.debug, "Relationship is one-to-many: \(sequence.label)")
                    inverseReference[0].sourceAlias = elementAlias
                    inverseReference[0].destinationAlias = sequenceAlias
                    if context.references[elementKey, default: []].append(inverseReference[0]).inserted {
                        context.log(.trace, "Inserted JOIN clause reference (filter one-to-many): \(inverseReference[0])")
                    }
                }
            }
            _ = context.references[elementKey].take()
        default:
            context.log(.debug, "Filter on non-model sequence: \(element.description)")
        }
        return sequence.copy(
            clause: condition.clause,
            bindings: condition.bindings,
            alias: elementAlias,
            entity: elementEntity,
            kind: .subquery
        )
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
                preconditionFailure("Unexpected bindable value for ForcedUnwrap: \(inner.description)")
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

/// `$0.property ?? "other"`
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
        if context.options.contains(.shouldMarkStartOfPredicateExpression) {
            let debug = debugVariableIDs(("context", context.key))
            sql = "/* SequenceMaximum (#\(context.level), \(debug)) */\n" + sql
        }
        return sequence.copy(clause: sql, bindings: bindings + bindings, kind: .expression)
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
        if context.options.contains(.shouldMarkStartOfPredicateExpression) {
            let debug = debugVariableIDs(("context", context.key))
            sql = "/* SequenceMinimum (#\(context.level), \(debug)) */\n" + sql
        }
        return sequence.copy(clause: sql, bindings: bindings + bindings, kind: .expression)
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
        if let desiredType = Desired.self as? any (PersistentModel & SendableMetatype).Type,
           let inputEntity = input.entity,
           let inputAlias = input.alias,
           let desiredEntity = context.schema.entity(for: desiredType) ?? Schema([desiredType]).entity(for: desiredType) {
            // Do not use `input.key` because it will emit an extra inheritance `JOIN` and can make the generated alias ambiguous.
            context.loadSchemaMetadata(for: desiredType)
            guard let alias = context.createInheritedAlias(input.key, from: inputEntity, as: inputAlias, to: desiredEntity) else {
                return input.copy(clause: "FALSE", kind: .binaryOperation)
            }
            if alias == inputAlias && inputEntity.name == desiredEntity.name {
                return input.copy(clause: "TRUE", kind: .binaryOperation)
            }
            return input.copy(
                clause: "\(quote(alias)).\(quote(pk)) IS NOT NULL",
                alias: alias,
                type: desiredType,
                entity: desiredEntity,
                kind: .binaryOperation
            )
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
