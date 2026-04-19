//
//  DataStoreMigration.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import DataStoreCore
private import DataStoreSupport
private import Foundation
private import Logging
private import Synchronization
internal import Collections
internal import DataStoreRuntime
internal import DataStoreSQL
internal import SQLiteHandle
internal import SQLiteStatement

#if swift(>=6.2)
internal import SwiftData
#else
@preconcurrency internal import SwiftData
#endif

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.migration")

extension DataStoreMigration {
    nonisolated internal static func load(schema: Schema, shouldRegister: Bool = false) -> DatabaseSchema {
        if shouldRegister {
            TypeRegistry.bootstrap(schema: schema)
        }
        var indexes = [any IndexDefinition]()
        let tableDefinitions = Self.makeTableDefinitions(
            schema: schema,
            indexes: &indexes,
            shouldRegister: shouldRegister
        )
        return .init(indexes: indexes, tables: tableDefinitions)
    }
    
    @SQLTableBuilder nonisolated fileprivate static func makeTableDefinitions(
        schema: Schema,
        indexes: inout [any IndexDefinition],
        shouldRegister: Bool = false
    ) -> [any TableDefinition] {
        let selection = schema
        DataStoreRuntime.makeTableDefinitions(schema: selection) { entity in
            let registeredType = Schema.type(for: entity.name)
            guard let type = registeredType ?? entity.type else {
                fatalError("The associated type for \(entity.name) entity was not registered.")
            }
            #if true
            if registeredType == nil {
                TypeRegistry.register(type, typeName: entity.name, metadata: entity)
            }
            #else
            if shouldRegister || registeredType == nil {
                // Register metatype if it was never supplied during initialization.
                TypeRegistry.register(type, typeName: entity.name, metadata: entity)
            }
            #endif
            var (schemaMetadata, keyPathVariants, _) = makePropertyMetadataArray(schema: selection, for: type)
            let discriminator = schemaMetadata.removeFirst()
            var schemaIndex: PropertyMetadata?
            var schemaUnique: PropertyMetadata?
            if let index = schemaMetadata.lastIndex(where: { $0.name == "Schema.Index" }) {
                schemaIndex = schemaMetadata.remove(at: index)
            }
            if let index = schemaMetadata.lastIndex(where: { $0.name == "Schema.Unique" }) {
                schemaUnique = schemaMetadata.remove(at: index)
            }
            // Only register the stored properties.
            if shouldRegister {
                // Only persist for the configuration's schema.
                type.overwritePropertyMetadata(schemaMetadata)
                for (canonicalKeyPath, keyPathVariant) in keyPathVariants {
                    type.addKeyPathVariantToPropertyMetadata(keyPathVariant, canonical: canonicalKeyPath)
                }
            }
            if let schemaProperty = schemaIndex {
                let tableIndexes = createTableIndexes(for: type, on: schemaProperty.metadata)
                indexes.append(contentsOf: tableIndexes)
                logger.debug("Found auxiliary metadata for indices: \(schemaProperty)")
            }
            var uniqueTableConstraints = [TableConstraint]()
            if let schemaProperty = schemaUnique {
                uniqueTableConstraints = createUniqueTableConstraints(for: type, on: schemaProperty.metadata)
                logger.debug("Found auxiliary metadata for uniqueness constraints: \(schemaProperty)")
            }
            return (discriminator, schemaMetadata, uniqueTableConstraints)
        }
    }
    
    nonisolated fileprivate static func setup(schema: DatabaseSchema, store: DatabaseStore) throws {
        let tables = schema.tables
        let indexes = schema.indexes
        try store.queue.withConnection(.writer) { connection in
            for table in tables { try connection.execute.create(table: table) }
            for index in indexes { try connection.execute.create(index: index) }
        }
        if store.configuration.options.contains(.useVerboseLogging) {
            logger.debug(
                {
                    let description = tables.map(\.sql).joined(separator: ",\n")
                    return "Generated \(tables.count) SQL tables:\n\(description)"
                }(),
                metadata: tables.reduce(into: .init()) { $0[$1.name] = .string($1.sql) }
            )
            logger.trace(
                {
                    let description = indexes.map(\.sql).joined(separator: ",\n")
                    return "Generated \(indexes.count) SQL indexes:\n\(description)"
                }(),
                metadata: indexes.reduce(into: .init()) { $0[$1.name] = .string($1.sql) }
            )
        }
    }
}

// elevate some inferred to lightweight

internal final class DataStoreMigration: StoreBound {
    internal typealias Store = DatabaseStore
    internal unowned let store: Store
    internal let analysis: Classifier
    internal let plan: Plan
    internal let oldSchemaSet: SchemaSet
    internal let newSchemaSet: SchemaSet
    private let shouldAutomaticallyMigrateOnSchemaChange: Bool
    
    internal enum Error: Swift.Error {
        case malformedSQLResult
        case requiresCustomMigration
    }
    
    @discardableResult internal init?(
        store: Store,
        custom: ((
            CustomMigrationContext,
            borrowing DatabaseConnection<Store>
        ) throws -> Void)? = nil
    ) throws {
        self.store = store
        let newSchema = self.store.schema
        let newSQLSchema = Self.load(schema: store.schema, shouldRegister: true)
        defer {
            if store.configuration.options.contains(.forceSchemaOverwrite) {
                try? store.setValue(newSchema, forKey: "schema")
            }
        }
        if store.configuration.options.contains(.disableSchemaMigrations) {
            return nil
        }
        self.shouldAutomaticallyMigrateOnSchemaChange =
        store.configuration.options.contains(.disableLightweightSchemaMigrations) == false
        guard let oldSchema = try store.getValue(forKey: "schema", as: Schema.self) else {
            logger.info("No SwiftData.Schema was ever stored. Saving current schema: \(newSchema.version)")
            try store.setValue(newSchema, forKey: "schema")
            try Self.setup(schema: newSQLSchema, store: store)
            return nil
        }
        let (oldDatabaseSchema, storedStatements, oldAuxiliaryObjectsByTable) = try store.queue.reader { connection in
            // Fetch SQLite's schema metadata.
            let tableRows = try connection.fetch(
                """
                SELECT "name", "sql" FROM sqlite_schema 
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                """
            )
            var definitions = [any TableDefinition]()
            var oldIndexDefinitions = [any IndexDefinition]()
            var storedStatements = [String: String]()
            for tableRow in tableRows {
                guard let tableName = tableRow[0] as? String,
                      let createTableStatement = tableRow[1] as? String else {
                    throw Error.malformedSQLResult
                }
                guard let oldEntity = oldSchema.entitiesByName[tableName] else {
                    continue
                }
                oldIndexDefinitions.append(contentsOf: createTableIndexes(for: oldEntity))
                logger.trace("Rebuilding definitions from SQLite schema metadata: \(tableName)")
                storedStatements[tableName] = createTableStatement
                let columnRows = try connection.query("PRAGMA table_info(\(quote(tableName)));")
                let foreignKeyRows = try connection.query("PRAGMA foreign_key_list(\(quote(tableName)));")
                let indexRows = try connection.query("PRAGMA index_list(\(quote(tableName)));")
                var columns = [any ColumnDefinition]()
                var primaryKeyColumnNames = [(position: Int64, name: String)]()
                for var columnRow in columnRows {
                    guard let columnName = columnRow["name"].take() as? String,
                          let _ = columnRow["type"].take() as? String else {
                        logger.error(
                            "Unable to parse column metadata.",
                            metadata: [
                                "table": .string(tableName),
                                "column_row": "\(columnRow)"
                            ]
                        )
                        continue
                    }
                    let property = oldEntity.storedPropertiesByName[columnName]
                    _ = columnRow["cid"].take()
                    var columnConstraints = [ColumnConstraint]()
                    if let primaryKeyPosition = columnRow["pk"].take() as? Int64, primaryKeyPosition > 0 {
                        primaryKeyColumnNames.append((primaryKeyPosition, columnName))
                        logger.trace("Applied PRIMARY KEY constraint: \(tableName).\(columnName)")
                    }
                    if let isNotNull = (columnRow["notnull"].take() as? Int64), isNotNull != 0 {
                        columnConstraints.append(.notNull)
                        logger.trace("Applied NOT NULL constraint: \(tableName).\(columnName)")
                    }
                    if let defaultValue = columnRow["dflt_value"].take() as? String {
                        columnConstraints.append(.defaultValue(defaultValue))
                        logger.trace("Applied DEFAULT VALUE: \(tableName).\(columnName)")
                    }
                    if !columnRow.isEmpty {
                        logger.trace("Remaining column metadata: \(columnRow)")
                    }
                    columns.append(SQLColumn(
                        name: columnName,
                        valueType: property?.valueType ?? Void.self,
                        constraints: columnConstraints
                    ))
                }
                var tableConstraints = [TableConstraint]()
                let sortedPrimaryKeyColumns = primaryKeyColumnNames
                    .sorted(by: { $0.position < $1.position })
                    .map(\.name)
                if sortedPrimaryKeyColumns.count == 1 {
                    let column = sortedPrimaryKeyColumns[0]
                    if let columnIndex = columns.firstIndex(where: { $0.name == column }) {
                        let existingColumn = columns[columnIndex]
                        columns[columnIndex] = SQLColumn(
                            name: existingColumn.name,
                            valueType: Void.self,
                            constraints: [.primaryKey] + existingColumn.constraints
                        )
                    }
                } else if sortedPrimaryKeyColumns.count > 1 {
                    tableConstraints.append(.primaryKey(sortedPrimaryKeyColumns))
                }
                let groupedForeignKeys = Dictionary(
                    grouping: foreignKeyRows,
                    by: { $0["id"] as? Int ?? 0 }
                )
                for (_, group) in groupedForeignKeys.sorted(by: { $0.key < $1.key }) {
                    let sortedGroup = group.sorted { ($0["seq"] as? Int ?? 0) < ($1["seq"] as? Int ?? 0) }
                    guard let referencedTable = sortedGroup.first?["table"] as? String else {
                        continue
                    }
                    let fromColumns = sortedGroup.compactMap { $0["from"] as? String }
                    let toColumns = sortedGroup.compactMap { $0["to"] as? String }
                    let onDelete = sortedGroup.first
                        .flatMap { $0["on_delete"] as? String }
                        .flatMap { $0 != "NO ACTION" ? ReferentialAction(rawValue: $0) : nil }
                    let onUpdate = sortedGroup.first
                        .flatMap { $0["on_update"] as? String }
                        .flatMap { $0 != "NO ACTION" ? ReferentialAction(rawValue: $0) : nil }
                    tableConstraints.append(.foreignKey(
                        fromColumns,
                        references: referencedTable,
                        at: toColumns,
                        onDelete: onDelete,
                        onUpdate: onUpdate
                    ))
                }
                for indexRow in indexRows {
                    guard indexRow["origin"] as? String == "u",
                          let indexName = indexRow["name"] as? String else {
                        continue
                    }
                    let indexInfoRows = try connection.query("PRAGMA index_info(\(quote(indexName)));")
                    let uniqueColumns = indexInfoRows
                        .sorted(by: { ($0["seqno"] as? Int ?? 0) < ($1["seqno"] as? Int ?? 0) })
                        .compactMap { $0["name"] as? String }
                    if !uniqueColumns.isEmpty {
                        tableConstraints.append(.unique(uniqueColumns))
                        logger.trace("Applied UNIQUE table constraint: \(tableName).\(uniqueColumns)")
                    }
                }
                definitions.append(SQLTable(
                    name: tableName,
                    constraints: tableConstraints, columns: { columns }
                ))
            }
            let auxiliaryRows = try connection.fetch(
                """
                SELECT "tbl_name", "type", "name", "sql"
                FROM sqlite_schema
                WHERE type IN ('index', 'trigger')
                    AND "tbl_name" NOT LIKE 'sqlite_%'
                    AND "sql" IS NOT NULL
                ORDER BY "tbl_name", "type", "name"
                """
            )
            var auxiliaryObjectsByTable = [String: [AuxiliaryObject]]()
            for auxiliaryRow in auxiliaryRows {
                guard let tableName = auxiliaryRow[0] as? String,
                      let rawKind = auxiliaryRow[1] as? String,
                      let name = auxiliaryRow[2] as? String,
                      let sql = auxiliaryRow[3] as? String,
                      let kind = AuxiliaryObject.Kind(rawValue: rawKind) else {
                    throw Error.malformedSQLResult
                }
                auxiliaryObjectsByTable[tableName, default: []].append(.init(
                    table: tableName,
                    name: name,
                    kind: kind,
                    sql: sql
                ))
            }
            return (DatabaseSchema(indexes: oldIndexDefinitions, tables: definitions), storedStatements, auxiliaryObjectsByTable)
        }
        logger.debug(
            "Reconstructed old schema and statements.",
            metadata: [
                "definitions.count": "\(oldDatabaseSchema.tables.count)",
                "statements.count": "\(storedStatements.count)"
            ]
        )
        self.oldSchemaSet = .init(
            ormSchema: oldSchema,
            sqlSchema: oldDatabaseSchema,
            sql: storedStatements,
            constraints: oldSchema.entities.reduce(into: .init(), { partialResult, entity in
                partialResult[entity.name] = createUniqueColumnGroups(for: entity)
            }),
            auxiliaryObjectsByTable: oldAuxiliaryObjectsByTable
        )
        self.newSchemaSet = .init(
            ormSchema: newSchema,
            sqlSchema: newSQLSchema,
            sql: newSQLSchema.tables.reduce(into: [String: String](), { partialResult, table in
                partialResult[table.name] = table.sql
            }),
            constraints: newSchema.entities.reduce(into: .init(), { partialResult, entity in
                partialResult[entity.name] = createUniqueColumnGroups(for: entity)
            }),
            auxiliaryObjectsByTable: Self.makeAuxiliaryObjects(from: newSQLSchema.indexes)
        )
        self.analysis = try .init(
            old: oldSchemaSet,
            new: newSchemaSet,
            diff: SchemaDiff(old: oldSchemaSet, new: newSchemaSet)
        )
        self.plan = try .init(old: oldSchemaSet, new: newSchemaSet, analysis: self.analysis)
        do {
            try self.apply(custom: custom)
        } catch {
            logger.error("Migration error: \(error)")
            throw error
        }
    }
    
    internal func apply(
        custom: ((
            CustomMigrationContext,
            borrowing DatabaseConnection<Store>
        ) throws -> Void)? = nil
    ) throws {
        logger.debug(
            "Applying plan",
            metadata: [
                "analysis.issues.count": "\(analysis.issues.count)",
                "analysis.operations.count": "\(analysis.operations.count)",
                "analysis.style": "\(analysis.style)",
                "analysis.warnings.count": "\(analysis.warnings.count)",
                "plan.steps.count": "\(plan.steps.count)",
                "plan.style": "\(plan.style)"
            ]
        )
        if store.configuration.options.contains(.disableLightweightSchemaMigrations), plan.style == .inferred {
            logger.notice("Skipping lightweight schema migration.")
            return
        }
        if plan.requiresCustomHandler, custom == nil {
            throw Error.requiresCustomMigration
        }
        switch plan.style {
        case .inferred, .custom:
            try store.queue.writer { connection in
                do {
                    try connection.execute("PRAGMA foreign_keys = OFF;")
                    defer { _ = try? connection.execute("PRAGMA foreign_keys = ON;") }
                    do {
                        try connection.execute("BEGIN TRANSACTION")
                        for step in self.plan.steps {
                            switch step {
                            case .validate(let validations):
                                try validate(validations, connection: connection)
                            case .sql(let statements):
                                for statement in statements {
                                    try connection.execute(statement)
                                }
                            case .rebuildTable(let rebuild):
                                try connection.execute(rebuild.createSQL)
                                if !rebuild.copySQL.isEmpty {
                                    try connection.execute(rebuild.copySQL)
                                }
                                try connection.execute(rebuild.dropSQL)
                                try connection.execute(rebuild.renameSQL)
                                for statement in rebuild.auxiliarySQL {
                                    try connection.execute(statement)
                                }
                            case .custom(let customStep):
                                let context = CustomMigrationContext(
                                    oldSchema: oldSchemaSet,
                                    newSchema: newSchemaSet,
                                    operations: customStep.operations
                                )
                                guard let custom else {
                                    preconditionFailure("Custom migration handler was not provided.")
                                }
                                try custom(context, connection)
                            case .persistSchemaMetadata:
                                if !store.configuration.options.contains(.disableSchemaMigrations) {
                                    try store.setValue(newSchemaSet.ormSchema, forKey: "schema", connection: connection)
                                }
                            }
                        }
                        let foreignKeyRows = try connection.query("PRAGMA foreign_key_check;")
                        if foreignKeyRows.isEmpty == false {
                            throw DataStoreError.unsupportedFeature
                        }
                        try connection.execute("COMMIT TRANSACTION")
                    } catch {
                        logger.error("Applying schema migration failed: \(error)")
                        try connection.execute("ROLLBACK TRANSACTION")
                        throw error
                    }
                }
            }
        case .compatible:
            if !store.configuration.options.contains(.disableSchemaMigrations) {
                try store.setValue(newSchemaSet.ormSchema, forKey: "schema")
            }
        case .unsupported:
            throw DataStoreError.unsupportedFeature
        }
    }
    
    private func validate(_ validations: [Validation], connection: borrowing DatabaseConnection<Store>) throws {
        for validation in validations {
            switch validation {
            case .uniqueness(let entity, let sourceColumns, let destinationColumns):
                guard try tableExists(entity, connection: connection) else {
                    continue
                }
                let existingSourceColumns = try sourceColumns.reduce(into: [String]()) { partialResult, column in
                    guard try columnExists(column, in: entity, connection: connection) else {
                        return
                    }
                    partialResult.append(column)
                }
                guard existingSourceColumns.isEmpty == false else {
                    continue
                }
                guard existingSourceColumns.count == sourceColumns.count else {
                    logger.debug(
                        "Skipping uniqueness validation because some source columns do not exist yet.",
                        metadata: [
                            "entity": "\(entity)",
                            "source_columns": "\(sourceColumns)",
                            "destination_columns": "\(destinationColumns)",
                            "resolved_source_columns": "\(existingSourceColumns)"
                        ]
                    )
                    continue
                }
                let selectColumns = existingSourceColumns.map(quote).joined(separator: ", ")
                let rows = try connection.fetch(
                    """
                    SELECT \(selectColumns), COUNT(*) AS count
                    FROM \(quote(entity))
                    GROUP BY \(selectColumns)
                    HAVING COUNT(*) > 1
                    LIMIT 1
                    """
                )
                if rows.isEmpty == false {
                    logger.error(
                        "Uniqueness validation failed.",
                        metadata: [
                            "entity": "\(entity)",
                            "sourceColumns": "\(existingSourceColumns)",
                            "destinationColumns": "\(destinationColumns)"
                        ]
                    )
                    throw DataStoreError.unsupportedFeature
                }
            case .nonNull(let entity, let sourceColumn, let destinationColumn):
                guard try tableExists(entity, connection: connection) else {
                    continue
                }
                guard try columnExists(sourceColumn, in: entity, connection: connection) else {
                    logger.error(
                        "Non-null validation could not resolve source column.",
                        metadata: [
                            "entity": "\(entity)",
                            "sourceColumn": "\(sourceColumn)",
                            "destinationColumn": "\(destinationColumn)"
                        ]
                    )
                    throw DataStoreError.unsupportedFeature
                }
                let rows = try connection.fetch(
                    """
                    SELECT 1 FROM \(quote(entity))
                    WHERE \(quote(sourceColumn)) IS NULL
                    LIMIT 1
                    """
                )
                if rows.isEmpty == false {
                    logger.error(
                        "Non-null validation failed.",
                        metadata: [
                            "entity": "\(entity)",
                            "sourceColumn": "\(sourceColumn)",
                            "destinationColumn": "\(destinationColumn)"
                        ]
                    )
                    throw DataStoreError.unsupportedFeature
                }
            case .foreignKey(let entity, _, _, _):
                let rows = try connection.fetch("PRAGMA foreign_key_check(\(quote(entity)));")
                if rows.isEmpty == false {
                    throw DataStoreError.unsupportedFeature
                }
            case .tableEmpty(let entity):
                guard try tableExists(entity, connection: connection) else {
                    continue
                }
                let rows = try connection.fetch(
                    """
                    SELECT 1 FROM \(quote(entity))
                    LIMIT 1
                    """
                )
                if rows.isEmpty == false {
                    logger.error(
                        "Table-empty validation failed.",
                        metadata: ["entity": "\(entity)"]
                    )
                    throw DataStoreError.unsupportedFeature
                }
            case .columnAllNull(let entity, let column):
                guard try tableExists(entity, connection: connection) else {
                    continue
                }
                guard try columnExists(column, in: entity, connection: connection) else {
                    continue
                }
                let rows = try connection.fetch(
                    """
                    SELECT 1 FROM \(quote(entity))
                    WHERE \(quote(column)) IS NOT NULL
                    LIMIT 1
                    """
                )
                if rows.isEmpty == false {
                    logger.error(
                        "Column-all-null validation failed.",
                        metadata: [
                            "entity": "\(entity)",
                            "column": "\(column)"
                        ]
                    )
                    throw DataStoreError.unsupportedFeature
                }
            case .columnUnused(let entity, let column, let isOptional):
                guard try tableExists(entity, connection: connection) else {
                    continue
                }
                guard try columnExists(column, in: entity, connection: connection) else {
                    continue
                }
                let rows: [[Any?]]
                if isOptional {
                    rows = try connection.fetch(
                        """
                        SELECT 1 FROM \(quote(entity))
                        WHERE \(quote(column)) IS NOT NULL
                        LIMIT 1
                        """
                    )
                } else {
                    rows = try connection.fetch(
                        """
                        SELECT 1 FROM \(quote(entity))
                        LIMIT 1
                        """
                    )
                }
                if rows.isEmpty == false {
                    logger.error(
                        "Column-unused validation failed.",
                        metadata: [
                            "entity": "\(entity)",
                            "column": "\(column)",
                            "optional": "\(isOptional)"
                        ]
                    )
                    throw DataStoreError.unsupportedFeature
                }
            case .typeConvertible:
                throw DataStoreError.unsupportedFeature
            }
        }
    }
    
    private func tableExists(
        _ tableName: String,
        connection: borrowing DatabaseConnection<Store>
    ) throws -> Bool {
        let escaped = tableName.replacingOccurrences(of: "'", with: "''")
        let rows = try connection.fetch(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = '\(escaped)'
            LIMIT 1
            """
        )
        return rows.isEmpty == false
    }
    
    private func columnExists(
        _ columnName: String,
        in tableName: String,
        connection: borrowing DatabaseConnection<Store>
    ) throws -> Bool {
        let rows = try connection.query("PRAGMA table_info(\(quote(tableName)));")
        return rows.contains { $0["name"] as? String == columnName }
    }
    
    private static func makeAuxiliaryObjects(
        from indexDefinitions: [any IndexDefinition]
    ) -> [String: [AuxiliaryObject]] {
        var result = [String: [AuxiliaryObject]]()
        for index in indexDefinitions {
            let prefix = index.isUnique ? "CREATE UNIQUE INDEX " : "CREATE INDEX "
            let sql = "\(prefix)\(index.sql);"
            result[index.table, default: []].append(.init(
                table: index.table,
                name: index.name,
                kind: .index,
                sql: sql
            ))
        }
        return result
    }
    
    internal struct SchemaSet {
        internal let ormSchema: Schema
        internal let sqlSchema: DatabaseSchema
        internal let sql: [String: String]
        internal let constraints: [String: [[String]]]
        internal let auxiliaryObjectsByTable: [String: [AuxiliaryObject]]
        
        internal var entitiesByName: [String: Schema.Entity] {
            ormSchema.entitiesByName
        }
        
        internal var tablesByName: [String: any TableDefinition] {
            Dictionary(uniqueKeysWithValues: sqlSchema.tables.map { ($0.name, $0) })
        }
        
        internal var indexesByName: [String: [any IndexDefinition]] {
            Dictionary(grouping: sqlSchema.indexes, by: \.table)
        }
    }
    
    internal struct AuxiliaryObject: Sendable, Hashable {
        internal let table: String
        internal let name: String
        internal let kind: Kind
        internal let sql: String
        
        internal enum Kind: String, Sendable, Hashable {
            case index
            case trigger
        }
    }
    
    internal struct CustomMigrationContext {
        internal let oldSchema: SchemaSet
        internal let newSchema: SchemaSet
        internal let operations: [Operation]
    }
    
    internal enum Style: Sendable {
        case compatible
        case inferred
        case custom
        case unsupported
    }
    
    internal enum Step: Hashable, Sendable {
        case validate([Validation])
        case sql([String])
        case rebuildTable(Rebuild)
        case custom(Custom)
        case persistSchemaMetadata
    }
    
    internal struct Custom: Hashable, Sendable {
        internal let operations: [Operation]
    }
    
    internal struct Rebuild: Hashable, Sendable {
        internal let table: String
        internal let temporaryTable: String
        internal let createSQL: String
        internal let copySQL: String
        internal let dropSQL: String
        internal let renameSQL: String
        internal let auxiliarySQL: [String]
    }
    
    internal enum Validation: Hashable, Sendable {
        case uniqueness(entity: String, sourceColumns: [String], destinationColumns: [String])
        case nonNull(entity: String, sourceColumn: String, destinationColumn: String)
        case foreignKey(entity: String, columns: [String], referencedTable: String, referencedColumns: [String])
        case tableEmpty(entity: String)
        case columnAllNull(entity: String, column: String)
        case columnUnused(entity: String, column: String, isOptional: Bool)
        case typeConvertible(entity: String, column: String)
    }
    
    internal enum Severity: Hashable, Sendable {
        case custom
        case unsupported
    }
    
    internal struct Issue: Hashable, Sendable {
        internal let severity: Severity
        internal let kind: Kind
        internal let entityName: String
        internal let propertyName: String?
        
        internal enum Kind: Hashable, Sendable {
            case ambiguousRename
            case incompatibleTypeChange
            case transformableRepresentationChanged
            case relationshipTopologyChanged
            case uniquenessRequiresValidation
            case nonOptionalWithoutDefault
            case externalStorageSemanticsChanged
            case unsupportedHashModifier
            case unsupportedPropertyKindChange
        }
    }
    
    internal struct Warning: Hashable, Sendable {
        internal let kind: Kind
        internal let entityName: String
        internal let propertyName: String?
        
        internal enum Kind: Hashable, Sendable {
            case dataValidationRequired
            case destructiveChange
            case tableRebuildRequired
        }
    }
    
    internal enum Operation: Equatable, Hashable {
        case createEntity(name: String)
        case dropEntity(name: String)
        case renameEntity(from: String, to: String)
        
        case addAttribute(entity: String, name: String, defaultValue: SQLValue?, isOptional: Bool)
        case dropAttribute(entity: String, name: String)
        case renameAttribute(entity: String, from: String, to: String)
        case alterAttributeNullability(entity: String, name: String, isOptional: Bool)
        case alterAttributeType(entity: String, name: String)
        case alterAttributeTransformable(entity: String, name: String)
        
        case addUniqueConstraint(entity: String, columns: [String])
        case dropUniqueConstraint(entity: String, columns: [String])
        
        case addIndex(entity: String, index: SQLIndex)
        case dropIndex(entity: String, index: SQLIndex)
        
        case addRelationship(entity: String, name: String)
        case dropRelationship(entity: String, name: String)
        case renameRelationship(entity: String, from: String, to: String)
        case alterRelationship(entity: String, name: String)
        
        internal var entityName: String {
            switch self {
            case .createEntity(let name):
                return name
            case .dropEntity(let name):
                return name
            case .renameEntity(let from, _):
                return from
            case .addAttribute(let entity, _, _, _):
                return entity
            case .dropAttribute(let entity, _):
                return entity
            case .renameAttribute(let entity, _, _):
                return entity
            case .alterAttributeNullability(let entity, _, _):
                return entity
            case .alterAttributeType(let entity, _):
                return entity
            case .alterAttributeTransformable(let entity, _):
                return entity
            case .addUniqueConstraint(let entity, _):
                return entity
            case .dropUniqueConstraint(let entity, _):
                return entity
            case .addIndex(let entity, _):
                return entity
            case .dropIndex(let entity, _):
                return entity
            case .addRelationship(let entity, _):
                return entity
            case .dropRelationship(let entity, _):
                return entity
            case .renameRelationship(let entity, _, _):
                return entity
            case .alterRelationship(let entity, _):
                return entity
            }
        }
    }
    
    internal struct Plan: Sendable {
        internal let style: Style
        internal let steps: [Step]
        internal let requiresCustomHandler: Bool
        
        internal init(old: SchemaSet, new: SchemaSet, analysis: Classifier) throws {
            let result = try Planner.makeSteps(
                old: old,
                new: new,
                style: analysis.style,
                operations: analysis.operations,
                issues: analysis.issues,
                warnings: analysis.warnings
            )
            self.style = analysis.style
            self.steps = result.steps
            self.requiresCustomHandler = result.requiresCustomHandler
        }
    }
    
    internal struct Planner: Sendable {
        internal let steps: [Step]
        internal let requiresCustomHandler: Bool
        
        internal static func makeSteps(
            old: SchemaSet,
            new: SchemaSet,
            style: Style,
            operations: [Operation],
            issues: [Issue],
            warnings: [Warning]
        ) throws -> Self {
            guard style != .unsupported else {
                throw DataStoreError.unsupportedFeature
            }
            logger.debug("Making \(style) migration steps: \(operations.count) operations")
            var steps = [Step]()
            var validations = [Validation]()
            var entityValidations = [Validation]()
            var sql = [String]()
            var rebuilds = [Rebuild]()
            var customOperations = [Operation]()
            var grouped = OrderedDictionary<String, [Operation]>()
            for operation in operations {
                switch operation {
                case .createEntity(let name):
                    if let definition = new.tablesByName[name] {
                        sql.append("CREATE TABLE \(definition.sql)")
                        sql.append(contentsOf: new.auxiliaryObjectsByTable[name, default: []].map(\.sql))
                        logger.debug(
                            "Plan: Creating table \(name)",
                            metadata: [
                                "sql": "\(definition.sql)"
                            ]
                        )
                    } else {
                        logger.warning("Plan: No schema definition for \(name)")
                    }
                case .dropEntity(let name):
                    entityValidations.append(.tableEmpty(entity: name))
                    sql.append("DROP TABLE IF EXISTS \(quote(name))")
                    logger.debug("Plan: Dropping table \(name)")
                case .renameEntity(let from, let to):
                    // FIXME: Unable to reliably detect a rename (currently will drop/add).
                    sql.append("ALTER TABLE \(quote(from)) RENAME TO \(quote(to))")
                    logger.debug("Plan: Renaming table \(from) to \(to)")
                default:
                    grouped[operation.entityName, default: []].append(operation)
                }
            }
            validations.append(contentsOf: entityValidations)
            for (entityName, entityOperations) in grouped {
                validations.append(contentsOf: plannedValidations(
                    for: entityOperations,
                    old: old
                ))
                if requiresCustom(entityOperations) {
                    customOperations.append(contentsOf: entityOperations)
                    logger.debug(
                        "Plan: Manual intervention required for \(entityName)",
                        metadata: ["operations": "\(entityOperations.count)"]
                    )
                    continue
                }
                if needsRebuild(entityOperations) {
                    if let rebuild = try rebuild(
                        for: entityName,
                        old: old,
                        new: new,
                        operations: entityOperations
                    ) {
                        rebuilds.append(rebuild)
                        logger.debug(
                            "Plan: Rebuild needed for \(entityName)",
                            metadata: ["operations": "\(entityOperations.count)"]
                        )
                    } else {
                        customOperations.append(contentsOf: entityOperations)
                        logger.debug(
                            "Plan: Rebuild could not be created for \(entityName)",
                            metadata: ["operations": "\(entityOperations.count)"]
                        )
                    }
                    continue
                }
                sql.append(contentsOf: directSQL(for: entityOperations, old: old, new: new))
            }
            if !validations.isEmpty {
                steps.append(.validate(validations))
                logger.debug(
                    "Plan: Adding steps for validation",
                    metadata: ["validations": "\(validations.count)"]
                )
            }
            if !sql.isEmpty {
                steps.append(.sql(sql))
                logger.debug(
                    "Plan: Adding steps for SQL",
                    metadata: ["sql": "\(sql)"]
                )
            }
            for rebuild in rebuilds {
                steps.append(.rebuildTable(rebuild))
                logger.debug(
                    "Plan: Adding steps to rebuild",
                    metadata: ["rebuild": "\(rebuild.table)"]
                )
            }
            if !customOperations.isEmpty {
                steps.append(.custom(.init(operations: customOperations)))
                logger.debug(
                    "Plan: Adding steps for custom operations",
                    metadata: ["custom": "\(customOperations.count)"]
                )
            }
            steps.append(.persistSchemaMetadata)
            logger.debug(
                "Plan: Planner has completed",
                metadata: [
                    "validations": "\(validations.count)",
                    "sql": "\(sql.count)",
                    "rebuilds": "\(rebuilds.count)",
                    "customOperations": "\(customOperations.count)"
                ]
            )
            return .init(
                steps: steps,
                requiresCustomHandler: customOperations.isEmpty == false
            )
        }
        
        private static func plannedValidations(for operations: [Operation], old: SchemaSet) -> [Validation] {
            var validations = [Validation]()
            var renamedColumns = [String: String]()
            for operation in operations {
                switch operation {
                case .renameAttribute(_, let from, let to):
                    renamedColumns[to] = from
                default:
                    break
                }
            }
            for operation in operations {
                switch operation {
                case .addUniqueConstraint(let entity, let columns):
                    let sourceColumns = columns.map { renamedColumns[$0] ?? $0 }
                    validations.append(.uniqueness(
                        entity: entity,
                        sourceColumns: sourceColumns,
                        destinationColumns: columns
                    ))
                case .alterAttributeNullability(let entity, let name, let isOptional):
                    if isOptional == false {
                        let sourceColumn = renamedColumns[name] ?? name
                        validations.append(.nonNull(
                            entity: entity,
                            sourceColumn: sourceColumn,
                            destinationColumn: name
                        ))
                    }
                case .addAttribute(let entity, _, let defaultValue, let isOptional):
                    if isOptional == false, defaultValue == nil {
                        validations.append(.tableEmpty(entity: entity))
                    }
                case .dropAttribute(let entity, let name):
                    let isOptional = oldColumnIsOptional(in: old, entity: entity, column: name)
                    validations.append(.columnUnused(
                        entity: entity,
                        column: name,
                        isOptional: isOptional
                    ))
                case .alterAttributeType(let entity, let name):
                    let sourceColumn = renamedColumns[name] ?? name
                    validations.append(.columnAllNull(entity: entity, column: sourceColumn))
                case .alterAttributeTransformable(let entity, let name):
                    let sourceColumn = renamedColumns[name] ?? name
                    validations.append(.columnAllNull(entity: entity, column: sourceColumn))
                case .alterRelationship(let entity, let name):
                    validations.append(.foreignKey(
                        entity: entity,
                        columns: [name],
                        referencedTable: "",
                        referencedColumns: []
                    ))
                default:
                    break
                }
            }
            return validations
        }
        
        private static func oldColumnIsOptional(in schema: SchemaSet, entity: String, column: String) -> Bool {
            guard let table = schema.tablesByName[entity],
                  let existingColumn = table.columns.first(where: { $0.name == column }) else {
                return true
            }
            return existingColumn.isOptional
        }
        
        private static func requiresCustom(_ operations: [Operation]) -> Bool {
            for operation in operations {
                switch operation {
                case .addRelationship: return true
                case .dropRelationship: return true
                case .renameRelationship: return true
                case .alterRelationship: return true
                default: break
                }
            }
            return false
        }
        
        private static func needsRebuild(_ operations: [Operation]) -> Bool {
            for operation in operations {
                switch operation {
                case .addAttribute(_, _, let defaultValue, let isOptional):
                    if isOptional == false, defaultValue == nil {
                        return true
                    }
                case .dropAttribute:
                    return true
                case .alterAttributeNullability:
                    return true
                case .alterAttributeType:
                    return true
                case .alterAttributeTransformable:
                    return true
                case .addUniqueConstraint:
                    return true
                case .dropUniqueConstraint:
                    return true
                case .addRelationship:
                    return true
                case .dropRelationship:
                    return true
                case .renameRelationship:
                    return true
                case .alterRelationship:
                    return true
                default:
                    break
                }
            }
            return false
        }
        
        private static func directSQL(for operations: [Operation], old: SchemaSet, new: SchemaSet) -> [String] {
            var sql = [String]()
            for operation in operations {
                switch operation {
                case .addAttribute(let entity, let name, let defaultValue, let isOptional):
                    let type = sqlType(for: new, entity: entity, property: name)
                    if isOptional {
                        sql.append(
                            """
                            ALTER TABLE \(quote(entity))
                            ADD COLUMN \(quote(name)) \(type)
                            """
                        )
                    } else if let defaultValue {
                        sql.append(
                            """
                            ALTER TABLE \(quote(entity))
                            ADD COLUMN \(quote(name)) \(type) NOT NULL DEFAULT \(defaultValue)
                            """
                        )
                    }
                case .renameAttribute(let entity, let from, let to):
                    sql.append(
                        """
                        ALTER TABLE \(quote(entity))
                        RENAME COLUMN \(quote(from)) TO \(quote(to))
                        """
                    )
                case .addUniqueConstraint(_, _):
                    logger.notice("Adding unique constraint is not supported yet.")
                    break
                case .addIndex(_, let index):
                    let prefix = index.isUnique ? "CREATE UNIQUE INDEX IF NOT EXISTS " : "CREATE INDEX IF NOT EXISTS "
                    sql.append("\(prefix)\(index.sql)")
                case .dropIndex(_, let index):
                    sql.append("DROP INDEX IF EXISTS \(quote(index.name))")
                default:
                    break
                }
            }
            return sql
        }
        
        private static func resolveIndex(
            in indexes: [any IndexDefinition],
            columns: [String],
            isUnique: Bool
        ) -> SQLIndex? {
            indexes.first(where: { index in
                index.isUnique == isUnique &&
                index.columns.compactMap(\.name) == columns
            }) as? SQLIndex
        }
        
        private static func makeUniqueIndex(
            in schema: SchemaSet,
            table: String,
            columns: [String]
        ) -> SQLIndex {
            .init(
                name: uniqueIndexName(in: schema, table: table, columns: columns),
                table: table,
                isUnique: true
            ) {
                for column in columns {
                    SQLIndexedColumn(name: column)
                }
            }
        }
        
        private static func rebuild(
            for entityName: String,
            old: SchemaSet,
            new: SchemaSet,
            operations: [Operation]
        ) throws -> Rebuild? {
            guard let newDefinition = new.tablesByName[entityName] else {
                return nil
            }
            let oldAuxiliaryObjects = old.auxiliaryObjectsByTable[entityName, default: []]
            if oldAuxiliaryObjects.isEmpty == false,
               requiresCustomAuxiliaryRewrite(operations) {
                return nil
            }
            let temp = "_migration_\(entityName)"
            let oldColumns = Self.columnNames(from: old.tablesByName[entityName])
            let newColumns = Self.columnNames(from: newDefinition)
            var renamedColumns = [String: String]()
            for operation in operations {
                switch operation {
                case .renameAttribute(let entity, let from, let to) where entity == entityName:
                    renamedColumns[to] = from
                default:
                    break
                }
            }
            var insertColumns = [String]()
            var selectColumns = [String]()
            for newColumn in newColumns {
                if let oldColumn = renamedColumns[newColumn], oldColumns.contains(oldColumn) {
                    insertColumns.append(quote(newColumn))
                    selectColumns.append(quote(oldColumn))
                    continue
                }
                if oldColumns.contains(newColumn) {
                    insertColumns.append(quote(newColumn))
                    selectColumns.append(quote(newColumn))
                }
            }
            let temporaryDefinition = SQLTable(
                schema: newDefinition.schema,
                name: temp,
                constraints: newDefinition.constraints
            ) {
                newDefinition.columns
            }
            let createSQL = "CREATE TABLE \(temporaryDefinition.sql)"
            let copySQL: String
            if insertColumns.isEmpty {
                copySQL = ""
            } else {
                copySQL =
                    """
                    INSERT INTO \(quote(temp)) (\(insertColumns.joined(separator: ", ")))
                    SELECT \(selectColumns.joined(separator: ", ")) FROM \(quote(entityName))
                    """
            }
            let dropSQL = "DROP TABLE \(quote(entityName))"
            let renameSQL = "ALTER TABLE \(quote(temp)) RENAME TO \(quote(entityName))"
            let auxiliarySQL = Self.rebuiltAuxiliarySQL(
                table: entityName,
                new: new,
                oldAuxiliaryObjects: oldAuxiliaryObjects
            )
            return .init(
                table: entityName,
                temporaryTable: temp,
                createSQL: createSQL,
                copySQL: copySQL,
                dropSQL: dropSQL,
                renameSQL: renameSQL,
                auxiliarySQL: auxiliarySQL
            )
        }
        
        private static func requiresCustomAuxiliaryRewrite(_ operations: [Operation]) -> Bool {
            for operation in operations {
                switch operation {
                case .renameAttribute:
                    return true
                case .dropAttribute:
                    return true
                default:
                    break
                }
            }
            return false
        }
        
        private static func rebuiltAuxiliarySQL(
            table entityName: String,
            new: SchemaSet,
            oldAuxiliaryObjects: [AuxiliaryObject]
        ) -> [String] {
            let preservedTriggers = oldAuxiliaryObjects
                .filter { $0.kind == .trigger }
                .map(\.sql)
            let newIndexes = new.auxiliaryObjectsByTable[entityName, default: []]
                .filter { $0.kind == .index }
                .map(\.sql)
            return preservedTriggers + newIndexes
        }
        
        private static func uniqueIndexName(
            in schema: SchemaSet,
            table: String,
            columns: [String]
        ) -> String {
            if let match = schema.indexesByName[table]?.first(where: { index in
                index.isUnique &&
                index.columns.map { $0.expression.sql.trimmingCharacters(in: .whitespacesAndNewlines) } ==
                columns.map(quote)
            }) {
                return match.name
            }
            return "\(table)_\(columns.joined(separator: "_"))_Unique"
        }
        
        private static func columnNames(from definition: (any TableDefinition)?) -> [String] {
            guard let definition else {
                return []
            }
            return definition.columns.map(\.name)
        }
        
        private static func sqlType(
            for schema: SchemaSet,
            entity: String,
            property: String
        ) -> String {
            guard let table = schema.tablesByName[entity],
                  let column = table.columns.first(where: { $0.name == property }) else {
                return "BLOB"
            }
            return column.type.description
        }
    }
}

extension DataStoreMigration {
    public struct Classifier: Sendable {
        public let style: Style
        public let operations: [Operation]
        public let issues: [Issue]
        public let warnings: [Warning]
        
        public init(old: SchemaSet, new: SchemaSet, diff: SchemaDiff) throws {
            var operations = [Operation]()
            var issues = [Issue]()
            var warnings = [Warning]()
            for entity in diff.added {
                operations.append(.createEntity(name: entity.name))
            }
            for entity in diff.removed {
                operations.append(.dropEntity(name: entity.name))
            }
            for entityDiff in diff.changed {
                operations.append(contentsOf: entityDiff.operations)
                issues.append(contentsOf: entityDiff.issues)
                warnings.append(contentsOf: entityDiff.warnings)
            }
            let oldIndexesByTable = old.indexesByName
            let newIndexesByTable = new.indexesByName
            let indexTableNames = Set(oldIndexesByTable.keys).union(newIndexesByTable.keys)
            logger.info(
                "Starting schema diff on indexes.",
                metadata: [
                    "old_indexes_count": "\(old.sqlSchema.indexes.count)",
                    "new_indexes_count": "\(new.sqlSchema.indexes.count)"
                ]
            )
            // FIXME: Distinguish `INDEX` and `UNIQUE INDEX`.
            for tableName in indexTableNames.sorted() {
                let oldIndexes = oldIndexesByTable[tableName] ?? []
                let newIndexes = newIndexesByTable[tableName] ?? []
                let oldColumnGroups = Set(
                    oldIndexes.compactMap { index in
                        let names = index.columns.compactMap(\.name)
                        return names.count == index.columns.count ? names : nil
                    }
                )
                let newColumnGroups = Set(
                    newIndexes.compactMap { index in
                        let names = index.columns.compactMap(\.name)
                        return names.count == index.columns.count ? names : nil
                    }
                )
                for columns in newColumnGroups where !oldColumnGroups.contains(columns) {
                    guard let index = newIndexes.first(where: { index in
                        index.columns.compactMap(\.name) == columns
                    }) as? SQLIndex else {
                        logger.error(
                            "Unable to resolve SQL index for added indexed columns.",
                            metadata: ["table": "\(tableName)", "columns": "\(columns)"]
                        )
                        continue
                    }
                    operations.append(.addIndex(entity: tableName, index: index))
                    logger.info("Found indexed columns to add: \(tableName).\(columns)")
                }
                for columns in oldColumnGroups where !newColumnGroups.contains(columns) {
                    guard let index = oldIndexes.first(where: { index in
                        index.columns.compactMap(\.name) == columns
                    }) as? SQLIndex else {
                        logger.error(
                            "Unable to resolve SQL index for removed indexed columns.",
                            metadata: [
                                "table": "\(tableName)",
                                "columns": "\(columns)"
                            ]
                        )
                        continue
                    }
                    operations.append(.dropIndex(entity: tableName, index: index))
                    logger.info("Found indexed columns to remove: \(tableName).\(columns)")
                }
            }

            let addedTableNames = Set(diff.added.map(\.name))
            let removedTableNames = Set(diff.removed.map(\.name))
            let uniqueTableNames = Set(old.constraints.keys)
                .union(new.constraints.keys)
                .subtracting(addedTableNames)
                .subtracting(removedTableNames)

            for tableName in uniqueTableNames.sorted() {
                let oldUniqueColumnGroups = Set(old.constraints[tableName] ?? [])
                let newUniqueColumnGroups = Set(new.constraints[tableName] ?? [])
                for columns in newUniqueColumnGroups where !oldUniqueColumnGroups.contains(columns) {
                    operations.append(.addUniqueConstraint(entity: tableName, columns: columns))
                    warnings.append(.init(
                        kind: .dataValidationRequired,
                        entityName: tableName,
                        propertyName: nil
                    ))
                    logger.info("Found UNIQUE columns to add: \(tableName).\(columns)")
                }
                for columns in oldUniqueColumnGroups where !newUniqueColumnGroups.contains(columns) {
                    operations.append(.dropUniqueConstraint(entity: tableName, columns: columns))
                    logger.info("Found UNIQUE columns to remove: \(tableName).\(columns)")
                }
            }
            var seenOperations = Set<Operation>()
            operations = operations.filter { seenOperations.insert($0).inserted }
            var seenIssues = Set<Issue>()
            issues = issues.filter { seenIssues.insert($0).inserted }
            var seenWarnings = Set<Warning>()
            warnings = warnings.filter { seenWarnings.insert($0).inserted }
            let style: Style
            if operations.isEmpty {
                style = .compatible
            } else if issues.contains(where: { $0.severity == .unsupported }) {
                style = .unsupported
            } else if issues.contains(where: { $0.severity == .custom }) {
                style = .custom
            } else {
                style = .inferred
            }
            self.style = style
            self.operations = operations
            self.issues = issues
            self.warnings = warnings
        }
    }
}

extension DataStoreMigration {
    internal protocol Diffing {
        associatedtype Element
        associatedtype Inner
        var added: [Element] { get set }
        var removed: [Element] { get set }
        var changed: [Inner] { get set }
    }
    
    internal struct SchemaDiff: Diffing {
        internal var added: [Schema.Entity] = []
        internal var removed: [Schema.Entity] = []
        internal var changed: [EntityDiff] = []
        
        internal init(old oldSet: SchemaSet, new newSet: SchemaSet) throws {
            let oldEntities = oldSet.ormSchema.entities
            let newEntities = newSet.ormSchema.entities
            let allEntitiesByName = Set(oldEntities.map(\.name)).union(newEntities.map(\.name)).sorted()
            logger.info(
                "Starting schema diff on entities.",
                metadata: ["old": "\(oldEntities.count)", "new": "\(newEntities.count)"]
            )
            for (_, entityName) in allEntitiesByName.enumerated() {
                let oldEntity = oldSet.ormSchema.entitiesByName[entityName]
                let newEntity = newSet.ormSchema.entitiesByName[entityName]
                guard let oldEntity else {
                    added.append(newEntity!)
                    logger.info("Found entity to add: \(newEntity!.name)")
                    continue
                }
                guard let newEntity else {
                    removed.append(oldEntity)
                    logger.info("Found entity to remove: \(oldEntity.name)")
                    continue
                }
                if oldEntity != newEntity {
                    logger.info("Found entity with changes: \(newEntity.name)")
                    changed.append(try EntityDiff(old: oldEntity, new: newEntity))
                }
            }
        }
    }
    
    internal struct EntityDiff: Diffing {
        internal var added: [any SchemaProperty] = []
        internal var removed: [any SchemaProperty] = []
        internal var changed: [any Diffing] = []
        internal var operations: OrderedSet<Operation> = []
        internal var issues: OrderedSet<Issue> = []
        internal var warnings: OrderedSet<Warning> = []
        
        internal init(old oldEntity: Schema.Entity, new newEntity: Schema.Entity) throws {
            let oldProperties = oldEntity.storedProperties
            let newProperties = newEntity.storedProperties
            var consumedOldPropertyNames = Set<String>()
            logger.info(
                "Starting entity diff: \(newEntity.name)",
                metadata: [
                    "oldProperties.count": "\(oldProperties.count)",
                    "newProperties.count": "\(newProperties.count)"
                ]
            )
            for newProperty in newProperties {
                let oldProperty: (any SchemaProperty)?
                if !newProperty.originalName.isEmpty,
                   let originalProperty = oldEntity.storedPropertiesByName[newProperty.originalName] {
                    logger.info(
                        "Found an original property renamed.",
                        metadata: [
                            "old": "\(oldEntity.name).\(originalProperty.name)",
                            "new": "\(newEntity.name).\(newProperty.name)"
                        ]
                    )
                    oldProperty = originalProperty
                    consumedOldPropertyNames.insert(originalProperty.name)
                } else if let sameNameProperty = oldEntity.storedPropertiesByName[newProperty.name] {
                    oldProperty = sameNameProperty
                    consumedOldPropertyNames.insert(sameNameProperty.name)
                } else {
                    oldProperty = nil
                }
                guard let oldProperty else {
                    logger.info("Found property added to entity: \(newEntity.name).\(newProperty.name)")
                    added.append(newProperty)
                    switch newProperty {
                    case let attribute as Schema.Attribute:
                        operations.append(.addAttribute(
                            entity: newEntity.name,
                            name: attribute.name,
                            defaultValue: .init(any: attribute.defaultValue as Any),
                            isOptional: attribute.isOptional
                        ))
                        if attribute.isUnique {
                            operations.append(.addUniqueConstraint(entity: newEntity.name, columns: [attribute.name]))
                            warnings.append(.init(
                                kind: .dataValidationRequired,
                                entityName: newEntity.name,
                                propertyName: attribute.name
                            ))
                        }
                        if attribute.isOptional == false, attribute.defaultValue == nil {
                            issues.append(.init(
                                severity: .custom,
                                kind: .nonOptionalWithoutDefault,
                                entityName: newEntity.name,
                                propertyName: attribute.name
                            ))
                        }
                    case let relationship as Schema.Relationship:
                        operations.append(.addRelationship(entity: newEntity.name, name: relationship.name))
                        issues.append(.init(
                            severity: .custom,
                            kind: .relationshipTopologyChanged,
                            entityName: newEntity.name,
                            propertyName: relationship.name
                        ))
                    default:
                        break
                    }
                    continue
                }
                switch (oldProperty, newProperty) {
                case (let oldAttribute as Schema.Attribute, let newAttribute as Schema.Attribute):
                    if oldAttribute != newAttribute {
                        let diff = PropertyDiff(old: oldAttribute, new: newAttribute, entityName: newEntity.name)
                        logger.info("Found attribute has changed in entity: \(newEntity.name).\(newAttribute.name)")
                        operations.append(contentsOf: diff.operations)
                        issues.append(contentsOf: diff.issues)
                        warnings.append(contentsOf: diff.warnings)
                    }
                case (let oldRelationship as Schema.Relationship, let newRelationship as Schema.Relationship):
                    if oldRelationship != newRelationship {
                        let diff = PropertyDiff(old: oldRelationship, new: newRelationship, entityName: newEntity.name)
                        logger.info("Found relationship has changed in entity: \(newEntity.name).\(newRelationship.name)")
                        operations.append(contentsOf: diff.operations)
                        issues.append(contentsOf: diff.issues)
                        warnings.append(contentsOf: diff.warnings)
                    }
                default:
                    logger.notice("Found property has changed in entity: \(newEntity.name).\(newProperty.name)")
                    issues.append(.init(
                        severity: .unsupported,
                        kind: .unsupportedPropertyKindChange,
                        entityName: newEntity.name,
                        propertyName: newProperty.name
                    ))
                }
            }
            for oldProperty in oldProperties where !consumedOldPropertyNames.contains(oldProperty.name) {
                logger.info("Found property removed from entity: \(oldEntity.name).\(oldProperty.name)")
                removed.append(oldProperty)
                switch oldProperty {
                case let attribute as Schema.Attribute:
                    operations.append(.dropAttribute(entity: oldEntity.name, name: attribute.name))
                    warnings.append(.init(
                        kind: .destructiveChange,
                        entityName: oldEntity.name,
                        propertyName: attribute.name
                    ))
                case let relationship as Schema.Relationship:
                    operations.append(.dropRelationship(entity: oldEntity.name, name: relationship.name))
                    issues.append(.init(
                        severity: .custom,
                        kind: .relationshipTopologyChanged,
                        entityName: oldEntity.name,
                        propertyName: relationship.name
                    ))
                default:
                    break
                }
            }
        }
    }
    
    internal struct PropertyDiff<T>: Diffing where T: SchemaProperty {
        internal var added: [any SchemaProperty] = []
        internal var removed: [any SchemaProperty] = []
        internal var changed: [any SchemaProperty] = []
        internal var operations: OrderedSet<Operation> = []
        internal var issues: OrderedSet<Issue> = []
        internal var warnings: OrderedSet<Warning> = []
        
        internal init(old oldAttribute: T, new newAttribute: T, entityName: String) where T: Schema.Attribute {
            self = .init(oldProperty: oldAttribute, newProperty: newAttribute, entityName: entityName)
            if oldAttribute.isOptional != newAttribute.isOptional {
                if newAttribute.isOptional == false {
                    warnings.append(.init(
                        kind: .dataValidationRequired,
                        entityName: entityName,
                        propertyName: newAttribute.name
                    ))
                    issues.append(.init(
                        severity: .custom,
                        kind: .nonOptionalWithoutDefault,
                        entityName: entityName,
                        propertyName: newAttribute.name
                    ))
                }
                operations.append(.alterAttributeNullability(
                    entity: entityName,
                    name: newAttribute.name,
                    isOptional: newAttribute.isOptional
                ))
                logger.debug(
                    "Property diff changed optionality/nullability.",
                    metadata: [
                        "old": "\(oldAttribute.name) = \(oldAttribute.isOptional)",
                        "new": "\(newAttribute.name) = \(newAttribute.isOptional)"
                    ]
                )
            }
            if oldAttribute.isUnique != newAttribute.isUnique {
                if newAttribute.isUnique {
                    operations.append(.addUniqueConstraint(entity: entityName, columns: [newAttribute.name]))
                } else {
                    operations.append(.dropUniqueConstraint(entity: entityName, columns: [newAttribute.name]))
                }
                logger.debug(
                    "Property diff changed unique constraint.",
                    metadata: [
                        "old": "\(oldAttribute.name) = \(oldAttribute.isUnique)",
                        "new": "\(newAttribute.name) = \(newAttribute.isUnique)"
                    ]
                )
            }
            if ObjectIdentifier(oldAttribute.valueType) != ObjectIdentifier(newAttribute.valueType) {
                operations.append(.alterAttributeType(entity: entityName, name: newAttribute.name))
                issues.append(.init(
                    severity: .custom,
                    kind: .incompatibleTypeChange,
                    entityName: entityName,
                    propertyName: newAttribute.name
                ))
                warnings.append(.init(
                    kind: .dataValidationRequired,
                    entityName: entityName,
                    propertyName: newAttribute.name
                ))
                logger.debug(
                    "Property diff changed value type.",
                    metadata: [
                        "old": "\(oldAttribute.name) = \(String(reflecting: oldAttribute.valueType)).self",
                        "new": "\(newAttribute.name) = \(String(reflecting: newAttribute.valueType)).self",
                        "old_object_identifier": "\(ObjectIdentifier(oldAttribute.valueType))",
                        "new_object_identifier": "\(ObjectIdentifier(newAttribute.valueType))"
                    ]
                )
            }
            if oldAttribute.isTransformable != newAttribute.isTransformable {
                operations.append(.alterAttributeTransformable(
                    entity: entityName,
                    name: newAttribute.name
                ))
                issues.append(.init(
                    severity: .custom,
                    kind: .transformableRepresentationChanged,
                    entityName: entityName,
                    propertyName: newAttribute.name
                ))
            }
            if oldAttribute.hashModifier != nil {
                issues.append(.init(
                    severity: .unsupported,
                    kind: .unsupportedHashModifier,
                    entityName: entityName,
                    propertyName: newAttribute.name
                ))
            }
            if newAttribute.hashModifier != nil {
                issues.append(.init(
                    severity: .unsupported,
                    kind: .unsupportedHashModifier,
                    entityName: entityName,
                    propertyName: newAttribute.name
                ))
            }
            let allOptions = Set(oldAttribute.options).union(newAttribute.options)
            for option in allOptions {
                switch option {
                case .allowsCloudEncryption:
                    break
                case .ephemeral:
                    break
                case .externalStorage:
                    if oldAttribute.options.contains(.externalStorage) !=
                        newAttribute.options.contains(.externalStorage) {
                        issues.append(.init(
                            severity: .custom,
                            kind: .externalStorageSemanticsChanged,
                            entityName: entityName,
                            propertyName: newAttribute.name
                        ))
                    }
                case .preserveValueOnDeletion:
                    break
                case .spotlight:
                    break
                case .unique:
                    break
                default:
                    break
                }
            }
        }
        
        internal init(old oldRelationship: T, new newRelationship: T, entityName: String) where T: Schema.Relationship {
            self = .init(oldProperty: oldRelationship, newProperty: newRelationship, entityName: entityName)
            var topologyChanged = false
            if oldRelationship.isToOneRelationship != newRelationship.isToOneRelationship {
                topologyChanged = true
            }
            if oldRelationship.destination != newRelationship.destination {
                topologyChanged = true
            }
            if oldRelationship.inverseName != newRelationship.inverseName {
                topologyChanged = true
            }
            if oldRelationship.inverseKeyPath != newRelationship.inverseKeyPath {
                topologyChanged = true
            }
            if oldRelationship.keypath != newRelationship.keypath {
                topologyChanged = true
            }
            if oldRelationship.minimumModelCount != newRelationship.minimumModelCount {
                topologyChanged = true
            }
            if oldRelationship.maximumModelCount != newRelationship.maximumModelCount {
                topologyChanged = true
            }
            if oldRelationship.deleteRule != newRelationship.deleteRule {
                topologyChanged = true
            }
            if topologyChanged {
                operations.append(.alterRelationship(entity: entityName, name: newRelationship.name))
                issues.append(.init(
                    severity: .custom,
                    kind: .relationshipTopologyChanged,
                    entityName: entityName,
                    propertyName: newRelationship.name
                ))
            }
            if oldRelationship.hashModifier != nil {
                issues.append(.init(
                    severity: .unsupported,
                    kind: .unsupportedHashModifier,
                    entityName: entityName,
                    propertyName: newRelationship.name
                ))
            }
            if newRelationship.hashModifier != nil {
                issues.append(.init(
                    severity: .unsupported,
                    kind: .unsupportedHashModifier,
                    entityName: entityName,
                    propertyName: newRelationship.name
                ))
            }
            if oldRelationship.isToOneRelationship && newRelationship.isToOneRelationship,
               oldRelationship.options.contains(.unique) != newRelationship.options.contains(.unique) {
                let relationshipColumns = [newRelationship.name + "_pk"]
                if newRelationship.options.contains(.unique) {
                    operations.append(.addUniqueConstraint(entity: entityName, columns: relationshipColumns))
                    warnings.append(.init(
                        kind: .dataValidationRequired,
                        entityName: entityName,
                        propertyName: newRelationship.name + "_pk"
                    ))
                } else {
                    operations.append(.dropUniqueConstraint(
                        entity: entityName,
                        columns: relationshipColumns
                    ))
                }
            }
            let allOptions = Set(oldRelationship.options).union(newRelationship.options)
            for option in allOptions {
                switch option {
                case .unique:
                    break
                default:
                    break
                }
            }
        }
        
        private init(oldProperty: T, newProperty: T, entityName: String) where T: SchemaProperty {
            logger.info("Starting property diff: \(newProperty.name)", metadata: nil)
            if oldProperty.name != newProperty.name {
                if T.self is Schema.Attribute.Type {
                    operations.append(.renameAttribute(entity: entityName, from: oldProperty.name, to: newProperty.name))
                }
                if T.self is Schema.Relationship.Type {
                    operations.append(.renameRelationship(entity: entityName, from: oldProperty.name, to: newProperty.name))
                }
                if !newProperty.originalName.isEmpty {
                    precondition(
                        oldProperty.name == newProperty.originalName,
                        "Expected old property name to match original name."
                    )
                }
                logger.debug(
                    "Property diff found mismatch in name.",
                    metadata: [
                        "oldProperty.name": "\(entityName).\(oldProperty.name)",
                        "newProperty.name": "\(entityName).\(newProperty.name)"
                    ]
                )
            }
        }
    }
}
