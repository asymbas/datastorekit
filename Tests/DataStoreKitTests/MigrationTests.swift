//
//  MigrationTests.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreKit
import Foundation
import SwiftData
import Testing
import TestSupport

@Suite(.bootstrap, .serialized)
struct MigrationTests {
    private static func makeTemporaryDirectory() throws -> URL {
        let component = "DataStoreKit-Migration-\(UUID().uuidString)"
        let directoryURL = URL.temporaryDirectory.appending(component: component, directoryHint: .isDirectory)
        try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        return directoryURL
    }
    
    private func makeModelContainer(
        _ versionedSchema: any VersionedSchema.Type,
        in directoryURL: URL
    ) throws -> ModelContainer {
        let schema = Schema(versionedSchema: versionedSchema)
        var lastError: any Error = CocoaError(.fileReadUnknown)
        for _ in 0..<30 {
            do {
                return try ModelContainer(
                    for: schema,
                    configurations: [DatabaseConfiguration(
                        name: "Migration",
                        types: versionedSchema.models,
                        schema: schema,
                        url: directoryURL
                    )]
                )
            } catch {
                lastError = error
                guard "\(error)".localizedCaseInsensitiveContains("busy") else {
                    throw error
                }
                Thread.sleep(forTimeInterval: 0.05)
            }
        }
        throw lastError
    }
    
    private func expectMigrationThrows(
        _ versioned: any VersionedSchema.Type,
        in directory: URL,
        messageContains expected: String,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        do {
            _ = try makeModelContainer(versioned, in: directory)
            Issue.record(
                "Expected migration to throw, but it succeeded.",
                sourceLocation: sourceLocation
            )
        } catch {
            #expect(
                "\(error)".localizedCaseInsensitiveContains(expected),
                "Unexpected error message: \(error)",
                sourceLocation: sourceLocation
            )
        }
    }
    
    @Test("Fresh install creates the schema without a prior store")
    func freshInstallCreatesSchema() throws {
        let directory = try Self.makeTemporaryDirectory()
        let modelContainer = try makeModelContainer(BaseV1.self, in: directory)
        let modelContext = ModelContext(modelContainer)
        modelContext.insert(BaseV1.Item(id: "a", name: "A"))
        try modelContext.save()
        let items = try modelContext.fetch(FetchDescriptor<BaseV1.Item>())
        #expect(items.count == 1)
    }
    
    @Test("Adding an optional attribute is lightweight and preserves rows")
    func addOptionalAttributeIsLightweight() throws {
        let directory = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(BaseV1.self, in: directory)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(BaseV1.Item(id: "a", name: "A"))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(AddOptionalV2.self, in: directory)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<AddOptionalV2.Item>())
        #expect(items.count == 1)
        #expect(items.first?.name == "A")
        #expect(items.first?.note == nil)
    }
    
    @Test("Adding a non-optional attribute with a default backfills existing rows")
    func addDefaultedAttributeBackfills() throws {
        let directory = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(BaseV1.self, in: directory)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(BaseV1.Item(id: "a", name: "A"))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(AddDefaultedV2.self, in: directory)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<AddDefaultedV2.Item>())
        #expect(items.count == 1)
        #expect(items.first?.count == 0)
    }
    
    @Test("Adding a non-optional attribute without a default succeeds on an empty store")
    func addRequiredNoDefaultOnEmptyStoreSucceeds() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            _ = try makeModelContainer(BaseV1.self, in: directoryURL)
        }
        let modelContainer = try makeModelContainer(AddRequiredNoDefaultV2.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<AddRequiredNoDefaultV2.Item>())
        #expect(items.isEmpty)
    }
    
    @Test("Adding a non-optional attribute without a default throws on a populated store")
    func addRequiredNoDefaultOnPopulatedStoreThrows() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(BaseV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(BaseV1.Item(id: "a", name: "A"))
            try modelContext.save()
        }
        expectMigrationThrows(AddRequiredNoDefaultV2.self, in: directoryURL, messageContains: "empty")
    }
    
    @Test("Renaming an attribute via originalName is lightweight and preserves values")
    func renameAttributeIsLightweight() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(BaseV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(BaseV1.Item(id: "a", name: "A"))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(RenameV2.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<RenameV2.Item>())
        #expect(items.count == 1)
        #expect(items.first?.fullName == "A")
    }
    
    @Test("Tightening optional to non-optional succeeds when no nulls exist")
    func tightenNullabilityWithoutNullsSucceeds() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(OptionalNoteV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(OptionalNoteV1.Item(id: "a", name: "A", note: "present"))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(RequiredNoteV2.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<RequiredNoteV2.Item>())
        #expect(items.count == 1)
        #expect(items.first?.note == "present")
    }
    
    @Test("Tightening optional to non-optional throws when nulls exist")
    func tightenNullabilityWithNullsThrows() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(OptionalNoteV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(OptionalNoteV1.Item(id: "a", name: "A", note: nil))
            try modelContext.save()
        }
        expectMigrationThrows(RequiredNoteV2.self, in: directoryURL, messageContains: "null")
    }
    
    @Test("Adding a new entity is lightweight")
    func addEntityIsLightweight() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(BaseV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(BaseV1.Item(id: "a", name: "A"))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(AddEntityV2.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        modelContext.insert(AddEntityV2.Tag(id: "t", label: "Label"))
        try modelContext.save()
        let tags = try modelContext.fetch(FetchDescriptor<AddEntityV2.Tag>())
        #expect(tags.count == 1)
    }
    
    @Test("Dropping an empty entity succeeds")
    func dropEmptyEntitySucceeds() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(AddEntityV2.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(AddEntityV2.Item(id: "a", name: "A"))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(BaseV1.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<BaseV1.Item>())
        #expect(items.count == 1)
    }
    
    @Test("Changing an attribute type requires a custom migration")
    func typeChangeRequiresCustomMigration() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(CodeIntV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(CodeIntV1.Item(id: "a", code: 7))
            try modelContext.save()
        }
        expectMigrationThrows(CodeStringV2.self, in: directoryURL, messageContains: "custom migration")
    }
    
    @Test("Adding a relationship requires a custom migration and throws")
    func addRelationshipRequiresCustomMigration() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(BaseV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(BaseV1.Item(id: "a", name: "A"))
            try modelContext.save()
        }
        expectMigrationThrows(AddRelationshipV2.self, in: directoryURL, messageContains: "custom migration")
    }
    
    @Test("Converting a model into a subclass requires a custom migration")
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func convertingIntoInheritanceRequiresCustomMigration() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(FlatAnimalV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(FlatAnimalV1.Animal(id: "a", name: "Rex"))
            try modelContext.save()
        }
        expectMigrationThrows(InheritedAnimalV2.self, in: directoryURL, messageContains: "custom migration")
    }
    
    @Test("Converting a model out of a subclass requires a custom migration")
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    func convertingOutOfInheritanceRequiresCustomMigration() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(InheritedAnimalV2.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(InheritedAnimalV2.Animal(id: "a", name: "Rex"))
            try modelContext.save()
        }
        expectMigrationThrows(FlatAnimalV1.self, in: directoryURL, messageContains: "custom migration")
    }
    
    @Test("Dropping an attribute is lightweight and preserves remaining data")
    func dropAttributeIsLightweight() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(DroppableV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(DroppableV1.Item(id: "a", name: "A", scratch: "temporary"))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(BaseV1.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<BaseV1.Item>())
        #expect(items.count == 1)
        #expect(items.first?.name == "A")
    }
    
    @Test("A compound add and drop in one version migrates together")
    func compoundAddAndDropIsLightweight() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(DroppableV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(DroppableV1.Item(id: "a", name: "A", scratch: "temporary"))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(AddOptionalV2.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<AddOptionalV2.Item>())
        #expect(items.count == 1)
        #expect(items.first?.name == "A")
        #expect(items.first?.note == nil)
    }
    
    @Test("A custom migration handler resolves a type change end to end")
    func customMigrationHandlerResolvesTypeChange() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(CodeIntV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(CodeIntV1.Item(id: "a", code: 7))
            try modelContext.save()
        }
        let schema = Schema(versionedSchema: CodeStringV2.self)
        let configuration = DatabaseConfiguration(
            name: "Migration",
            types: CodeStringV2.models,
            schema: schema,
            url: directoryURL,
            customMigration: { _, connection in
                _ = try connection.execute("ALTER TABLE Item RENAME COLUMN code TO code_old")
                _ = try connection.execute("ALTER TABLE Item ADD COLUMN code TEXT")
                _ = try connection.execute("UPDATE Item SET code = CAST(code_old AS TEXT)")
                _ = try connection.execute("ALTER TABLE Item DROP COLUMN code_old")
            }
        )
        let modelContainer = try ModelContainer(for: schema, configurations: [configuration])
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<CodeStringV2.Item>())
        #expect(items.count == 1)
        #expect(items.first?.code == "7")
    }
    
    @Test("Enabling external storage relocates inline data and preserves it")
    func enablingExternalStorageRelocatesData() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(InlineBlobV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(InlineBlobV1.Item(id: "a", blob: Data("hello".utf8)))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(ExternalBlobV2.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<ExternalBlobV2.Item>())
        #expect(items.count == 1)
        #expect(items.first?.blob == Data("hello".utf8))
    }
    
    @Test("Disabling external storage relocates data back inline and preserves it")
    func disablingExternalStorageRelocatesData() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(ExternalStoredV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(ExternalStoredV1.Item(id: "a", blob: Data("world".utf8)))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(InlineRestoredV2.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<InlineRestoredV2.Item>())
        #expect(items.count == 1)
        #expect(items.first?.blob == Data("world".utf8))
    }
    
    @Test("Adding a unique constraint with duplicates throws")
    func addUniqueConstraintWithDuplicatesThrows() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(NonUniqueCodeV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(NonUniqueCodeV1.Item(id: "a", code: "dup"))
            modelContext.insert(NonUniqueCodeV1.Item(id: "b", code: "dup"))
            try modelContext.save()
        }
        expectMigrationThrows(UniqueCodeV2.self, in: directoryURL, messageContains: "duplicate")
    }
    
    @Test("Adding a unique constraint without duplicates succeeds")
    func addUniqueConstraintWithoutDuplicatesSucceeds() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(NonUniqueCodeV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(NonUniqueCodeV1.Item(id: "a", code: "one"))
            modelContext.insert(NonUniqueCodeV1.Item(id: "b", code: "two"))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(UniqueCodeV2.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<UniqueCodeV2.Item>())
        #expect(items.count == 2)
    }
    
    @Test("A multi-entity lightweight migration preserves both entities")
    func multiEntityLightweightMigration() throws {
        let directoryURL = try Self.makeTemporaryDirectory()
        do {
            let modelContainer = try makeModelContainer(TwoEntityV1.self, in: directoryURL)
            let modelContext = ModelContext(modelContainer)
            modelContext.insert(TwoEntityV1.Item(id: "a", name: "A"))
            modelContext.insert(TwoEntityV1.Tag(id: "t", label: "L"))
            try modelContext.save()
        }
        let modelContainer = try makeModelContainer(TwoEntityV2.self, in: directoryURL)
        let modelContext = ModelContext(modelContainer)
        let items = try modelContext.fetch(FetchDescriptor<TwoEntityV2.Item>())
        let tags = try modelContext.fetch(FetchDescriptor<TwoEntityV2.Tag>())
        #expect(items.count == 1)
        #expect(items.first?.name == "A")
        #expect(tags.count == 1)
        #expect(tags.first?.label == "L")
    }
}

extension MigrationTests {
    enum BaseV1: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(1, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var name: String
            
            init(id: String, name: String) {
                self.id = id
                self.name = name
            }
        }
    }
    
    enum AddOptionalV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var name: String
            var note: String?
            
            init(id: String, name: String, note: String? = nil) {
                self.id = id
                self.name = name
                self.note = note
            }
        }
    }
    
    enum AddDefaultedV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var name: String
            var count: Int = 0
            
            init(id: String, name: String) {
                self.id = id
                self.name = name
            }
        }
    }
    
    enum AddRequiredNoDefaultV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var name: String
            var weight: Double
            
            init(id: String, name: String, weight: Double) {
                self.id = id
                self.name = name
                self.weight = weight
            }
        }
    }
    
    enum RenameV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            @Attribute(originalName: "name") var fullName: String
            
            init(id: String, fullName: String) {
                self.id = id
                self.fullName = fullName
            }
        }
    }
    
    enum OptionalNoteV1: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(1, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var name: String
            var note: String?
            
            init(id: String, name: String, note: String?) {
                self.id = id
                self.name = name
                self.note = note
            }
        }
    }
    
    enum RequiredNoteV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var name: String
            var note: String = ""
            
            init(id: String, name: String, note: String) {
                self.id = id
                self.name = name
                self.note = note
            }
        }
    }
    
    enum AddEntityV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self, Tag.self]
        
        @Model final class Item {
            var id: String
            var name: String
            
            init(id: String, name: String) {
                self.id = id
                self.name = name
            }
        }
        
        @Model final class Tag {
            var id: String
            var label: String
            
            init(id: String, label: String) {
                self.id = id
                self.label = label
            }
        }
    }
    
    enum CodeIntV1: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(1, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var code: Int
            
            init(id: String, code: Int) {
                self.id = id
                self.code = code
            }
        }
    }
    
    enum CodeStringV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var code: String
            
            init(id: String, code: String) {
                self.id = id
                self.code = code
            }
        }
    }
    
    enum AddRelationshipV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self, Tag.self]
        
        @Model final class Item {
            var id: String
            var name: String
            var tag: Tag?
            
            init(id: String, name: String, tag: Tag? = nil) {
                self.id = id
                self.name = name
                self.tag = tag
            }
        }
        
        @Model final class Tag {
            var id: String
            var label: String
            
            init(id: String, label: String) {
                self.id = id
                self.label = label
            }
        }
    }
    
    enum InlineBlobV1: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(1, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var blob: Data
            
            init(id: String, blob: Data) {
                self.id = id
                self.blob = blob
            }
        }
    }
    
    enum ExternalBlobV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            @Attribute(.externalStorage) var blob: Data
            
            init(id: String, blob: Data) {
                self.id = id
                self.blob = blob
            }
        }
    }
    
    enum ExternalStoredV1: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(1, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            @Attribute(.externalStorage) var blob: Data
            
            init(id: String, blob: Data) {
                self.id = id
                self.blob = blob
            }
        }
    }
    
    enum InlineRestoredV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var blob: Data
            
            init(id: String, blob: Data) {
                self.id = id
                self.blob = blob
            }
        }
    }
    
    enum NonUniqueCodeV1: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(1, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var code: String
            
            init(id: String, code: String) {
                self.id = id
                self.code = code
            }
        }
    }
    
    enum UniqueCodeV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            @Attribute(.unique) var code: String
            
            init(id: String, code: String) {
                self.id = id
                self.code = code
            }
        }
    }
    
    enum TwoEntityV1: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(1, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self, Tag.self]
        
        @Model final class Item {
            var id: String
            var name: String
            
            init(id: String, name: String) {
                self.id = id
                self.name = name
            }
        }
        
        @Model final class Tag {
            var id: String
            var label: String
            
            init(id: String, label: String) {
                self.id = id
                self.label = label
            }
        }
    }
    
    enum TwoEntityV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self, Tag.self]
        
        @Model final class Item {
            var id: String
            var name: String
            var note: String?
            
            init(id: String, name: String, note: String? = nil) {
                self.id = id
                self.name = name
                self.note = note
            }
        }
        
        @Model final class Tag {
            var id: String
            var label: String
            var color: String?
            
            init(id: String, label: String, color: String? = nil) {
                self.id = id
                self.label = label
                self.color = color
            }
        }
    }
    
    enum DroppableV1: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(1, 0, 0)
        static let models: [any PersistentModel.Type] = [Item.self]
        
        @Model final class Item {
            var id: String
            var name: String
            var scratch: String
            
            init(id: String, name: String, scratch: String) {
                self.id = id
                self.name = name
                self.scratch = scratch
            }
        }
    }
    
    enum FlatAnimalV1: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(1, 0, 0)
        static let models: [any PersistentModel.Type] = [Animal.self]
        
        @Model final class Animal {
            var id: String
            var name: String
            
            init(id: String, name: String) {
                self.id = id
                self.name = name
            }
        }
    }
    
    @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
    enum InheritedAnimalV2: VersionedSchema {
        static let versionIdentifier: Schema.Version = .init(2, 0, 0)
        static let models: [any PersistentModel.Type] = [Creature.self, Animal.self]
        
        @Model class Creature {
            var id: String
            
            init(id: String) {
                self.id = id
            }
        }
        
        @available(iOS 26.0, macOS 26.0, tvOS 26.0, visionOS 26.0, watchOS 26.0, *)
        @Model class Animal: Creature {
            var name: String
            
            init(id: String, name: String) {
                self.name = name
                super.init(id: id)
            }
        }
    }
}
