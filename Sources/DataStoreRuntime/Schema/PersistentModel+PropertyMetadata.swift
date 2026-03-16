//
//  PersistentModel+PropertyMetadata.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSQL
import Logging
import ObjectiveC
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.bootstrap")

extension PersistentModel where Self: AnyObject {
    nonisolated public static var databaseSchemaMetadata: [PropertyMetadata] {
        _getOrInitializeSnapshot(for: Self.self, make: { [] }).entries
    }
}

extension PersistentModel where Self: AnyObject {
    nonisolated public static func schemaMetadata(for keyPath: PartialKeyPath<Self> & Sendable)
    -> PropertyMetadata? {
        let storage = _getOrInitializeSnapshot(for: Self.self, make: { [] })
        guard let index = storage.indexByKeyPath[keyPath] else {
            logger.trace("Key path not found in index map for \(Self.self).self: \(keyPath)")
            return nil
        }
        return storage.entries[index]
    }
    
    /// Get the `PropertyMetadata` by providing the key path that is mapped to it.
    nonisolated public static func schemaMetadata(for keyPath: AnyKeyPath & Sendable)
    -> PropertyMetadata? {
        let storage = _getOrInitializeSnapshot(for: Self.self, make: { [] })
        guard let index = storage.indexByKeyPath[keyPath] else {
            logger.trace("Key path not found in index map for \(Self.self).self: \(keyPath)")
            return nil
        }
        return storage.entries[index]
    }
    
    /// Get the `PropertyMetadata` by providing the property name (not the column name).
    nonisolated public static func schemaMetadata(for name: String) -> PropertyMetadata? {
        let storage = _getOrInitializeSnapshot(for: Self.self, make: { [] })
        guard let index = storage.indexByName[name] else {
            logger.trace("Name not found in index map for \(Self.self).self: \(name)")
            return nil
        }
        return storage.entries[index]
    }
    
    nonisolated package static var schemaMetadataIndexByKeyPath: [AnyKeyPath: Int] {
        _getOrInitializeSnapshot(for: Self.self, make: { [] }).indexByKeyPath
    }
    
    nonisolated package static var schemaMetadataIndexByName: [String: Int] {
        _getOrInitializeSnapshot(for: Self.self, make: { [] }).indexByName
    }
    
    nonisolated package static func schemaMetadataByKeyPath() -> [AnyKeyPath & Sendable: PropertyMetadata] {
        _withPropertyMetadata { buffer in
            var dictionary = Dictionary<AnyKeyPath & Sendable, PropertyMetadata>(minimumCapacity: buffer.count)
            guard let baseAddress = buffer.baseAddress else {
                return dictionary
            }
            var pointer = baseAddress
            let end = baseAddress.advanced(by: buffer.count)
            while pointer != end {
                let property = pointer.pointee
                dictionary[property.keyPath] = consume property
                pointer = pointer.advanced(by: 1)
            }
            return dictionary
        }
    }
    
    /// Register `variant` as another key-path that identifies the same property
    /// described by `propertyMetadata` within the given `class`.
    ///
    /// This uses `propertyMetadata.keyPath` as the canonical root key path.
    nonisolated package static func addKeyPathVariantToPropertyMetadata(
        _ variant: AnyKeyPath & Sendable,
        for property: PropertyMetadata
    ) {
        let `class`: AnyClass = Self.self
        let oldSnapshot = _getOrInitializeSnapshot(for: `class`) { [] }
        let canonical = property.keyPath
        let newSnapshot = oldSnapshot.addingKeyPathVariant(variant, canonical: canonical)
        guard newSnapshot !== oldSnapshot else { return }
        _publishSnapshot(newSnapshot, in: `class`)
        logger.debug("Added a key path alias: \(Self.self).\(property.name) -> \(canonical) = \(variant)")
    }
    
    nonisolated package static func addKeyPathVariantToPropertyMetadata(
        _ variant: AnyKeyPath & Sendable,
        canonical canonicalKeyPath: AnyKeyPath & Sendable
    ) {
        let `class`: AnyClass = Self.self
        let oldSnapshot = _getOrInitializeSnapshot(for: `class`) { [] }
        let canonical = canonicalKeyPath
        let newSnapshot = oldSnapshot.addingKeyPathVariant(variant, canonical: canonical)
        guard newSnapshot !== oldSnapshot else { return }
        _publishSnapshot(newSnapshot, in: `class`)
        logger.trace("Added a key path alias: \(Self.self) -> \(canonical) = \(variant)")
    }
    
    nonisolated package static func appendPropertyMetadata(_ property: PropertyMetadata) {
        var snapshotCopy = Self.databaseSchemaMetadata
        snapshotCopy.append(property)
        Self.overwritePropertyMetadata(consume snapshotCopy)
        logger.debug("Appended a new PropertyMetadata: \(property)")
    }
    
    nonisolated package static func overwritePropertyMetadata(_ newProperties: [PropertyMetadata]) {
        _overwriteSnapshot(newProperties, in: Self.self)
    }
    
    nonisolated package static func clearPropertyMetadataCache() {
        _clearSnapshot(for: Self.self)
    }
}

extension PersistentModel where Self: AnyObject {
    nonisolated internal static
    func _withPropertyMetadata<T>(_ body: (UnsafeBufferPointer<PropertyMetadata>) -> T) -> T {
        return _getOrInitializeSnapshot(for: Self.self, make: { [] })
            .entries
            .withUnsafeBufferPointer(body)
    }
    
    nonisolated internal static func _schemaMetadata(for keyPath: AnyKeyPath & Sendable)
    -> PropertyMetadata? {
        let targetKeyPath = ObjectIdentifier(keyPath)
        return _withPropertyMetadata { buffer in
            guard let baseAddress = buffer.baseAddress else {
                return nil
            }
            var pointer = baseAddress
            let end = baseAddress.advanced(by: buffer.count)
            while pointer != end {
                if ObjectIdentifier(pointer.pointee.keyPath) == targetKeyPath {
                    let index = pointer - baseAddress
                    return buffer[index]
                }
                pointer = pointer.advanced(by: 1)
            }
            return nil
        }
    }
}

private final class _AssociationKeyToken {}

nonisolated private let _associationKeyBits: Atomic<UInt> = .init(0)

@inline(__always) nonisolated
private func _makeKeyPointer() -> UnsafeRawPointer {
    let bits = _associationKeyBits.load(ordering: .acquiring)
    if bits != 0 { return UnsafeRawPointer(bitPattern: bits).unsafelyUnwrapped }
    let raw = Unmanaged.passUnretained(_AssociationKeyToken.self as AnyObject).toOpaque()
    let newBits = UInt(bitPattern: Int(bitPattern: raw))
    _ = _associationKeyBits.compareExchange(
        expected: 0,
        desired: newBits,
        ordering: .acquiringAndReleasing
    )
    let published = _associationKeyBits.load(ordering: .acquiring)
    return UnsafeRawPointer(bitPattern: published).unsafelyUnwrapped
}

private final class _Box: Sendable {
    nonisolated fileprivate let reference: AtomicLazyReference<_Snapshot> = .init()
}

@inline(__always) nonisolated
private func _getOrInitializeBox(for `class`: AnyClass) -> _Box {
    let key = _makeKeyPointer()
    if let box = objc_getAssociatedObject(`class`, key) as? _Box { return box }
    let newBox = _Box()
    objc_setAssociatedObject(`class`, key, newBox, .OBJC_ASSOCIATION_RETAIN)
    return (objc_getAssociatedObject(`class`, key) as? _Box) ?? newBox
}

private final class _Snapshot: Sendable {
    nonisolated fileprivate let entries: [PropertyMetadata]
    nonisolated fileprivate let indexByKeyPath: [AnyKeyPath & Sendable: Int]
    nonisolated fileprivate let indexByName: [String: Int]
    
    nonisolated fileprivate init(
        entries: [PropertyMetadata],
        indexByKeyPath: [AnyKeyPath & Sendable: Int],
        indexByName: [String: Int]
    ) {
        self.entries = entries
        self.indexByKeyPath = indexByKeyPath
        self.indexByName = indexByName
    }
    
    nonisolated fileprivate convenience init(_ entries: [PropertyMetadata]) {
        let count = entries.count
        var byKeyPath = Dictionary<AnyKeyPath & Sendable, Int>(minimumCapacity: count)
        var byName = Dictionary<String, Int>(minimumCapacity: count)
        for (index, entry) in entries.enumerated() {
            byKeyPath[entry.keyPath] = index
            byName[entry.name] = index
        }
        self.init(
            entries: entries,
            indexByKeyPath: consume byKeyPath,
            indexByName: consume byName
        )
    }
}

extension _Snapshot {
    /// Return a new snapshot with `variant` added as another key-path for the same property as `canonical`.
    /// If canonical isn't known, this is a no-op.
    nonisolated fileprivate func addingKeyPathVariant(
        _ variant: AnyKeyPath & Sendable,
        canonical: AnyKeyPath & Sendable
    ) -> _Snapshot {
        guard let index = self.indexByKeyPath[canonical] else { return self }
        if indexByKeyPath[variant] == index { return self }
        var newIndexByKeyPath = self.indexByKeyPath
        newIndexByKeyPath[variant] = index
        return .init(entries: entries, indexByKeyPath: newIndexByKeyPath, indexByName: indexByName)
    }
}

@inline(__always) nonisolated
private func _getOrInitializeSnapshot(for `class`: AnyClass, make: () -> [PropertyMetadata])
-> _Snapshot {
    let box = _getOrInitializeBox(for: `class`)
    if let snapshot = box.reference.load() { return snapshot }
    return box.reference.storeIfNil(_Snapshot(make()))
}

@inline(__always) nonisolated
private func _overwriteSnapshot(_ entries: [PropertyMetadata], in `class`: AnyClass) {
    let newSnapshot = _Snapshot(consume entries)
    _publishSnapshot(newSnapshot, in: `class`)
}

@inline(__always) nonisolated
private func _publishSnapshot(_ snapshot: _Snapshot, in `class`: AnyClass) {
    let key = _makeKeyPointer()
    let newBox = _Box()
    _ = newBox.reference.storeIfNil(consume snapshot)
    objc_setAssociatedObject(`class`, key, newBox, .OBJC_ASSOCIATION_RETAIN)
}

@inline(__always) nonisolated
private func _clearSnapshot(for `class`: AnyClass) {
    objc_setAssociatedObject(`class`, _makeKeyPointer(), nil, .OBJC_ASSOCIATION_RETAIN)
}
