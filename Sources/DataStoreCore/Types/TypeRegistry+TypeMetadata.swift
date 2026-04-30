//
//  TypeRegistry+TypeMetadata.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Logging
private import ObjectiveC
private import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

extension TypeRegistry {
    /// All registered `TypeMetadata` entries in the current snapshot (with stable ordering).
    nonisolated public static var entries: [TypeMetadata] {
        _getOrInitializeSnapshot().entries
    }
    
    /// Get the `TypeMetadata` entry for the given type.
    nonisolated public static func getValue(forType type: AnyClass) -> TypeMetadata? {
        let snapshot = _getOrInitializeSnapshot()
        guard let index = snapshot.indexByType[ObjectIdentifier(type)] else {
            return nil
        }
        return snapshot.entries[index]
    }
    
    /// Get the `TypeMetadata` entry for the given type name.
    nonisolated public static func getValue(forTypeName typeName: String) -> TypeMetadata? {
        let snapshot = _getOrInitializeSnapshot()
        guard let index = snapshot.indexByTypeName[typeName] else {
            return nil
        }
        return snapshot.entries[index]
    }
    
    /// Get the `TypeMetadata` entry for the given mangled type name.
    nonisolated public static func getValue(forMangledTypeName mangledTypeName: String) -> TypeMetadata? {
        let snapshot = _getOrInitializeSnapshot()
        guard let index = snapshot.indexByMangledTypeName[mangledTypeName] else {
            return nil
        }
        return snapshot.entries[index]
    }
    
    /// Get the type for the given type name.
    nonisolated public static func getType(forName typeName: String) -> AnyClass? {
        getValue(forTypeName: typeName)?.type
    }
    
    /// Get the type for the given mangled type name.
    nonisolated public static func getType(forMangledName mangledTypeName: String) -> AnyClass? {
        getValue(forMangledTypeName: mangledTypeName)?.type
    }
    
    /// Get the type name for the given type.
    nonisolated public static func getName(forType type: AnyClass) -> String? {
        getValue(forType: type)?.typeName
    }
    
    /// Get the mangled type name for the given type.
    nonisolated public static func getMangledName(forType type: AnyClass) -> String? {
        getValue(forType: type)?.mangledTypeName
    }
    
    /// Get the metadata for the given type.
    nonisolated public static func getMetadata(forType type: AnyClass) -> (any Sendable)? {
        getValue(forType: type)?.metadata
    }
    
    /// Get the metadata for the given type name.
    nonisolated public static func getMetadata(forName typeName: String) -> (any Sendable)? {
        getValue(forTypeName: typeName)?.metadata
    }
    
    /// Get the metadata for the given mangled type name.
    nonisolated public static func getMetadata(forMangledName mangledTypeName: String) -> (any Sendable)? {
        getValue(forMangledTypeName: mangledTypeName)?.metadata
    }
}

nonisolated private let _lock: Mutex<Void> = .init(())

extension TypeRegistry {
    /// Registers a new metatype in the registry.
    nonisolated public static func register<T>(_ metatype: T.Type) {
        let (pointer, length) = _getTypeName(metatype, qualified: true)
        let string = String(decoding: UnsafeBufferPointer(start: pointer, count: length), as: UTF8.self)
        Self.register(metatype as! AnyClass, typeName: string, mangledTypeName: _mangledTypeName(T.self)!)
    }
    
    /// Upserts by type, replacing any existing records that collide by `type`, `typeName`, or `mangledTypeName`.
    nonisolated public static func register(
        _ type: AnyClass,
        typeName: String,
        mangledTypeName: String,
        metadata: (any Sendable)? = nil
    ) {
        _lock.withLock { _ in
            let currentEntries = _getOrInitializeSnapshot().entries
            var nextEntries = [TypeMetadata]()
            nextEntries.reserveCapacity(max(currentEntries.count, 1))
            let targetType = ObjectIdentifier(type)
            for currentEntry in currentEntries {
                if ObjectIdentifier(currentEntry.type) == targetType { continue }
                if currentEntry.typeName == typeName { continue }
                if currentEntry.mangledTypeName == mangledTypeName { continue }
                nextEntries.append(currentEntry)
            }
            nextEntries.append(TypeMetadata(
                type: type,
                typeName: typeName,
                mangledTypeName: mangledTypeName,
                metadata: metadata
            ))
            _overwriteSnapshot(consume nextEntries)
        }
    }
    
    /// Registers a type with optional name overrides.
    nonisolated public static func register<T: AnyObject>(
        _ type: T.Type,
        typeName: String? = nil,
        mangledTypeName: String? = nil,
        metadata: (any Sendable)? = nil
    ) {
        let genericType: Any.Type? = type
        let type: Any.Type? = genericType ?? (mangledTypeName != nil ? _typeByName(mangledTypeName!) : nil)
        let typeName = typeName ?? (type != nil ? {
            let (pointer, length) = _getTypeName(type!, qualified: true)
            return String(decoding: UnsafeBufferPointer(start: pointer, count: length), as: UTF8.self)
        }() : nil)
        let mangledTypeName = mangledTypeName ?? (type != nil ? _mangledTypeName(type!) ?? {
            let (pointer, length) = _getMangledTypeName(type!)
            return String(decoding: UnsafeBufferPointer(start: pointer, count: length), as: UTF8.self)
        }() : nil)
        Self.register(
            type as! AnyClass,
            typeName: typeName ?? String(describing: type!),
            mangledTypeName: mangledTypeName!,
            metadata: metadata
        )
    }
    
    /// Sets `typeName` and/or `mangledTypeName` when key is known by any form.
    /// Returns `false` if no matching entry exists.
    @discardableResult nonisolated public static func setValue(
        typeName newTypeName: String? = nil,
        mangledTypeName newMangledTypeName: String? = nil,
        forKey key: Key
    ) -> Bool {
        guard let existing = {
            switch key {
            case .type(let value): getValue(forType: value)
            case .typeName(let value): getValue(forTypeName: value)
            case .mangledTypeName(let value): getValue(forMangledTypeName: value)
            }
        }() else {
            logger.warning("TypeRegistry found no match to set value: \(key)")
            return false
        }
        let finalTypeName = newTypeName ?? existing.typeName
        let finalMangledTypeName = newMangledTypeName ?? existing.mangledTypeName
        Self.register(existing.type, typeName: finalTypeName, mangledTypeName: finalMangledTypeName)
        return true
    }
    
    /// Removes a type entry by any key. No-op if missing.
    nonisolated public static func removeValue(forKey key: Key) {
        let snapshot = _getOrInitializeSnapshot()
        var index: Int?
        switch key {
        case .type(let value): index = snapshot.indexByType[ObjectIdentifier(value)]
        case .typeName(let value): index = snapshot.indexByTypeName[value]
        case .mangledTypeName(let value): index = snapshot.indexByMangledTypeName[value]
        }
        guard let index else { return }
        var nextEntries = snapshot.entries
        nextEntries.remove(at: index)
        _overwriteSnapshot(consume nextEntries)
    }
    
    /// Removes all entries (publishes an empty snapshot).
    nonisolated public static func removeAll() {
        _overwriteSnapshot([])
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
private func _getOrInitializeBox() -> _Box {
    let key = _makeKeyPointer()
    let host: AnyClass = _AssociationKeyToken.self
    if let box = objc_getAssociatedObject(host, key) as? _Box { return box }
    let newBox = _Box()
    objc_setAssociatedObject(host, key, newBox, .OBJC_ASSOCIATION_RETAIN)
    return (objc_getAssociatedObject(host, key) as? _Box) ?? newBox
}

private final class _Snapshot: Sendable {
    nonisolated fileprivate let entries: [TypeMetadata]
    nonisolated fileprivate let indexByType: [ObjectIdentifier: Int]
    nonisolated fileprivate let indexByTypeName: [String: Int]
    nonisolated fileprivate let indexByMangledTypeName: [String: Int]
    
    nonisolated fileprivate init(_ entries: [TypeMetadata]) {
        self.entries = entries
        let count = entries.count
        var byType = Dictionary<ObjectIdentifier, Int>(minimumCapacity: count)
        var byTypeName = Dictionary<String, Int>(minimumCapacity: count)
        var byMangledTypeName = Dictionary<String, Int>(minimumCapacity: count)
        for (index, entry) in entries.enumerated() {
            byType[ObjectIdentifier(entry.type)] = index
            byTypeName[entry.typeName] = index
            byMangledTypeName[entry.mangledTypeName] = index
        }
        self.indexByType = consume byType
        self.indexByTypeName = consume byTypeName
        self.indexByMangledTypeName = consume byMangledTypeName
    }
}

@inline(__always) nonisolated
private func _getOrInitializeSnapshot(make: () -> [TypeMetadata] = { [] })
-> _Snapshot {
    let box = _getOrInitializeBox()
    if let snapshot = box.reference.load() { return snapshot }
    return box.reference.storeIfNil(_Snapshot(make()))
}

@inline(__always) nonisolated
private func _overwriteSnapshot(_ entries: [TypeMetadata]) {
    let newSnapshot = _Snapshot(consume entries)
    _publishSnapshot(newSnapshot)
}

@inline(__always) nonisolated
private func _publishSnapshot(_ snapshot: _Snapshot) {
    let key = _makeKeyPointer()
    let newBox = _Box()
    _ = newBox.reference.storeIfNil(consume snapshot)
    let host: AnyClass = _AssociationKeyToken.self
    objc_setAssociatedObject(host, key, newBox, .OBJC_ASSOCIATION_RETAIN)
}
