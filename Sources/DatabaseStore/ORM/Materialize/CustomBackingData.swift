//
//  CustomBackingData.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreSupport
import SwiftData

public final class CustomBackingData<M: PersistentModel>: BackingData {
    /// Inherited from `BackingData.Model`.
    public typealias Model = M
    /// Inherited from `BackingData.persistentModelID`.
    public var persistentModelID: PersistentIdentifier?
    /// Inherited from `BackingData.metadata`.
    public var metadata: Any
    internal var modelContext: ModelContext?
    
    /// Inherited from `BackingData.init(for:).`
    public required init(for modelType: M.Type) {
        self.persistentModelID = nil
        self.metadata = ()
    }
    
    public init(persistentModelID: PersistentIdentifier? = nil, metadata: Any = ()) {
        self.persistentModelID = persistentModelID
        self.metadata = metadata
    }
    
    package init(snapshot: DatabaseSnapshot) {
        self.persistentModelID = snapshot.persistentIdentifier
        self.metadata = snapshot
    }
    
    private var snapshot: DatabaseSnapshot? {
        get { metadata as? DatabaseSnapshot }
        set { self.metadata = newValue ?? () }
    }
    
    /// Inherited from `BackingData.getValue(forKey:)`.
    @_disfavoredOverload
    public func getValue<Value>(forKey keyPath: KeyPath<M, Value>) -> Value
    where Value: Decodable {
        guard let keyPath: KeyPath<M, Value> & Sendable = sendable(cast: keyPath) else {
            fatalError()
        }
        return snapshot.unsafelyUnwrapped.getValue(keyPath: keyPath) as! Value
    }
    
    /// Inherited from `BackingData.getValue(forKey:)`.
    public func getValue<Value>(forKey keyPath: KeyPath<M, Value>) -> Value
    where Value: PersistentModel {
        guard let keyPath: KeyPath<M, Value> & Sendable = sendable(cast: keyPath) else {
            fatalError()
        }
        return snapshot.unsafelyUnwrapped.getValue(keyPath: keyPath) as! Value
    }
    
    /// Inherited from `BackingData.getValue(forKey:)`.
    public func getValue<Value>(forKey keyPath: KeyPath<M, Value?>) -> Value?
    where Value: PersistentModel {
        guard let keyPath: KeyPath<M, Value?> & Sendable = sendable(cast: keyPath) else {
            fatalError()
        }
        return snapshot.unsafelyUnwrapped.getValue(keyPath: keyPath) as? Value
    }
    
    /// Inherited from `BackingData.getValue(forKey:)`.
    public func getValue<Value, OtherModel>(forKey keyPath: KeyPath<M, Value>) -> Value
    where Value: RelationshipCollection, OtherModel == Value.PersistentElement {
        guard let keyPath: KeyPath<Value, OtherModel> & Sendable = sendable(cast: keyPath) else {
            fatalError()
        }
        return snapshot.unsafelyUnwrapped.getValue(keyPath: keyPath) as! Value
    }
    
    /// Inherited from `BackingData.getValue(forKey:)`.
    public func getValue<Value, OtherModel>(forKey keyPath: KeyPath<M, Value>) -> Value
    where Value: Decodable, Value: RelationshipCollection, OtherModel == Value.PersistentElement {
        guard let keyPath: KeyPath<Value, OtherModel> & Sendable = sendable(cast: keyPath) else {
            fatalError()
        }
        return snapshot.unsafelyUnwrapped.getValue(keyPath: keyPath) as! Value
    }
    
    /// Inherited from `BackingData.getTransformableValue(forKey:)`.
    public func getTransformableValue<Value>(forKey keyPath: KeyPath<M, Value>) -> Value {
        fatalError()
    }
    
    /// Inherited from `BackingData.setValue(forKey:to:)`.
    @_disfavoredOverload
    public func setValue<Value>(forKey keyPath: KeyPath<M, Value>, to newValue: Value)
    where Value: Encodable {
        guard let keyPath: KeyPath<M, Value> & Sendable = sendable(cast: keyPath) else {
            fatalError()
        }
        guard let newValue: any DataStoreSnapshotValue = sendable(cast: newValue) else {
            fatalError()
        }
        snapshot?.setValue(newValue, keyPath: keyPath)
    }
    
    /// Inherited from `BackingData.setValue(forKey:to:)`.
    public func setValue<Value>(forKey keyPath: KeyPath<M, Value>, to newValue: Value)
    where Value: PersistentModel {
        guard let keyPath: KeyPath<M, Value> & Sendable = sendable(cast: keyPath) else {
            fatalError()
        }
        guard let newValue: any DataStoreSnapshotValue = sendable(cast: newValue) else {
            fatalError()
        }
        snapshot?.setValue(newValue, keyPath: keyPath)
    }
    
    /// Inherited from `BackingData.setValue(forKey:to:)`.
    public func setValue<Value>(forKey keyPath: KeyPath<M, Value?>, to newValue: Value?)
    where Value: PersistentModel {
        guard let keyPath: KeyPath<M, Value> & Sendable = sendable(cast: keyPath) else {
            fatalError()
        }
        guard let newValue: any DataStoreSnapshotValue = sendable(cast: newValue as Any) else {
            fatalError()
        }
        snapshot?.setValue(newValue, keyPath: keyPath)
    }
    
    /// Inherited from `BackingData.setValue(forKey:to:)`.
    public func setValue<Value, OtherModel>(forKey keyPath: KeyPath<M, Value>, to newValue: Value)
    where Value: RelationshipCollection, OtherModel == Value.PersistentElement {
        guard let keyPath: KeyPath<M, Value> & Sendable = sendable(cast: keyPath) else {
            fatalError()
        }
        guard let newValue: any DataStoreSnapshotValue = sendable(cast: newValue) else {
            fatalError()
        }
        snapshot?.setValue(newValue, keyPath: keyPath)
    }
    
    /// Inherited from `BackingData.setValue(forKey:to:)`.
    public func setValue<Value, OtherModel>(forKey keyPath: KeyPath<M, Value>, to newValue: Value)
    where Value: Encodable, Value: RelationshipCollection, OtherModel == Value.PersistentElement {
        guard let keyPath: KeyPath<M, Value> & Sendable = sendable(cast: keyPath) else {
            fatalError()
        }
        guard let newValue: any DataStoreSnapshotValue = sendable(cast: newValue) else {
            fatalError()
        }
        snapshot?.setValue(newValue, keyPath: keyPath)
    }
    
    /// Inherited from `BackingData.setTransformableValue(forKey:to:)`.
    public func setTransformableValue<Value>(forKey keyPath: KeyPath<M, Value>, to newValue: Value) {
        fatalError()
    }
}
