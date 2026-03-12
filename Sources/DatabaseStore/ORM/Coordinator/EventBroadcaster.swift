//
//  EventBroadcaster.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import AsyncAlgorithms
import Foundation
import Logging
import SwiftData
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.coordinator")

package struct Subscription: Sendable {
    nonisolated internal let id: UUID
    nonisolated internal let stream: AsyncThrowingStream<
        ContiguousArray<any DataStoreSnapshotValue>,
        any Swift.Error
    >
    nonisolated internal let cancel: @Sendable () -> Void
}

package final class EventBroadcaster: Sendable {
    internal typealias Value = ContiguousArray<any DataStoreSnapshotValue>
    nonisolated internal let subscribers: Mutex<[UUID: AsyncThrowingStream<Value, any Swift.Error>.Continuation]> = .init([:])
    
    nonisolated internal func subscribe() -> Subscription {
        let id = UUID()
        let stream = AsyncThrowingStream<Value, any Swift.Error> { continuation in
            subscribers.withLock { subscribers in
                subscribers[id] = continuation
                logger.trace("Added subscriber: \(id) (total: \(subscribers.count))")
            }
            continuation.onTermination = { @Sendable [weak self] _ in
                self?.removeSubscriber(for: id)
                logger.trace("Called from termination: \(id)")
            }
        }
        return Subscription(
            id: id,
            stream: stream,
            cancel: { [weak self] in self?.unsubscribe(for: id) }
        )
    }
    
    nonisolated internal func broadcast(_ value: Value) {
        for continuation in subscribers.withLock(\.values) {
            continuation.yield(value)
        }
    }
    
    nonisolated internal func broadcast(for id: UUID, value: Value) {
        if let continuation = subscribers.withLock({ $0[id] }) {
            logger.trace("Broadcasting snapshot to subscriber: \(id)")
            continuation.yield(value)
        }
    }
    
    nonisolated private func unsubscribe(for id: UUID) {
        let continuation = subscribers.withLock { $0.removeValue(forKey: id) }
        continuation?.finish(throwing: CancellationError())
        if continuation != nil {
            logger.trace("Subscriber cancelled: \(id) (remaining: \(subscribers.withLock(\.count)))")
        }
    }
    
    nonisolated private func removeSubscriber(for id: UUID) {
        subscribers.withLock {
            $0.removeValue(forKey: id)
            logger.trace("Subscriber removed: \(id) (remaining: \($0.count))")
        }
    }
}
