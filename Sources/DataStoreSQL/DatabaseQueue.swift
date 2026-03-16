//
//  DatabaseQueue.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreCore
import Dispatch
import Logging
import Synchronization

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.sql")

public final class DatabaseQueue<Store>: Sendable where Store: DatabaseProtocol {
    nonisolated private final let _lastUpdated: Atomic<UInt64>
    nonisolated private final let lifecycle: Atomic<Lifecycle> = .init(.open)
    nonisolated private final let closeLock: Mutex<Void> = .init(())
    nonisolated private final let timeout: DispatchTimeInterval
    nonisolated private final let writerPool: ConnectionPool
    nonisolated private final let readerPool: ConnectionPool?
    nonisolated internal final let attachment: Store.Attachment?
    
    nonisolated package final let makeTransaction:
    @Sendable (any EditingStateProviding, Store.Handle) -> Store.Transaction?
    
    nonisolated internal final let onTransactionFailure:
    @Sendable (borrowing DatabaseConnection<Store>) -> Void
    
    /// The most recent time the queue was marked as updated.
    nonisolated public final var lastUpdated: DispatchTime {
        get { .init(uptimeNanoseconds: _lastUpdated.load(ordering: .relaxed)) }
        set { _lastUpdated.store(newValue.uptimeNanoseconds, ordering: .relaxed) }
    }
    
    /// The number of currently available connections in the queue.
    nonisolated public final var count: Int {
        let readersAvailable = self.readerPool?.availableCount ?? 0
        let writersAvailable = self.writerPool.availableCount
        return readersAvailable + writersAvailable
    }
    
    nonisolated package init(
        writers: Int,
        readers: Int,
        attachment: Store.Attachment?,
        makeTransaction:
        @escaping @Sendable (any EditingStateProviding, Store.Handle) -> Store.Transaction?,
        onTransactionFailure:
        @escaping @Sendable (borrowing DatabaseConnection<Store>) -> Void,
        makeWriterConnection: (Int) throws -> Store.Handle,
        makeReaderConnection: (Int) throws -> Store.Handle
    ) throws {
        _lastUpdated = .init(DispatchTime.now().uptimeNanoseconds)
        self.attachment = attachment
        self.makeTransaction = makeTransaction
        self.timeout = .seconds(60)
        precondition(writers >= 1, "There must be at least one writer.")
        precondition(readers >= 0, "Number of readers cannot be negative.")
        self.writerPool = ConnectionPool(role: .writer, capacity: writers)
        self.readerPool = readers > 0 ? ConnectionPool(role: .reader, capacity: readers) : nil
        var created: [Store.Handle] = []
        created.reserveCapacity(writers + readers)
        do {
            for index in 0..<writers {
                let writer = try makeWriterConnection(index)
                created.append(writer)
                writerPool.addInitial(writer)
                logger.trace("Initialized writer connection #\(index): \(writer.id)")
            }
            if let readerPool {
                for index in 0..<readers {
                    let reader = try makeReaderConnection(index)
                    created.append(reader)
                    readerPool.addInitial(reader)
                    logger.trace("Initialized reader connection #\(index): \(reader.id)")
                }
            }
        } catch {
            for handle in created {
                _ = try? handle.close()
            }
            throw error
        }
        self.onTransactionFailure = onTransactionFailure
        logger.info(
            "DatabaseQueue<\(Store.Handle.self)>",
            metadata: ["writers": "\(writers)", "readers": "\(readers)"]
        )
    }
    
    /// Updates the queue's last updated time.
    ///
    /// - Parameter threshold:
    ///   A threshold time that the current value must be older than before it is updated.
    nonisolated internal final func update(ifOlderThan threshold: DispatchTime? = nil) {
        let newValue = DispatchTime.now().uptimeNanoseconds
        if let threshold {
            if _lastUpdated.load(ordering: .relaxed) < threshold.uptimeNanoseconds {
                _lastUpdated.store(newValue, ordering: .relaxed)
            }
        } else {
            _lastUpdated.store(newValue, ordering: .relaxed)
        }
    }
    
    /// Closes the queue and shuts down all connection pools.
    nonisolated public final func close() throws {
        try closeLock.withLock { _ in
            let state = lifecycle.load(ordering: .sequentiallyConsistent)
            if state == .closed { return }
            lifecycle.store(.closing, ordering: .sequentiallyConsistent)
            logger.debug("Database<\(Store.Handle.self)>: closing")
            for pool in [writerPool, readerPool].compactMap({ $0 }) {
                try pool.shutdown(timeout: timeout)
            }
            lifecycle.store(.closed, ordering: .sequentiallyConsistent)
        }
    }
    
    internal enum Lifecycle: UInt8, AtomicRepresentable {
        case open = 0
        case closing
        case closed
    }
    
    public enum Error: Equatable, Swift.Error {
        case isClosing
        case isClosed
        case acquireTimeout
        case connectionPoolIsClosed
        case invalidHandleRole
    }
}

extension DatabaseQueue {
    /// Acquires a database connection for the requested role.
    ///
    /// - Parameters:
    ///   - role:
    ///     The preferred connection role. The default value is `nil`.
    ///     When a role is not specified, the queue prefers a reader connection when one is available.
    ///   - editingState:
    ///     The identifier associated objects use to locate a managed context and related state.
    /// - Returns:
    ///   A database connection.
    nonisolated public final func connection(
        _ role: DataStoreRole? = nil,
        for editingState: (any EditingStateProviding)? = nil
    ) throws -> DatabaseConnection<Store> {
        switch lifecycle.load(ordering: .sequentiallyConsistent) {
        case .open: break
        case .closing: throw DatabaseQueue.Error.isClosing
        default: throw DatabaseQueue.Error.isClosed
        }
        let context: Store.Context? = {
            switch editingState {
            case let value?: self.attachment?.makeObjectContext(editingState: value)
            case nil: Optional<Store.Context>.none
            }
        }()
        let handle: Store.Handle
        switch role {
        case .some(.writer):
            handle = try self.writerPool.acquire(timeout: timeout)
        case .some(.reader), .none:
            if let readerPool = self.readerPool {
                handle = try readerPool.acquire(timeout: timeout)
            } else {
                handle = try self.writerPool.acquire(timeout: timeout)
            }
        }
        logger.debug("DatabaseConnection acquired: \(handle.id)")
        return .init(
            for: editingState,
            queue: self,
            handle: handle,
            context: context,
            transaction: nil
        )
    }
    
    /// Requests a connection for a specific data store role.
    ///
    /// - Parameters:
    ///   - role:
    ///     The required connection role.
    ///   - editingState:
    ///     The identifier associated objects use to locate a managed context and related state.
    /// - Returns:
    ///   A database connection.
    nonisolated public final func request(
        _ role: DataStoreRole,
        for editingState: (any EditingStateProviding)? = nil
    ) throws -> DatabaseConnection<Store> {
        try self.connection(role, for: editingState)
    }
    
    /// Releases a previously acquired database connection back to the queue.
    ///
    /// - Parameter connection: The connection to release.
    nonisolated public final func release(_ connection: consuming DatabaseConnection<Store>) {
        let transaction = connection.transaction.take()
        self.release(connection.release(), transaction: transaction)
    }
    
    nonisolated internal final func release(
        _ handle: Store.Handle,
        transaction: (any DatabaseTransaction)? = nil
    ) {
        if let transaction, transaction.hasChanges {
            self.lastUpdated = .now()
        }
        switch handle.role {
        case .some(.writer):
            writerPool.release(handle)
        case .some(.reader):
            guard let readerPool = self.readerPool else {
                writerPool.release(handle)
                return
            }
            readerPool.release(handle)
        case .none:
            _ = try? handle.close()
            return
        }
        logger.debug(
            "DatabaseConnection released: \(handle.id)",
            metadata: ["role": "\(handle.role, default: "nil")", "available": "\(count)"]
        )
    }
}

extension DatabaseQueue {
    nonisolated public final func _withConnection<Result>(
        _ role: DataStoreRole? = nil,
        for editingState: (any EditingStateProviding)? = nil,
        _ operation: (borrowing DatabaseConnection<Store>) throws -> Result
    ) throws -> Result {
        let connection = try connection(role, for: editingState)
        do {
            let result = try operation(connection)
            release(consume connection)
            return result
        } catch {
            release(consume connection)
            throw error
        }
    }
    
    /// Performs an operation using a managed connection that is automatically released afterward.
    ///
    /// - Parameters:
    ///   - role:
    ///     The preferred connection role. The default value is `nil`.
    ///     When a role is not specified, the queue prefers a reader connection when one is available.
    ///   - editingState:
    ///     The identifier associated objects use to locate a managed context and related state.
    ///   - operation:
    ///     The operation to perform with the connection.
    /// - Returns:
    ///   The result of the operation.
    nonisolated public final func withConnection<Result>(
        _ role: DataStoreRole? = nil,
        for editingState: (any EditingStateProviding)? = nil,
        _ operation: (inout sending DatabaseConnection<Store>) throws -> sending Result
    ) throws -> Result {
        var connection = try connection(role, for: editingState)
        do {
            let result = try operation(&connection)
            release(consume connection)
            return result
        } catch {
            release(consume connection)
            throw error
        }
    }
    
    /// Performs an operation using a reader connection.
    ///
    /// - Parameters:
    ///   - editingState:
    ///     The identifier associated objects use to locate a managed context and related state.
    ///   - operation:
    ///     The operation to perform with the connection.
    /// - Returns:
    ///   The result of the operation.
    nonisolated public final func reader<Result>(
        for editingState: (any EditingStateProviding)? = nil,
        _ operation: (inout sending DatabaseConnection<Store>) throws -> sending Result
    ) throws -> Result {
        try withConnection(readerPool == nil ? .writer : .reader, for: editingState, operation)
    }
    
    /// Performs an operation using a writer connection.
    ///
    /// - Parameters:
    ///   - editingState:
    ///     The identifier associated objects use to locate a managed context and related state.
    ///   - operation:
    ///     The operation to perform with the connection.
    /// - Returns:
    ///   The result of the operation.
    nonisolated public final func writer<Result>(
        for editingState: (any EditingStateProviding)? = nil,
        _ operation: (inout sending DatabaseConnection<Store>) throws -> sending Result
    ) throws -> Result {
        try withConnection(.writer, for: editingState, operation)
    }
}

extension DatabaseQueue {
    package final class ConnectionPool: Sendable {
        nonisolated private let role: DataStoreRole
        nonisolated private let capacity: Int
        nonisolated private let semaphore: DispatchSemaphore
        nonisolated private let connections: Mutex<[Store.Handle]>
        nonisolated private let isClosed: Atomic<Bool> = .init(false)
        
        nonisolated fileprivate init(role: DataStoreRole, capacity: Int) {
            self.role = role
            self.capacity = capacity
            self.semaphore = DispatchSemaphore(value: 0)
            self.connections = .init([])
            logger.trace("Created ConnectionPool for \(role) connections: \(capacity)")
        }
        
        nonisolated fileprivate var availableCount: Int {
            connections.withLock(\.count)
        }
        
        nonisolated fileprivate func addInitial(_ handle: Store.Handle) {
            precondition(handle.role == role, "Handle role mismatch for pool.")
            connections.withLock { $0.append(handle) }
            semaphore.signal()
        }
        
        nonisolated fileprivate func acquire(timeout: DispatchTimeInterval) throws -> Store.Handle {
            if isClosed.load(ordering: .sequentiallyConsistent) {
                throw DatabaseQueue.Error.connectionPoolIsClosed
            }
            let deadline = DispatchTime.now() + timeout
            guard semaphore.wait(timeout: deadline) == .success else {
                throw DatabaseQueue.Error.acquireTimeout
            }
            if isClosed.load(ordering: .sequentiallyConsistent) {
                semaphore.signal()
                throw DatabaseQueue.Error.connectionPoolIsClosed
            }
            return connections.withLock { connections in
                if let index = connections.firstIndex(where: { $0.role == role }) {
                    return connections.remove(at: index)
                } else {
                    fatalError()
                }
            }
        }
        
        nonisolated fileprivate func release(_ handle: Store.Handle) {
            guard handle.role == role else {
                _ = try? handle.close()
                return
            }
            if isClosed.load(ordering: .sequentiallyConsistent) {
                _ = try? handle.close()
                semaphore.signal()
                return
            }
            let inserted: Bool = self.connections.withLock { connections in
                if connections.contains(where: { $0.id == handle.id }) {
                    return false
                }
                connections.append(handle)
                return true
            }
            if inserted {
                semaphore.signal()
            } else {
                _ = try? handle.close()
                logger.debug("Handle was already released: \(handle.id)")
            }
        }
        
        nonisolated fileprivate func shutdown(timeout: DispatchTimeInterval) throws {
            if isClosed.load(ordering: .sequentiallyConsistent) {
                return
            }
            let deadline = DispatchTime.now() + timeout
            var acquired = 0
            for _ in 0..<capacity {
                guard semaphore.wait(timeout: deadline) == .success else {
                    for _ in 0..<acquired { semaphore.signal() }
                    throw SQLError(.unknown)
                }
                acquired += 1
            }
            isClosed.store(true, ordering: .sequentiallyConsistent)
            let drained = self.connections.withLock { connections in
                let drained = connections
                connections.removeAll(keepingCapacity: false)
                return drained
            }
            for handle in drained {
                try handle.close()
                logger.debug("Released handle: \(handle.id)")
            }
        }
    }
}
