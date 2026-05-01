//
//  DatabaseUtilities.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Logging
package import Foundation

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit")

nonisolated package func measure<T>(
    _ function: StaticString = #function,
    _ label: String = "",
    _ block: () throws -> T
) rethrows -> T {
    let startTime = DispatchTime.now()
    defer {
        let endTime = DispatchTime.now()
        let nanoseconds = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let milliseconds = Double(nanoseconds) / 1_000_000
        let measured = String(format: "%.3f", milliseconds)
        if label.isEmpty {
            logger.info("\(function)\nMeasured \(measured)ms")
        } else {
            logger.info("\(label) - \(function)\nMeasured \(measured)ms")
        }
    }
    return try block()
}

nonisolated(nonsending) package func measureAsync<T>(
    _ function: StaticString = #function,
    _ label: String = "",
    _ block: () async throws -> T
) async rethrows -> T {
    let startTime = DispatchTime.now()
    defer {
        let endTime = DispatchTime.now()
        let nanoseconds = endTime.uptimeNanoseconds - startTime.uptimeNanoseconds
        let milliseconds = Double(nanoseconds) / 1_000_000
        let measured = String(format: "%.3f", milliseconds)
        if label.isEmpty {
            logger.info("\(function)\nMeasured \(measured)ms (async)")
        } else {
            logger.info("\(label) - \(function)\nMeasured \(measured)ms (async)")
        }
    }
    return try await block()
}

nonisolated package func delay(by timeInterval: TimeInterval = 0.1) {
    #if RELEASE
    #error("This should not be called in release builds.")
    #endif
    Thread.sleep(forTimeInterval: timeInterval)
    fflush(stdout)
}

nonisolated package var threadDescription: String {
    let isMainThread = "Main Thread? \(Thread.isMainThread)"
    let isMultiThreaded = "Multiple Threads? \(Thread.isMultiThreaded())"
    let currentThread = "Current Thread: \(Thread.current)"
    let operationQueue = "Operation Queue: \(String(describing: OperationQueue.current))"
    return "\(isMainThread), \(isMultiThreaded), \(currentThread), \(operationQueue)"
}

nonisolated package func printMirrorDetails(
    of value: Any,
    indentLevel: Int = 0,
    label: String? = nil
) {
    let indent = String(repeating: " ", count: 4 * indentLevel)
    let mirror = Mirror(reflecting: value)
    let typeName = String(describing: mirror.subjectType)
    if let label {
        print("\(indent)\(label): \(typeName) = \(value)")
    } else {
        print("\(indent)\(typeName) = \(value)")
    }
    for child in mirror.children {
        if let label = child.label {
            printMirrorDetails(of: child.value, indentLevel: indentLevel + 1, label: label)
        } else {
            printMirrorDetails(of: child.value, indentLevel: indentLevel + 1)
        }
    }
    if let superclassMirror = mirror.superclassMirror {
        print("\(indent) superclass")
        printMirrorDetails(of: superclassMirror.subjectType, indentLevel: indentLevel + 1)
    }
}

nonisolated package func liftKeyPath<Super, Sub, Value>(
    _ keyPath: KeyPath<Super, Value>,
    to _: Sub.Type
) -> KeyPath<Sub, Value>? {
    guard Sub.self is Super.Type else { return nil }
    #if false
    return unsafeBitCast(keyPath, to: KeyPath<Sub, Value>.self)
    #else
    let raw = Unmanaged.passUnretained(keyPath as AnyObject).toOpaque()
    let test = Unmanaged<KeyPath<Sub, Value>>.fromOpaque(raw).takeUnretainedValue()
    return test
    #endif
}

nonisolated package func liftKeyPath<Super, Sub, Value>(
    _ keyPath: KeyPath<Super, Value?>,
    to _: Sub.Type
) -> KeyPath<Sub, Value?>? {
    guard Sub.self is Super.Type else { return nil }
    #if false
    return unsafeBitCast(keyPath, to: KeyPath<Sub, Value?>.self)
    #else
    let raw = Unmanaged.passUnretained(keyPath as AnyObject).toOpaque()
    let test = Unmanaged<KeyPath<Sub, Value?>>.fromOpaque(raw).takeUnretainedValue()
    return test
    #endif
}

nonisolated package func subclasses<T>(of root: T.Type) -> [T.Type] {
    var count: UInt32 = 0
    guard let classes = objc_copyClassList(&count) else { return [] }
    defer { free(UnsafeMutableRawPointer(classes)) }
    var result: [T.Type] = []
    for index in 0..<Int(count) {
        let candidate: AnyClass = classes[index]
        var superclass: AnyClass? = candidate
        while let type = superclass {
            if type == root as? AnyClass {
                if let typed = candidate as? T.Type {
                    result.append(typed)
                }
                break
            }
            superclass = class_getSuperclass(type)
        }
    }
    return result
}

nonisolated package func isEqual<T: Equatable>(lhs: Any, rhs: Any, as type: T.Type) -> Bool {
    guard let lhs = lhs as? T, let rhs = rhs as? T else { return false }
    return lhs == rhs
}

nonisolated package func isNil(_ value: Any) -> Bool {
    func unwrap<T>() -> T { return value as! T }
    let unwrappedValue: Any? = unwrap()
    return unwrappedValue == nil
}

nonisolated package func sendable<T: Sendable>(cast value: Any) -> T? {
    value as? T
}

nonisolated package func quote(_ identifier: String) -> String {
    "\"\(identifier.replacingOccurrences(of: "\"", with: "\"\""))\""
}

nonisolated package func combineFlags<T>(_ flags: some Sequence<T>) -> T.RawValue
where T: RawRepresentable, T.RawValue: FixedWidthInteger {
    flags.reduce(0) { $0 | $1.rawValue }
}
