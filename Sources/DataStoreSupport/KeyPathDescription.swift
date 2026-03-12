//
//  KeyPathDescription.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

public struct KeyPathDescription: CustomStringConvertible, Sendable {
    nonisolated public let rootType: String
    nonisolated public let components: [Component]
    
    nonisolated public var description: String {
        var result = "KeyPathDescription(\nrootType: \(rootType))\n"
        for (index, component) in components.enumerated() {
            result += "  [\(index)]: \(component)\n"
        }
        result += ")"
        return result
    }
    
    nonisolated public static func parse(keyPath: AnyKeyPath) -> KeyPathDescription? {
        let trimmed = String(describing: keyPath).trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.hasPrefix("\\") else {
            return nil
        }
        let body = trimmed.dropFirst()
        guard let dotIndex = body.firstIndex(of: ".") else {
            return nil
        }
        let rootType = String(body[..<dotIndex])
        let remainder = body[body.index(after: dotIndex)...]
        let rawSegments = remainder.split(separator: ".", omittingEmptySubsequences: false)
        let components: [Component] = rawSegments.compactMap { segmentSubstring in
            var segment = segmentSubstring.trimmingCharacters(in: .whitespaces)
            if segment.hasPrefix("<computed ") {
                while segment.hasSuffix("?") || segment.hasSuffix(">") {
                    segment = String(segment.dropLast())
                }
                let inside = segment.dropFirst("<computed ".count)
                guard let parenthesisIndex = inside.lastIndex(of: "("),
                      inside.hasSuffix(")") else {
                    return nil
                }
                let pointer = inside[..<parenthesisIndex]
                let typeSubstring = inside[
                    inside.index(after: parenthesisIndex)
                    ..< inside.index(before: inside.endIndex)
                ]
                let typeName = String(typeSubstring)
                let (base, isOptional, isArray) = analyzeTypeName(typeName)
                return .init(
                    property: nil,
                    computedPointer: String(pointer),
                    computedType: typeName,
                    unwrappedType: base,
                    isOptional: isOptional,
                    isArray: isArray
                )
            }
            guard !segment.isEmpty else {
                return nil
            }
            if segment.hasSuffix("?") { segment = String(segment.dropLast()) }
            return .init(
                property: segment,
                computedPointer: nil,
                computedType: nil,
                unwrappedType: nil,
                isOptional: false,
                isArray: false
            )
        }
        return .init(rootType: rootType, components: components)
    }
    
    nonisolated private static func analyzeTypeName(_ typeName: String) -> (
        unwrappedTypeName: String,
        isOptional: Bool,
        isArray: Bool
    ) {
        var base = typeName.trimmingCharacters(in: .whitespaces)
        var isOptional = false
        var isArray = false
        if base.hasPrefix("Optional<"), base.hasSuffix(">") {
            base = String(base.dropFirst("Optional<".count).dropLast())
            isOptional = true
        }
        if base.hasPrefix("Array<"), base.hasSuffix(">") {
            base = String(base.dropFirst("Array<".count).dropLast())
            isArray = true
        }
        return (base, isOptional, isArray)
    }
    
    public struct Component: CustomStringConvertible, Sendable {
        nonisolated public let property: String?
        nonisolated public let computedPointer: String?
        nonisolated public let computedType: String?
        nonisolated public let unwrappedType: String?
        nonisolated public let isOptional: Bool
        nonisolated public let isArray: Bool
        
        nonisolated public var isComputed: Bool {
            computedPointer != nil
        }
        
        nonisolated public var description: String {
            """
            KeyPathComponent(
                property: \(property ?? "N/A"),
                computedPointer: \(computedPointer ?? "N/A"),
                computedType: \(computedType ?? "N/A"),
                unwrappedType: \(unwrappedType ?? "N/A"),
                isOptional: \(isOptional),
                isArray: \(isArray)
            )
            """
        }
    }
}
