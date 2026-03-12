//
//  SQLBuilder.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

@resultBuilder public enum SQLBuilder {
    public typealias Component = any SQLFragment
    
    nonisolated public static func buildBlock(_ fragments: Component...) -> [Component] {
        fragments
    }
    
    nonisolated public static func buildBlock(_ fragments: [Component]) -> [Component] {
        fragments
    }
    
    nonisolated public static func buildBlock(_ parts: [Component]...) -> [Component] {
        parts.flatMap { $0 }
    }
    
    nonisolated public static func buildArray(_ components: [[Component]]) -> [Component] {
        components.flatMap { $0 }
    }
    
    nonisolated public static func buildOptional(_ component: [Component]?) -> [Component] {
        component ?? []
    }
    
    nonisolated public static func buildEither(first component: [Component]) -> [Component] {
        component
    }
    
    nonisolated public static func buildEither(second component: [Component]) -> [Component] {
        component
    }
    
    nonisolated public static func buildExpression(_ expression: Component) -> [Component] {
        [expression]
    }
    
    nonisolated public static func buildExpression(_ expression: [Component]) -> [Component] {
        expression
    }
    
    nonisolated public static func buildExpression(_ string: String) -> [Component] {
        [Raw(string)]
    }
    
    nonisolated public static func buildLimitedAvailability(_ component: [Component]) -> [Component] {
        []
    }
    
    nonisolated public static func buildPartialBlock(first: [Component]) -> [Component] {
        first
    }
    
    nonisolated public static func buildPartialBlock(accumulated: [Component], next: [Component]) -> [Component] {
        accumulated + next
    }
    
    nonisolated public static func buildFinalResult(_ component: [Component]) -> [Component] {
        component
    }
}

public struct Raw: SQLFragment {
    nonisolated public var sql: String
    
    nonisolated public init(_ sql: String) {
        self.sql = sql
    }
}
