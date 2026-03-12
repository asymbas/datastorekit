//
//  SQLTableBuilder.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

@resultBuilder public enum SQLTableBuilder {
    public typealias Expression = any TableDefinition
    public typealias Component = [Expression]
    
    nonisolated public static func buildExpression(_ expression: Expression) -> Component {
        [expression]
    }
    
    nonisolated public static func buildExpression(_ expression: Expression?) -> Component {
        expression == nil ? [] : [expression!]
    }
    
    nonisolated public static func buildExpression(_ expression: Component) -> Component {
        expression
    }
    
    nonisolated public static func buildBlock(_ component: Component...) -> Component {
        component.flatMap(\.self)
    }
    
    nonisolated public static func buildOptional(_ component: Component?) -> Component {
        component ?? []
    }
    
    nonisolated public static func buildArray(_ component: [Component]) -> Component {
        component.flatMap(\.self)
    }
    
    nonisolated public static func buildEither(first component: Component) -> Component {
        component
    }
    
    nonisolated public static func buildEither(second component: Component) -> Component {
        component
    }
}
