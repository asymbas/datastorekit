//
//  SQLTableBuilder.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

@resultBuilder package enum SQLTableBuilder {
    package typealias Expression = any TableDefinition
    package typealias Component = [Expression]
    
    nonisolated package static func buildExpression(_ expression: Expression) -> Component {
        [expression]
    }
    
    nonisolated package static func buildExpression(_ expression: Expression?) -> Component {
        expression == nil ? [] : [expression!]
    }
    
    nonisolated package static func buildExpression(_ expression: Component) -> Component {
        expression
    }
    
    nonisolated package static func buildBlock(_ component: Component...) -> Component {
        component.flatMap(\.self)
    }
    
    nonisolated package static func buildOptional(_ component: Component?) -> Component {
        component ?? []
    }
    
    nonisolated package static func buildArray(_ component: [Component]) -> Component {
        component.flatMap(\.self)
    }
    
    nonisolated package static func buildEither(first component: Component) -> Component {
        component
    }
    
    nonisolated package static func buildEither(second component: Component) -> Component {
        component
    }
}
