//
//  Compatibility.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

#if swift(>=6.2)
public typealias SendableMetatype = Swift.SendableMetatype
#else
public typealias SendableMetatype = Any
#endif

#if swift(<6.2)
extension String.StringInterpolation {
    nonisolated public mutating func appendInterpolation<T>(
        _ value: T?,
        `default` defaultValue: @autoclosure () -> String
    ) {
        appendLiteral(value.map { String(describing: $0) } ?? defaultValue())
    }
}
#endif
