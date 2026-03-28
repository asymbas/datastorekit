//
//  UnwrapType.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

#if !SwiftPlaygrounds

private protocol _OptionalMetatype {
    nonisolated static var wrapped: Any.Type { get }
}

extension Optional: _OptionalMetatype {
    nonisolated fileprivate static var wrapped: Any.Type { Wrapped.self }
}

nonisolated package func unwrapOptionalMetatype(_ type: Any.Type) -> Any.Type {
    (type as? _OptionalMetatype.Type)?.wrapped ?? type
}

private protocol _ArrayMetatype {
    nonisolated static var element: Any.Type { get }
}

extension Array: _ArrayMetatype {
    nonisolated fileprivate static var element: Any.Type { Element.self }
}

nonisolated package func unwrapArrayMetatype(_ type: Any.Type) -> Any.Type {
    (type as? _ArrayMetatype.Type)?.element ?? type
}

#else

private protocol _OptionalMetatype {
    nonisolated static var wrapped: Any.Type { get }
}

extension Optional: _OptionalMetatype {
    nonisolated fileprivate static var wrapped: Any.Type { Wrapped.self }
}

nonisolated public func unwrapOptionalMetatype(_ type: Any.Type) -> Any.Type {
    (type as? _OptionalMetatype.Type)?.wrapped ?? type
}

private protocol _ArrayMetatype {
    nonisolated static var element: Any.Type { get }
}

extension Array: _ArrayMetatype {
    nonisolated fileprivate static var element: Any.Type { Element.self }
}

nonisolated public func unwrapArrayMetatype(_ type: Any.Type) -> Any.Type {
    (type as? _ArrayMetatype.Type)?.element ?? type
}

#endif
