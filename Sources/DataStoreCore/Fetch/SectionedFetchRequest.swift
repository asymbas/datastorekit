//
//  SectionedFetchRequest.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation
import SwiftData

public struct SectionedFetchDescriptor<T, SectionID>: Sendable
where T: PersistentModel, SectionID: Hashable & Sendable {
    nonisolated public var sectionKeyPath: KeyPath<T, SectionID> & Sendable
    nonisolated public var sectionSortBy: SortOrder
    nonisolated public var descriptor: FetchDescriptor<T>
    nonisolated public var limitPerSection: Int?
    nonisolated public var includesEmptySections: Bool
    
    nonisolated public init(
        _ descriptor: FetchDescriptor<T> = .init(),
        sectionedBy sectionKeyPath: KeyPath<T, SectionID> & Sendable,
        sectionSortBy: SortOrder = .forward,
        limitPerSection: Int? = nil,
        includesEmptySections: Bool = false
    ) {
        self.descriptor = descriptor
        self.sectionKeyPath = sectionKeyPath
        self.sectionSortBy = sectionSortBy
        self.limitPerSection = limitPerSection
        self.includesEmptySections = includesEmptySections
    }
}

public struct SectionedFetchResults<SectionID, Element>
where SectionID: Hashable & Sendable, Element: PersistentModel {
    nonisolated public var sections: [Section]
    
    nonisolated package init(sections: [Section]) {
        self.sections = sections
    }
    
    public struct Section {
        nonisolated public var id: SectionID
        nonisolated public var count: Int
        nonisolated public var elements: [Element]
        
        nonisolated package init(id: SectionID, count: Int, elements: [Element]) {
            self.id = id
            self.count = count
            self.elements = elements
        }
    }
}
