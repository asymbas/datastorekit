//
//  SQLUtilities.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Foundation

nonisolated private func extractTableName(from sql: String) -> String? {
    let normalized = sql
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .replacingOccurrences(of: "\n", with: " ")
    let pattern = #"from\s+(?:`|"|')?([a-zA-Z0-9_]+)(?:`|"|')?"#
    let regex = try? NSRegularExpression(pattern: pattern, options: .caseInsensitive)
    let range = NSRange(normalized.startIndex..<normalized.endIndex, in: normalized)
    if let match = regex?.firstMatch(in: normalized, options: [], range: range),
       let nameRange = Range(match.range(at: 1), in: normalized) {
        return String(normalized[nameRange])
    }
    return nil
}

nonisolated public func renderTables(
    sql: String,
    rows: [[String: any Sendable]],
    chunkSize: Int = 5,
    primaryKey: String? = "pk"
) -> String {
    guard sql.trimmingCharacters(in: .whitespacesAndNewlines).lowercased().hasPrefix("select") else {
        return "Not a SELECT statement."
    }
    guard let first = rows.first else {
        return "No rows returned from table: \(extractTableName(from: sql) ?? "Unknown")"
    }
    var output = ""
    var columns = Array(first.keys)
    if let primaryKey, columns.contains(primaryKey) {
        columns.removeAll(where: { $0 == primaryKey })
        columns.sort()
        columns.insert(primaryKey, at: 0)
    } else {
        columns.sort()
    }
    var columnWidths = columns.map(\.count)
    for row in rows {
        for (index, key) in columns.enumerated() {
            let stringValue = row[key].map { String(describing: $0) } ?? "NULL"
            columnWidths[index] = max(columnWidths[index], stringValue.count)
        }
    }
    let chunkRanges: [Range<Int>] = (chunkSize <= 0)
    ? [0..<columns.count]
    : Array(stride(from: 0, to: columns.count, by: chunkSize)).map {
        $0..<min($0 + chunkSize, columns.count)
    }
    for chunkRange in chunkRanges {
        let chunkColumns = Array(columns[chunkRange])
        let chunkWidths = Array(columnWidths[chunkRange])
        let totalWidths = [6] + chunkWidths
        func padded(_ value: String, to width: Int) -> String {
            " " + value + String(repeating: " ", count: width - value.count + 1)
        }
        func separator(_ widths: [Int]) -> String {
            "+" + widths
                .map { String(repeating: "-", count: $0 + 2) }
                .joined(separator: "+") + "+"
        }
        let tableName = extractTableName(from: sql) ?? "Unknown"
        let rangeDescription = "\(chunkRange.lowerBound + 1)-\(chunkRange.upperBound)"
        let headerCells = zip(chunkColumns, chunkWidths)
            .map { padded($0, to: $1) }
            .joined(separator: "|") + "|"
        output += "* Columns \(rangeDescription) of \(columns.count) from table: \(tableName)\n"
        output += separator(totalWidths) + "\n"
        output += "|" + padded("#", to: 6) + "|" + headerCells + "\n"
        output += separator(totalWidths) + "\n"
        for (rowIndex, row) in rows.enumerated() {
            let indexPrefix = padded("#\(rowIndex)", to: 6)
            let rowContent = zip(chunkColumns, chunkWidths)
                .map { padded(row[$0].map { String(describing: $0) } ?? "NULL", to: $1) }
                .joined(separator: "|")
            output += "|" + indexPrefix + "|" + rowContent + "|\n"
        }
        output += separator(totalWidths) + "\n\n"
    }
    return output
}

nonisolated internal func printTables(
    sql: String,
    rows: [[String: any Sendable]],
    chunkSize: Int = 5,
    primaryKey: String? = "pk"
) {
    guard sql
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .lowercased()
        .hasPrefix("select") else {
        print("Unable to print, because it's not a SELECT statement.")
        return
    }
    guard let first = rows.first else {
        print("No rows returned from table: \(extractTableName(from: sql) ?? "Unknown")")
        return
    }
    var columns = Array(first.keys)
    if let primaryKey, columns.contains(primaryKey) {
        columns.removeAll(where: { $0 == primaryKey })
        columns.insert(primaryKey, at: 0)
    }
    var columnWidths = columns.map { $0.count }
    for row in rows {
        for (index, key) in columns.enumerated() {
            let stringValue = String(describing: row[key] ?? "NULL")
            columnWidths[index] = max(columnWidths[index], stringValue.count)
        }
    }
    let chunkRanges: [Range<Int>]
    if chunkSize <= 0 {
        chunkRanges = [0..<columns.count]
    } else {
        chunkRanges = Array(stride(from: 0, to: columns.count, by: chunkSize)).map { start in
            start..<min(start + chunkSize, columns.count)
        }
    }
    for chunkRange in chunkRanges {
        let chunkColumns = Array(columns[chunkRange])
        let chunkWidths = Array(columnWidths[chunkRange])
        let totalWidths = [6] + chunkWidths
        func padded(_ value: String, to width: Int) -> String {
            " " + value + String(repeating: " ", count: width - value.count + 1)
        }
        func separator(_ widths: [Int]) -> String {
            "+" + widths
                .map { String(repeating: "-", count: $0 + 2) }
                .joined(separator: "+") + "+"
        }
        let tableName = extractTableName(from: sql) ?? "Unknown"
        let rangeDescription = "\(chunkRange.lowerBound + 1)-\(chunkRange.upperBound)"
        let headerCells = zip(chunkColumns, chunkWidths)
            .map { padded($0, to: $1) }
            .joined(separator: "|") + "|"
        print("* Columns \(rangeDescription) of \(columns.count) from table: \(tableName)")
        print(separator(totalWidths))
        print("|" + padded("#", to: 6) + "|" + headerCells)
        print(separator(totalWidths))
        for (rowIndex, row) in rows.enumerated() {
            let indexPrefix = padded("#\(rowIndex)", to: 6)
            let rowContent = zip(chunkColumns, chunkWidths)
                .map { padded(String(describing: row[$0] ?? "NULL"), to: $1) }
                .joined(separator: "|")
            print("|" + indexPrefix + "|" + rowContent + "|")
        }
        print(separator(totalWidths))
        print()
    }
}

nonisolated internal func printASCIITable(headers: [String], rows: [[String]]) {
    let columnWidths = headers.indices.map { index in
        max(headers[index].count, rows.map { $0[index].count }.max() ?? 0)
    }
    func line(character: Character) -> String {
        "+" + columnWidths
            .map { String(repeating: character, count: $0 + 2) }
            .joined(separator: "+") + "+"
    }
    func rowLine(cells: [String]) -> String {
        "|" + cells.enumerated().map { index, cell in
            " " + cell.padding(
                toLength: columnWidths[index],
                withPad: " ",
                startingAt: 0
            ) + " "
        }.joined(separator: "|") + "|"
    }
    print(line(character: "-"))
    print(rowLine(cells: headers))
    print(line(character: "-"))
    for row in rows { print(rowLine(cells: row)) }
    print(line(character: "-"))
}
