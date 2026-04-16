//
//  SQLValue.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

private import Logging
private import Synchronization
public import DataStoreSupport
public import Foundation
public import System

nonisolated private let logger: Logger = .init(label: "com.asymbas.datastorekit.sql")

public struct SQLValue: Equatable, Hashable, Sendable {
    nonisolated private let storage: Storage
    
    nonisolated public var sqlType: SQLType {
        storage.sqlType
    }
    
    nonisolated public var valueType: any SendableMetatype.Type {
        storage.valueType
    }
    
    nonisolated public var base: any Sendable {
        storage.base
    }
    
    nonisolated public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.sqlType == rhs.sqlType && lhs.description == rhs.description
    }
    
    nonisolated public func hash(into hasher: inout Hasher) {
        hasher.combine(sqlType)
        hasher.combine(description)
    }
    
    private final class Storage: Sendable {
        nonisolated private let cache: AtomicLazyReference<Cache> = .init()
        nonisolated fileprivate let sqlType: SQLType
        nonisolated fileprivate let valueType: any Sendable.Type
        nonisolated fileprivate let base: any Sendable
        
        nonisolated fileprivate init(
            sqlType: SQLType,
            valueType: any Sendable.Type,
            base: any Sendable
        ) {
            self.sqlType = sqlType
            self.valueType = valueType
            self.base = base
        }
        
        nonisolated internal var description: String {
            if let literal = self.cache.load() { return literal.value }
            let literal: String
            switch base {
            case is NSNull, is SQLNull:
                literal = "NULL"
            case let value as any BinaryInteger, let value? as (any BinaryInteger)?:
                literal = "\(value.description)"
            case let value as any BinaryFloatingPoint, let value? as (any BinaryFloatingPoint)?:
                literal = "\(value)"
            case let value as String, let value? as String?:
                literal = "'\(value)'"
            case let value as Data, let value? as Data?:
                literal = "X'" + value.map { String(format: "%02X", $0) }.joined() + "'"
            case let value as any Codable:
                literal = "'\(SQLValue.json(value))'"
            default:
                literal = "\(base)"
            }
            return cache.storeIfNil(.init(value: literal)).value
        }
        
        private final class Cache: Sendable {
            nonisolated internal let value: String
            nonisolated internal init(value: String) { self.value = value }
        }
    }
}

extension SQLValue {
    nonisolated private init(_ storage: Storage) {
        self.storage = storage
    }
    
    /// Creates an SQL value from another of the same type.
    nonisolated public init(_ value: Self) {
        self = value
    }
    
    nonisolated public init(_ value: NSNull) {
        self = .null
    }
    
    nonisolated public init(_ value: SQLNull) {
        self = .null
    }
    
    nonisolated public init(_ value: Bool) {
        self.storage = .init(
            sqlType: .integer,
            valueType: Bool.self,
            base: Int64(value ? 1 : 0)
        )
    }
    
    /// Creates an SQL `INTEGER` value.
    nonisolated public init(_ value: Int64) {
        self.storage = .init(sqlType: .integer, valueType: Int64.self, base: value)
    }
    
    /// Creates an SQL `INTEGER` value and normalizes to `Int64` for any signed and unsigned integer types.
    nonisolated public init<T: BinaryInteger & Sendable>(_ value: T) {
        guard let value = Int64(exactly: value) else {
            fatalError("Integer value \(value) out of range for Int64/SQLite INTEGER")
        }
        self = .init(value)
    }
    
    /// Creates an SQL `REAL` value.
    nonisolated public init(_ value: Double) {
        self.storage = .init(sqlType: .real, valueType: Double.self, base: value)
    }
    
    /// Creates an SQL `REAL` value and normalizes to `Double` for any floating point types.
    nonisolated public init<T: BinaryFloatingPoint & Sendable>(_ value: T) {
        self = .init(Double(value))
    }
    
    nonisolated public init(_ value: Decimal) {
        self = .init((value as NSDecimalNumber).doubleValue)
    }
    
    nonisolated public init(_ value: Date) {
        self = .init(value.timeIntervalSince1970)
    }
    
    nonisolated public init(_ value: Measurement<UnitDuration>) {
        self = .init(value.converted(to: .seconds).value)
    }
    
    nonisolated public init<T: StringProtocol & Sendable>(_ value: T) {
        self.storage = .init(sqlType: .text, valueType: T.self, base: value)
    }
    
    nonisolated public init(_ value: FilePath) {
        self.storage = .init(sqlType: .text, valueType: FilePath.self, base: value.string)
    }
    
    nonisolated public init(_ value: URL) {
        self.storage = .init(sqlType: .text, valueType: URL.self, base: value.absoluteString)
    }
    
    nonisolated public init(_ value: UUID) {
        self.storage = .init(sqlType: .text, valueType: UUID.self, base: value.uuidString)
    }
    
    nonisolated public init(_ value: Data) {
        self.storage = .init(sqlType: .blob, valueType: Data.self, base: value)
    }
    
    nonisolated public init<T: Codable & Collection & Sendable>(_ value: T) {
        if value.isEmpty {
            self.storage = .init(sqlType: .text, valueType: T.self, base: "[]")
        } else {
            self = .object(value)
        }
    }
    
    nonisolated public init<Key, Value>(_ value: [Key: Value])
    where Key: Codable & Hashable & Sendable, Value: Codable & Sendable {
        if value.isEmpty {
            self.storage = .init(sqlType: .text, valueType: [Key: Value].self, base: "{}")
        } else {
            self = .object(value)
        }
    }
    
    /// Creates a compatible type-erased SQL value.
    nonisolated public init(any value: Any) {
        guard let type: any Sendable.Type = sendable(cast: Swift.type(of: value)) else {
            fatalError("An SQL value must conform to Sendable: \(value) \(Swift.type(of: value))")
        }
        switch value {
        case _ where isNil(value):
            fallthrough
        case is NSNull, is SQLNull:
            self.storage = .init(sqlType: .null, valueType: type, base: SQLNull())
        case let value as Optional<Any> where value == nil:
            self.storage = .init(sqlType: .null, valueType: type, base: SQLNull())
        case let value as any RawRepresentable:
            self = Self(any: value.rawValue)
        case let value as Bool:
            self = .init(value)
        case let value as any BinaryInteger & Sendable:
            self = .init(value)
        case let value as any BinaryFloatingPoint & Sendable:
            self = .init(value)
        case let value as Decimal:
            self = .init(value)
        case let value as Date:
            self = .init(value)
        case let value as Measurement<UnitDuration>:
            self = .init(value)
        case let value as String:
            self.storage = .init(sqlType: .text, valueType: type, base: value)
        case let value as FilePath:
            self = .init(value)
        case let value as URL:
            self = .init(value)
        case let value as UUID:
            self = .init(value)
        case let value as Data:
            self = .init(value)
        case let value as Codable & Sendable:
            self = .object(value)
        case let value:
            if let value = value as? any Collection<Any>, value.isEmpty {
                self.storage = .init(sqlType: .text, valueType: type, base: "[]")
            } else if let value = value as? Dictionary<AnyHashable, Any>, value.isEmpty {
                self.storage = .init(sqlType: .text, valueType: type, base: "{}")
            } else {
                self.storage = .init(sqlType: .null, valueType: type, base: SQLNull())
            }
        }
    }
    
    nonisolated public static func object<T: Codable & Sendable>(_ value: T) -> Self {
        return .init(.init(sqlType: .text, valueType: T.self, base: Self.json(value)))
    }
    
    nonisolated public static func json<T: Decodable>(_ value: String, as _: T.Type = T.self) -> T? {
        do {
            return try JSONDecoder().decode(T.self, from: value.data(using: .utf8)!)
        } catch {
            return nil
        }
    }
    
    nonisolated public static func json<T: Encodable>(_ value: T) -> String {
        do {
            let data = try JSONEncoder().encode(value)
            if let string = String(data: data, encoding: .utf8) {
                return string
            } else {
                fatalError("Failed to encode Codable value as a UTF-8 String: \(value)")
            }
        } catch {
            fatalError("The value cannot be encoded into a JSON representation: \(error)")
        }
    }
}

extension SQLValue: Codable {
    private enum CodingKeys: String, CodingKey {
        case type
        case value
    }
    
    /// Inherited from `Decodable.init(from:)`.
    nonisolated public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let typeString = try container.decode(String.self, forKey: .type)
        switch typeString {
        case "null":
            self.storage = .init(sqlType: .null, valueType: SQLNull.self, base: SQLNull())
        case "integer":
            let value = try container.decode(Int64.self, forKey: .value)
            self.storage = .init(sqlType: .integer, valueType: Int64.self, base: value)
        case "real":
            let value = try container.decode(Double.self, forKey: .value)
            self.storage = .init(sqlType: .real, valueType: Double.self, base: value)
        case "text":
            let value = try container.decode(String.self, forKey: .value)
            self.storage = .init(sqlType: .text, valueType: String.self, base: value)
        case "blob":
            let value = try container.decode(Data.self, forKey: .value)
            self.storage = .init(sqlType: .blob, valueType: Data.self, base: value)
        default:
            throw DecodingError.dataCorruptedError(
                forKey: .type,
                in: container,
                debugDescription: "Unsupported SQLValue type: \(typeString)"
            )
        }
    }
    
    /// Inherited from `Encodable.encode(to:)`.
    nonisolated public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch storage.sqlType {
        case .null:
            try container.encode("null", forKey: .type)
            try container.encodeNil(forKey: .value)
        case .integer:
            guard let value = storage.base as? Int64 else {
                throw EncodingError.invalidValue(
                    storage.base,
                    .init(
                        codingPath: container.codingPath,
                        debugDescription: "Expected Int64 for INTEGER."
                    )
                )
            }
            try container.encode("integer", forKey: .type)
            try container.encode(value, forKey: .value)
        case .real:
            guard let value = storage.base as? Double else {
                throw EncodingError.invalidValue(
                    storage.base,
                    .init(
                        codingPath: container.codingPath,
                        debugDescription: "Expected Double for REAL."
                    )
                )
            }
            try container.encode("real", forKey: .type)
            try container.encode(value, forKey: .value)
        case .text:
            if let value = storage.base as? String {
                try container.encode("text", forKey: .type)
                try container.encode(value, forKey: .value)
            } else if let value = storage.base as? any StringProtocol {
                try container.encode("text", forKey: .type)
                try container.encode(String(value), forKey: .value)
            } else {
                throw EncodingError.invalidValue(
                    storage.base,
                    .init(
                        codingPath: container.codingPath,
                        debugDescription: "Expected String for TEXT."
                    )
                )
            }
        case .blob:
            guard let value = storage.base as? Data else {
                throw EncodingError.invalidValue(
                    storage.base,
                    .init(
                        codingPath: container.codingPath,
                        debugDescription: "Expected Data for BLOB."
                    )
                )
            }
            try container.encode("blob", forKey: .type)
            try container.encode(value, forKey: .value)
        }
    }
}

extension SQLValue {
    nonisolated public static let `true`: Self = .init(true)
    nonisolated public static let `false`: Self = .init(false)
    nonisolated public static let currentTime: Self = .init("CURRENT_TIME")!
    nonisolated public static let currentDate: Self = .init("CURRENT_DATE")!
    nonisolated public static let currentTimestamp: Self = .init("CURRENT_TIMESTAMP")!
    
    nonisolated public static func integer<T>(_ value: T) -> Self
    where T: FixedWidthInteger & Sendable {
        .init(value)
    }
    
    nonisolated public static func integer<T>(_ value: T) -> Self
    where T: BinaryInteger & Sendable {
        .init(value)
    }
    
    nonisolated public static func real<T>(_ value: T) -> Self
    where T: BinaryFloatingPoint & Sendable {
        .init(value)
    }
    
    nonisolated public static func text<T>(_ value: T) -> Self
    where T: StringProtocol & Sendable {
        .init(value)
    }
    
    nonisolated public static func blob(_ value: Data) -> Self {
        .init(value)
    }
    
    nonisolated public static var null: Self {
        .init(Storage.init(sqlType: .null, valueType: SQLNull.self, base: SQLNull()))
    }
}

extension SQLValue: LosslessStringConvertible {
    /// Inherited from `LosslessStringConvertible.init(_:)`.
    nonisolated public init?(_ description: String) {
        let sqlType: SQLType
        let valueType: any Sendable.Type
        let base: any Sendable
        switch description.lowercased() {
        case "null":
            sqlType = .null; valueType = SQLNull.self; base = SQLNull()
        case "true", "1":
            sqlType = .integer; valueType = Int64.self; base = 1 as Int64
        case "false", "0":
            sqlType = .integer; valueType = Int64.self; base = 0 as Int64
        case let literalValue:
            if let blobValue = parseHexBlobLiteral(literalValue) {
                sqlType = .blob; valueType = Data.self; base = blobValue
            } else if let integerValue = Int64(literalValue) {
                sqlType = .integer; valueType = Int64.self; base = integerValue
            } else if let realValue = Double(literalValue) {
                sqlType = .real; valueType = Double.self; base = realValue
            } else {
                sqlType = .text; valueType = String.self; base = literalValue
            }
        }
        self.storage = .init(sqlType: sqlType, valueType: valueType, base: base)
        func parseHexBlobLiteral(_ literalValue: String) -> Data? {
            guard literalValue.count >= 3,
                  literalValue.hasPrefix("x'"),
                  literalValue.hasSuffix("'") else {
                return nil
            }
            let hex = literalValue.dropFirst(2).dropLast()
            guard hex.count % 2 == 0 else { return nil }
            var data = Data()
            data.reserveCapacity(hex.count / 2)
            var index = hex.startIndex
            while index < hex.endIndex {
                let next = hex.index(index, offsetBy: 2)
                let byteString = hex[index..<next]
                guard let byte = UInt8(byteString, radix: 16) else {
                    return nil
                }
                data.append(byte)
                index = next
            }
            return data
        }
    }
}

extension SQLValue: CustomStringConvertible {
    /// Inherited from `CustomStringConvertible.description`.
    nonisolated public var description: String {
        storage.description
    }
}

extension SQLValue {
    nonisolated public static func convert<T>(
        _ value: any Codable & Sendable,
        as type: T.Type
    ) -> T? where T: Decodable {
        if value is T { return value as? T }
        switch value {
        case let value where T.self is any RawRepresentable.Type:
            if let type = T.self as? any RawRepresentable.Type {
                return Self.convert(value, as: type) as? T
            } else {
                return value as? T
            }
        case let value as (any FixedWidthInteger), let value? as (any FixedWidthInteger)?:
            return Self.convert(value, as: type)
        case let value as Double, let value? as Double?:
            return Self.convert(value, as: type)
        case let value as String, let value? as String?:
            return Self.convert(value, as: type)
        case let value as Data, let value? as Data?:
            return value as? T
        case is NSNull:
            return Optional<T>.none
        case is Optional<T>:
            return nil
        default:
            fatalError(
                """
                Unable to convert value:
                \(value) \(Swift.type(of: value)).self to \(T.self).self.
                """
            )
        }
    }
    
    nonisolated public static func convert<T>(
        _ value: any Codable & Sendable,
        as type: T.Type
    ) -> T where T: RawRepresentable {
        switch T.RawValue.self {
        case let type as any Decodable.Type:
            guard let rawValue = Self.convert(value, as: type) as? T.RawValue,
                  let convertedValue = T(rawValue: rawValue) else {
                fallthrough
            }
            return convertedValue
        default:
            fatalError(
                """
                Unable to convert RawRepresentable.RawValue:
                \(value) \(Swift.type(of: value)).self to \(T.RawValue.self).self (\(T.self)).
                """
            )
        }
    }
    
    nonisolated public static func convert<T>(
        _ value: some FixedWidthInteger,
        as type: T.Type
    ) -> T? where T: Decodable {
        if T.self is Bool.Type || T.self is Optional<Bool>.Type {
            return (value != .zero) as? T
        }
        switch T.self {
        case is any SignedInteger.Type:
            return convertValueAsSignedInteger(value, as: type)
        case is any UnsignedInteger.Type:
            return convertValueAsUnsignedInteger(value, as: type)
        default:
            return nil
        }
    }
    
    nonisolated private static func convertValueAsSignedInteger<T>(
        _ value: some FixedWidthInteger,
        as type: T.Type
    ) -> T? where T: Decodable {
        switch T.self {
        case is Int.Type, is Optional<Int>.Type:
            return Int(truncatingIfNeeded: value) as? T
        case is Int8.Type, is Optional<Int8>.Type:
            guard value >= Int64(Int8.min), value <= Int64(Int8.max) else {
                return nil
            }
            return Int8(truncatingIfNeeded: value) as? T
        case is Int16.Type, is Optional<Int16>.Type:
            guard value >= Int64(Int16.min), value <= Int64(Int16.max) else {
                return nil
            }
            return Int16(truncatingIfNeeded: value) as? T
        case is Int32.Type, is Optional<Int32>.Type:
            guard value >= Int64(Int32.min), value <= Int64(Int32.max) else {
                return nil
            }
            return Int32(truncatingIfNeeded: value) as? T
        case is Int64.Type, is Optional<Int64>.Type:
            return Int64(value) as? T
        default:
            return nil
        }
    }
    
    nonisolated private static func convertValueAsUnsignedInteger<T>(
        _ value: some FixedWidthInteger,
        as type: T.Type
    ) -> T? where T: Decodable {
        switch T.self {
        case is UInt.Type, is Optional<UInt>.Type:
            guard value >= 0 else { return nil }
            return UInt(truncatingIfNeeded: value) as? T
        case is UInt8.Type, is Optional<UInt8>.Type:
            guard value >= 0, value <= Int64(UInt8.max) else {
                return nil
            }
            return UInt8(truncatingIfNeeded: value) as? T
        case is UInt16.Type, is Optional<UInt16>.Type:
            guard value >= 0, value <= Int64(UInt16.max) else {
                return nil
            }
            return UInt16(truncatingIfNeeded: value) as? T
        case is UInt32.Type, is Optional<UInt32>.Type:
            guard value >= 0, value <= Int64(UInt32.max) else {
                return nil
            }
            return UInt32(truncatingIfNeeded: value) as? T
        case is UInt64.Type, is Optional<UInt64>.Type:
            guard value >= 0 else {
                return nil
            }
            return UInt64(value) as? T
        default:
            return nil
        }
    }
    
    nonisolated public static func convert<T>(
        _ value: Double,
        as type: T.Type
    ) -> T? where T: Decodable {
        switch T.self {
        case is Float.Type, is Optional<Float>.Type:
            return Float(value) as? T
        case is CGFloat.Type, is Optional<CGFloat>.Type:
            return CGFloat(value) as? T
        case is Decimal.Type, is Optional<Decimal>.Type:
            return (Decimal(value) as Decimal?) as? T
        case is Date.Type, is Optional<Date>.Type:
            return Date(timeIntervalSince1970: value) as? T
        case is Measurement<UnitDuration>.Type, is Optional<Measurement<UnitDuration>>.Type:
            return Measurement(value: value, unit: UnitDuration.seconds) as? T
        default:
            return nil
        }
    }
    
    nonisolated public static func convert<T>(
        _ value: String,
        as type: T.Type
    ) -> T? where T: Decodable {
        switch T.self {
        case is FilePath.Type, is Optional<FilePath>.Type:
            return FilePath(value) as? T
        case is URL.Type, is Optional<URL>.Type:
            return URL(string: value) as? T
        case is UUID.Type, is Optional<UUID>.Type:
            return UUID(uuidString: value) as? T
        case is any Codable.Type:
            do {
                guard let data = value.data(using: .utf8) else {
                    return nil
                }
                return try JSONDecoder().decode(T.self, from: data)
            } catch {
                logger.error("Unable to decode \(T.self).self from JSON data: \(value)")
                fallthrough
            }
        default:
            return nil
        }
    }
}

extension SQLValue {
    // Type metadata will be lost.
    nonisolated public static func row(columns: [String], values: [any Sendable])
    throws -> String {
        var objectValues = [String: Any](minimumCapacity: values.count)
        for (key, value) in zip(columns, values) {
            guard let value = value as? any Encodable else { continue }
            let data = try JSONEncoder().encode(value)
            let object = try JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])
            objectValues[key] = object
        }
        let data = try JSONSerialization.data(withJSONObject: objectValues, options: [])
        return String(decoding: data, as: UTF8.self)
    }
    
    // Type metadata will be lost.
    nonisolated public static func row(_ string: any Sendable)
    throws -> [String: any Sendable] {
        let json = string as? String ?? "{}"
        let decodedValues: [String: any Sendable] = try {
            guard let data = json.data(using: .utf8),
                  let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                return [:]
            }
            return object.mapValues { sendable(cast: $0) ?? SQLNull() }
        }()
        return decodedValues
    }
    
    nonisolated public static func row(
        _ string: any Sendable,
        columnTypes: [String: any (Decodable & Sendable).Type]
    ) throws -> [String: any Sendable] {
        let json = string as? String ?? "{}"
        guard let data = json.data(using: .utf8),
              let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            return [:]
        }
        var decoded = [String: any Sendable](minimumCapacity: object.count)
        for (column, fragment) in object {
            if let type = columnTypes[column] {
                decoded[column] = try row(fragment: fragment, as: type)
            } else {
                decoded[column] = sendable(cast: fragment) ?? SQLNull()
            }
        }
        return decoded
    }
    
    nonisolated private static func row<T: Decodable & Sendable>(
        fragment: Any,
        as type: T.Type
    ) throws -> any Sendable {
        if fragment is NSNull {
            if let value = try rowDecodeJSONFragment(fragment, as: type) {
                return value
            }
            return SQLNull()
        }
        if let value = try rowDecodeJSONFragment(fragment, as: type) {
            return value
        }
        if let value = rowConvertSpecial(fragment, as: type) {
            return value
        }
        if let string = fragment as? String,
           let value = try rowDecodeJSONStringPayload(string, as: type) {
            return value
        }
        return SQLNull()
    }
    
    nonisolated private static func rowDecodeJSONFragment<T: Decodable & Sendable>(
        _ fragment: Any,
        as type: T.Type
    ) throws -> (any Sendable)? {
        let data = try JSONSerialization.data(withJSONObject: fragment, options: [.fragmentsAllowed])
        return try JSONDecoder().decode(type, from: data)
    }
    
    nonisolated private static func rowDecodeJSONStringPayload<T: Decodable & Sendable>(
        _ string: String,
        as type: T.Type
    ) throws -> (any Sendable)? {
        guard let data = string.data(using: .utf8) else {
            return nil
        }
        return try JSONDecoder().decode(type, from: data)
    }
    
    nonisolated private static func rowConvertSpecial<T: Decodable & Sendable>(
        _ fragment: Any,
        as type: T.Type
    ) -> (any Sendable)? {
        if let number = fragment as? NSNumber {
            if CFGetTypeID(number) == CFBooleanGetTypeID() {
                let value = number.boolValue ? Int64(1) : Int64(0)
                return rowConvertInteger(value, as: type)
            }
            if CFNumberIsFloatType(number) {
                return rowConvertReal(number.doubleValue, as: type)
            }
            return rowConvertInteger(number.int64Value, as: type)
        }
        if let string = fragment as? String {
            return rowConvertText(string, as: type)
        }
        return nil
    }
}

extension SQLValue {
    nonisolated private static func convertIntegerValue<T>(_ value: Int64, as type: T.Type)
    -> T? where T: Decodable & Sendable {
        let function: (Int64, T.Type) -> T? = SQLValue.convert
        return function(value, type)
    }
    
    nonisolated private static func convertRealValue<T>(_ value: Double, as type: T.Type)
    -> T? where T: Decodable & Sendable {
        let function: (Double, T.Type) -> T? = SQLValue.convert
        return function(value, type)
    }
    
    nonisolated private static func convertTextValue<T>(_ value: String, as type: T.Type)
    -> T? where T: Decodable & Sendable {
        let function: (String, T.Type) -> T? = SQLValue.convert
        return function(value, type)
    }
    
    nonisolated private static func rowConvertInteger<T>(_ value: Int64, as type: T.Type)
    -> (any Sendable)? where T: Decodable & Sendable {
        switch type {
        case is Bool.Type:
            convertIntegerValue(value, as: Bool.self)
        case is Optional<Bool>.Type:
            convertIntegerValue(value, as: Optional<Bool>.self)
        case is Int.Type:
            convertIntegerValue(value, as: Int.self)
        case is Optional<Int>.Type:
            convertIntegerValue(value, as: Optional<Int>.self)
        case is Int8.Type:
            convertIntegerValue(value, as: Int8.self)
        case is Optional<Int8>.Type:
            convertIntegerValue(value, as: Optional<Int8>.self)
        case is Int16.Type:
            convertIntegerValue(value, as: Int16.self)
        case is Optional<Int16>.Type:
            convertIntegerValue(value, as: Optional<Int16>.self)
        case is Int32.Type:
            convertIntegerValue(value, as: Int32.self)
        case is Optional<Int32>.Type:
            convertIntegerValue(value, as: Optional<Int32>.self)
        case is Int64.Type:
            convertIntegerValue(value, as: Int64.self)
        case is Optional<Int64>.Type:
            convertIntegerValue(value, as: Optional<Int64>.self)
        case is UInt.Type:
            convertIntegerValue(value, as: UInt.self)
        case is Optional<UInt>.Type:
            convertIntegerValue(value, as: Optional<UInt>.self)
        case is UInt8.Type:
            convertIntegerValue(value, as: UInt8.self)
        case is Optional<UInt8>.Type:
            convertIntegerValue(value, as: Optional<UInt8>.self)
        case is UInt16.Type:
            convertIntegerValue(value, as: UInt16.self)
        case is Optional<UInt16>.Type:
            convertIntegerValue(value, as: Optional<UInt16>.self)
        case is UInt32.Type:
            convertIntegerValue(value, as: UInt32.self)
        case is Optional<UInt32>.Type:
            convertIntegerValue(value, as: Optional<UInt32>.self)
        case is UInt64.Type:
            convertIntegerValue(value, as: UInt64.self)
        case is Optional<UInt64>.Type:
            convertIntegerValue(value, as: Optional<UInt64>.self)
        default:
            nil
        }
    }
    
    nonisolated private static func rowConvertReal<T>(_ value: Double, as type: T.Type)
    -> (any Sendable)? where T: Decodable & Sendable {
        switch type {
        case is Float.Type:
            convertRealValue(value, as: Float.self)
        case is Optional<Float>.Type:
            convertRealValue(value, as: Optional<Float>.self)
        case is Double.Type:
            value
        case is Optional<Double>.Type:
            Optional<Double>(value) as any Sendable
        case is CGFloat.Type:
            convertRealValue(value, as: CGFloat.self)
        case is Optional<CGFloat>.Type:
            convertRealValue(value, as: Optional<CGFloat>.self)
        case is Decimal.Type:
            convertRealValue(value, as: Decimal.self)
        case is Optional<Decimal>.Type:
            convertRealValue(value, as: Optional<Decimal>.self)
        case is Date.Type:
            convertRealValue(value, as: Date.self)
        case is Optional<Date>.Type:
            convertRealValue(value, as: Optional<Date>.self)
        case is Measurement<UnitDuration>.Type:
            convertRealValue(value, as: Measurement<UnitDuration>.self)
        case is Optional<Measurement<UnitDuration>>.Type:
            convertRealValue(value, as: Optional<Measurement<UnitDuration>>.self)
        default:
            nil
        }
    }
    
    nonisolated private static func rowConvertText<T>(_ value: String, as type: T.Type)
    -> (any Sendable)? where T: Decodable & Sendable {
        switch type {
        case is String.Type:
            value
        case is Optional<String>.Type:
            Optional<String>(value) as any Sendable
        case is FilePath.Type:
            convertTextValue(value, as: FilePath.self)
        case is Optional<FilePath>.Type:
            convertTextValue(value, as: Optional<FilePath>.self)
        case is URL.Type:
            convertTextValue(value, as: URL.self)
        case is Optional<URL>.Type:
            convertTextValue(value, as: Optional<URL>.self)
        case is UUID.Type:
            convertTextValue(value, as: UUID.self)
        case is Optional<UUID>.Type:
            convertTextValue(value, as: Optional<UUID>.self)
        default:
            nil
        }
    }
}
