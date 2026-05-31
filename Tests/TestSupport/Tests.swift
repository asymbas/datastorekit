//
//  Tests.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import DataStoreKit
import Foundation
import Logging
import Testing

let options: DataStoreOptions = {
    []
}()

let shouldDisableLogger = ProcessInfo.processInfo.environment["DATASTOREKIT_DISABLE_LOGGING"] != nil

let level: Logger.Level = {
    switch ProcessInfo.processInfo.environment["DATASTOREKIT_LOG_LEVEL"]?.lowercased() {
    case "trace": .trace
    case "debug": .debug
    case "info": .info
    case "notice": .notice
    case "warning": .warning
    case "error": .error
    case "critical": .critical
    default: .notice
    }
}()

nonisolated private let loggingDefault: Void = {
    LoggingSystem.bootstrap { label in
        guard !shouldDisableLogger else {
            return SwiftLogNoOpLogHandler()
        }
        var handler = StreamLogHandler.standardOutput(label: label)
        if label.split(separator: ".").contains("query") {
            handler.logLevel = .trace
        } else {
            handler.logLevel = level
        }
        return handler
    }
}()

extension Trait where Self == LoggingBootstrap {
    public static var bootstrap: Self { .init() }
}

public struct LoggingBootstrap: SuiteTrait, TestTrait, TestScoping {
    nonisolated public func provideScope(
        for test: Test,
        testCase: Test.Case?,
        performing function: @Sendable () async throws -> Void
    ) async throws {
        _ = loggingDefault
        try await function()
    }
}
