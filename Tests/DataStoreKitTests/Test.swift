//
//  Test.swift
//  DataStoreKit
//
//  Copyright 2025 Asymbas and Anferne Pineda.
//  Licensed under the Apache License, Version 2.0 (see LICENSE file).
//  SPDX-License-Identifier: Apache-2.0
//

import Logging
import Testing

internal let logging: Void = {
    LoggingSystem.bootstrap { label in
        if label.split(separator: ".").contains("query") {
            var handler = StreamLogHandler.standardOutput(label: label)
            handler.logLevel = .trace
            return handler
        } else {
            return SwiftLogNoOpLogHandler()
        }
    }
}()
