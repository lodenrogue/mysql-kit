import Logging
import MySQLKit
import SQLKitBenchmark
import XCTest
import NIOSSL
import AsyncKit
import Algorithms

class MySQLKitTests: XCTestCase {
    func testSQLBenchmark() throws {
        try SQLBenchmarker(on: self.sql).run()
    }

    func testNullDecode() throws {
        struct Person: Codable {
            let id: Int
            let name: String?
        }

        let rows = try self.sql.raw("SELECT 1 as `id`, null as `name`")
            .all(decoding: Person.self).wait()
        XCTAssertEqual(rows[0].id, 1)
        XCTAssertEqual(rows[0].name, nil)
    }

    func testCustomJSONCoder() throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .secondsSince1970
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .secondsSince1970
        let db = self.mysql.sql(encoder: .init(json: encoder), decoder: .init(json: decoder))

        struct Foo: Codable, Equatable {
            var bar: Bar
        }
        struct Bar: Codable, Equatable {
            var baz: Date
        }

        try db.create(table: "foo")
            .column("bar", type: .custom(SQLRaw("JSON")))
            .run().wait()
        defer { try! db.drop(table: "foo").ifExists().run().wait() }

        let foo = Foo(bar: .init(baz: .init(timeIntervalSince1970: 1337)))
        try db.insert(into: "foo").model(foo).run().wait()

        let rows = try db.select().columns("*").from("foo").all(decoding: Foo.self).wait()
        XCTAssertEqual(rows, [foo])
    }
    
    func testRecordingPerfStats() throws {
        struct Foo: Codable {
            let id: Int
            let name: String
//            let notJson: String
            let jsonIt: [String: [Double]]
        }
        let inserts = (1...10000).map { Foo(id: $0, name: "\($0)", /*notJson: (1...2500).map(Double.init).map(String.init(_:)).joined()) }*/ jsonIt: .init(uniqueKeysWithValues: (1...100).map { ("\($0)", (1...25).map(Double.init)) })) }
        try self.sql.drop(table: "foo").ifExists().run().wait()
        try self.sql.create(table: "foo")
            .column("id", type: .bigint, .primaryKey(autoIncrement: false), .notNull)
            .column("name", type: .text, .notNull)
//            .column("notJson", type: .custom(SQLRaw("mediumtext")), .notNull)
            .column("jsonIt", type: .custom(SQLRaw("json")), .notNull)
            .run().wait()
        defer { try! self.sql.drop(table: "foo").ifExists().run().wait() }
        var perfs: [SQLQueryPerformanceRecord] = []
        for subset in inserts.chunks(ofCount: 250) {
            try perfs.append(self.sql.insert(into: "foo").models(Array(subset)).runRecordingPerformance().wait())
        }
        var totals = perfs.reduce(into: SQLQueryPerformanceRecord()) { $0.aggregate(record: $1) }
        perfs.first![metric: .serializedQueryText].map { totals.record(value: $0, for: .serializedQueryText) }
        perfs.first![metric: .fluentBypassFlag].map { totals.record(value: $0, for: .fluentBypassFlag) }
        let (rows, selPerf) = try self.sql.select().column("*").from("foo").allRecordingPerformance(decoding: Foo.self).wait()
        
        self.sql.logger.info("INSERT (all) \(totals.description)")
        self.sql.logger.info("SELECT (\(rows.count) rows) \(selPerf.description)")
    }

    var sql: SQLDatabase {
        self.mysql.sql()
    }

    var mysql: MySQLDatabase {
        self.pools.database(logger: .init(label: "codes.vapor.mysql"))
    }

    var eventLoopGroup: EventLoopGroup!
    var pools: EventLoopGroupConnectionPool<MySQLConnectionSource>!

    override func setUpWithError() throws {
        try super.setUpWithError()
        XCTAssertTrue(isLoggingConfigured)
        var tls = TLSConfiguration.makeClientConfiguration()
        tls.certificateVerification = .none
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        let configuration = MySQLConfiguration(
            hostname: env("MYSQL_HOSTNAME") ?? "localhost",
            port: env("MYSQL_PORT").flatMap(Int.init) ?? 3306,
            username: env("MYSQL_USERNAME") ?? "vapor_username",
            password: env("MYSQL_PASSWORD") ?? "vapor_password",
            database: env("MYSQL_DATABASE") ?? "vapor_database",
            tlsConfiguration: tls
        )
        self.pools = .init(
            source: .init(configuration: configuration),
            maxConnectionsPerEventLoop: 2,
            requestTimeout: .seconds(30),
            logger: .init(label: "codes.vapor.mysql"),
            on: self.eventLoopGroup
        )
    }

    override func tearDownWithError() throws {
        try self.pools.syncShutdownGracefully()
        self.pools = nil
        try self.eventLoopGroup.syncShutdownGracefully()
        self.eventLoopGroup = nil
        try super.tearDownWithError()
    }
}

func env(_ name: String) -> String? {
    getenv(name).flatMap { String(cString: $0) }
}

let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = env("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ?? .debug
        return handler
    }
    return true
}()
