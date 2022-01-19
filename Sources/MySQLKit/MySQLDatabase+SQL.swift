import SQLKit
import Dispatch

extension MySQLDatabase {
    public func sql(
        encoder: MySQLDataEncoder = .init(),
        decoder: MySQLDataDecoder = .init()
    ) -> SQLDatabase {
        _MySQLSQLDatabase(database: self, encoder: encoder, decoder: decoder)
    }
}


private struct _MySQLSQLDatabase {
    let database: MySQLDatabase
    let encoder: MySQLDataEncoder
    let decoder: MySQLDataDecoder
}

extension _MySQLSQLDatabase: SQLDatabase {
    var logger: Logger {
        self.database.logger
    }
    
    var eventLoop: EventLoop {
        self.database.eventLoop
    }
    
    var dialect: SQLDialect {
        MySQLDialect()
    }
    
    func execute(sql query: SQLExpression, _ onRow: @escaping (SQLRow) -> ()) -> EventLoopFuture<Void> {
        let (sql, binds) = self.serialize(query)
        do {
            return try self.database.query(sql, binds.map { encodable in
                return try self.encoder.encode(encodable)
            }, onRow: { row in
                onRow(row.sql(decoder: self.decoder))
            })
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }
    
    func execute(
        sqlWithPerformanceTracking query: SQLExpression,
        _ onRow: @escaping (SQLRow) -> ()
    ) -> EventLoopFuture<SQLQueryPerformanceRecord> {
        var perfRecord = SQLQueryPerformanceRecord()
        let queryStart = DispatchTime.now()
        
        let (sql, binds) = perfRecord.measure(metric: .serializationDuration) { self.serialize(query) }
        perfRecord.record(binds.count, for: .boundParameterCount)
        if binds.count > 10 {
            let condensedSql = sql.replacingOccurrences(
                of: String(repeating: " ?, ", count: binds.count - 3),
                with: "..<\(binds.count - 3)>..",
                options: .backwards,
                range: nil
            )
            perfRecord.record(condensedSql, for: .serializedQueryText)
        } else {
            perfRecord.record(sql, for: .serializedQueryText)
        }
        
        do {
            let encodedBinds = try perfRecord.measure(metric: .parameterEncodingDuration) {
                try binds.map { try self.encoder.encode($0) }
            }
            let processingStart = DispatchTime.now()
            
            return self.database.query(sql, encodedBinds, onRow: { row in
                let rowDecodeStart = DispatchTime.now()
                let rowSql = row.sql(decoder: self.decoder)
                
                perfRecord.record(additional: DispatchTime.secondsElapsed(since: rowDecodeStart), for: .outputRowsDecodingDuration)
                onRow(rowSql)
            }).map {
                perfRecord.record(DispatchTime.secondsElapsed(since: processingStart), for: .processingDuration)
                perfRecord.deduct(valueFor: .outputRowsDecodingDuration, from: .processingDuration)
                perfRecord.record(DispatchTime.secondsElapsed(since: queryStart), for: .fullExecutionDuration)
                perfRecord.record(true, for: .fluentBypassFlag)
                return perfRecord
            }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
            
    }
}
