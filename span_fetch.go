package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func setMetricIfValueFloat(attributes []attribute.KeyValue, key string, value sql.NullFloat64) []attribute.KeyValue {
	if !value.Valid || value.Float64 == 0 {
		return attributes
	}
	return append(attributes, attribute.Float64(key, value.Float64))
}

func setMetricIfValue(attributes []attribute.KeyValue, key string, value sql.NullInt64) []attribute.KeyValue {
	if !value.Valid || value.Int64 == 0 {
		return attributes
	}
	return append(attributes, attribute.Int64(key, value.Int64))
}

type BlockStats struct {
	hit     sql.NullInt64
	read    sql.NullInt64
	written sql.NullInt64
	dirtied sql.NullInt64
}

type BlockTime struct {
	readTime  sql.NullFloat64
	writeTime sql.NullFloat64
}

func fetchSpans(ctx context.Context, conn *pgx.Conn, tracer trace.Tracer, f *FixedIdGenerator) {
	query := `select
		trace_id, parent_id, span_id,

		span_type, span_operation, deparse_info, parameters,
		span_start, span_start_ns, duration,

		startup,
		pid, subxact_count,
		sql_error_code,
		rows,

		plan_startup_cost, plan_total_cost, plan_rows, plan_width,

		shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written,
		local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written,
		blk_read_time, blk_write_time,
		temp_blks_read, temp_blks_written, temp_blk_read_time, temp_blk_write_time,

		wal_records, wal_fpi, wal_bytes,
		jit_functions, jit_generation_time, jit_inlining_time, jit_optimization_time, jit_emission_time

		from pg_tracing_consume_spans order by span_start;`
	log.Printf("Query: %s", query)
	rows, err := conn.Query(ctx, query)
	fatalIf(err)

	for rows.Next() {
		var traceId int64
		var parentId int64
		var spanId int64
		var span_type string
		var span_operation string
		var deparse_info sql.NullString
		var parameters sql.NullString
		var span_start time.Time
		var span_start_ns int16
		var duration uint64

		var startup sql.NullInt64
		var pid int32
		var subxact_count int32
		var sql_error_code string
		var rowNumber sql.NullInt64
		var planStartupCost sql.NullFloat64
		var planTotalCost sql.NullFloat64
		var planRows sql.NullFloat64
		var planWidth sql.NullInt64

		var sharedBlks BlockStats
		var localBlks BlockStats
		var blkTime BlockTime

		var tempBlks BlockStats
		var tempBlkTime BlockTime

		var wal_records sql.NullInt64
		var wal_fpi sql.NullInt64
		var wal_bytes sql.NullInt64

		var jit_functions sql.NullInt64
		var jit_generation_time sql.NullFloat64
		var jit_inlining_time sql.NullFloat64
		var jit_optimization_time sql.NullFloat64
		var jit_emission_time sql.NullFloat64

		if err := rows.Scan(&traceId, &parentId, &spanId,
			&span_type, &span_operation, &deparse_info, &parameters,
			&span_start, &span_start_ns, &duration, &startup, &pid, &subxact_count, &sql_error_code, &rowNumber,
			&planStartupCost, &planTotalCost, &planRows, &planWidth,
			&sharedBlks.hit, &sharedBlks.read, &sharedBlks.dirtied, &sharedBlks.written,
			&localBlks.hit, &localBlks.read, &localBlks.dirtied, &localBlks.written,
			&blkTime.readTime, &blkTime.writeTime,

			&tempBlks.read, &tempBlks.written,
			&tempBlkTime.readTime, &tempBlkTime.writeTime,

			&wal_records, &wal_fpi, &wal_bytes,
			&jit_functions, &jit_generation_time, &jit_inlining_time, &jit_optimization_time, &jit_emission_time); err != nil {
			log.Fatal(err)
		}
		log.Printf("traceId: %d, parentId: %d, spanId: %d, span_operation: %s, start: %s, start_ns: %d, duration: %d", traceId, parentId, spanId, span_operation, span_start, span_start_ns, duration)

		utraceId := uint64(traceId)
		uparentId := uint64(parentId)
		uspanId := uint64(spanId)

		attributes := make([]attribute.KeyValue, 0)
		setMetricIfValue(attributes, "rows", rowNumber)
		attributes = append(attributes, attribute.Int("pid", int(pid)))
		attributes = append(attributes, attribute.Int("subxact_count", int(subxact_count)))

		attributes = setMetricIfValue(attributes, "block.shared.hit", sharedBlks.hit)
		attributes = setMetricIfValue(attributes, "block.shared.read", sharedBlks.read)
		attributes = setMetricIfValue(attributes, "block.shared.dirtied", sharedBlks.dirtied)
		attributes = setMetricIfValue(attributes, "block.shared.written", sharedBlks.written)

		attributes = setMetricIfValue(attributes, "block.local.hit", localBlks.hit)
		attributes = setMetricIfValue(attributes, "block.local.read", localBlks.read)
		attributes = setMetricIfValue(attributes, "block.local.dirtied", localBlks.dirtied)
		attributes = setMetricIfValue(attributes, "block.local.written", localBlks.written)

		attributes = setMetricIfValueFloat(attributes, "block.read_time", blkTime.readTime)
		attributes = setMetricIfValueFloat(attributes, "block.write_time", blkTime.writeTime)

		attributes = setMetricIfValue(attributes, "block.temp.read", tempBlks.read)
		attributes = setMetricIfValue(attributes, "block.temp.written", tempBlks.written)
		attributes = setMetricIfValueFloat(attributes, "block.temp.read_time", tempBlkTime.readTime)
		attributes = setMetricIfValueFloat(attributes, "block.temp.write_time", tempBlkTime.writeTime)

		attributes = setMetricIfValue(attributes, "wal.records", wal_records)
		attributes = setMetricIfValue(attributes, "wal.fpi", wal_fpi)
		attributes = setMetricIfValue(attributes, "wal.bytes", wal_bytes)

		attributes = setMetricIfValueFloat(attributes, "plan.startup_cost", planStartupCost)
		attributes = setMetricIfValueFloat(attributes, "plan.total_cost", planTotalCost)
		attributes = setMetricIfValueFloat(attributes, "plan.rows", planRows)
		attributes = setMetricIfValue(attributes, "plan.width", planWidth)

		attributes = setMetricIfValue(attributes, "jit.functions", jit_functions)
		attributes = setMetricIfValueFloat(attributes, "jit.generation_time", jit_generation_time)
		attributes = setMetricIfValueFloat(attributes, "jit.inlining_time", jit_inlining_time)
		attributes = setMetricIfValueFloat(attributes, "jit.optimization_time", jit_optimization_time)
		attributes = setMetricIfValueFloat(attributes, "jit.emission_time", jit_emission_time)

		if sql_error_code != "00000" {
			attributes = append(attributes, attribute.String("error.msg", "Query error"))
			attributes = append(attributes, attribute.String("error.msg", sql_error_code))
		}

		traceIdBytes := make([]byte, 16)
		binary.BigEndian.PutUint64(traceIdBytes[0:16], utraceId)
		spanIdBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(spanIdBytes[0:8], uspanId)
		parentIdBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(parentIdBytes[0:8], uparentId)

		// TODO: Use span events
		// setMetricIfValue(attributes, "first_tuple", startup)

		spanStartNs := span_start.Add(time.Duration(span_start_ns))
		startOptions := []trace.SpanStartOption{
			trace.WithTimestamp(spanStartNs),
			trace.WithAttributes(attributes...),
			trace.WithSpanKind(trace.SpanKindServer),
		}

		psc := trace.SpanContext{}
		psc = psc.WithTraceID(trace.TraceID(traceIdBytes))
		psc = psc.WithSpanID(trace.SpanID(parentIdBytes))
		ctx = trace.ContextWithSpanContext(ctx, psc)

		spanName := span_operation
		if deparse_info.Valid {
			spanName = fmt.Sprintf("%s %s", spanName, deparse_info.String)
		}

		// Modify the fixed spanID generator before starting the span
		f.FixedSpanID = trace.SpanID(spanIdBytes)
		_, span := tracer.Start(ctx, spanName, startOptions...)
		// End the span
		spanEndNs := spanStartNs.Add(time.Duration(duration))
		endOptions := []trace.SpanEndOption{
			trace.WithTimestamp(spanEndNs),
		}
		span.End(endOptions...)

		//		meta := make(map[string]string, 0)
		//		if parameters.Valid {
		//			// We're expecting something like
		//			// $1 = '1', $2 = '2'
		//			generate_meta_parameters(meta, parameters.String)
		//		}
	}

}
