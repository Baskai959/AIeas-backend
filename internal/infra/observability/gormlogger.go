package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	gormlogger "gorm.io/gorm/logger"
	"gorm.io/gorm/utils"
)

// GormSlogLogger 把 GORM 内部日志桥接到 slog.Logger，消除 GORM 自带 ANSI 颜色码污染。
// 字段命名统一为 sql / rows / elapsed_ms / source / error，方便 JSON 模式下被日志系统索引。
type GormSlogLogger struct {
	base                      *slog.Logger
	level                     gormlogger.LogLevel
	slowThreshold             time.Duration
	ignoreRecordNotFoundError bool
}

// NewGormLogger 构造一个桥接到 slog 的 GORM logger。
//   - base 必须非空；nil 时使用默认 slog logger，避免 panic。
//   - slowThreshold <= 0 时关闭慢 SQL 检测。
//   - ignoreNotFound 为 true 时 ErrRecordNotFound 不再上报为 ERROR。
func NewGormLogger(base *slog.Logger, slowThreshold time.Duration, ignoreNotFound bool) gormlogger.Interface {
	if base == nil {
		base = slog.Default()
	}
	return &GormSlogLogger{
		base:                      base.With(slog.String("component", "gorm")),
		level:                     gormlogger.Warn,
		slowThreshold:             slowThreshold,
		ignoreRecordNotFoundError: ignoreNotFound,
	}
}

// LogMode 返回一份切换了日志级别的副本，符合 GORM 接口约定。
func (l *GormSlogLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	clone := *l
	clone.level = level
	return &clone
}

// Info 输出 GORM 的 Info 级别消息（schema 创建、AutoMigrate 等）。
func (l *GormSlogLogger) Info(ctx context.Context, msg string, args ...interface{}) {
	if l.level < gormlogger.Info {
		return
	}
	l.base.InfoContext(ctx, formatGormMessage(msg, args...))
}

// Warn 输出 GORM 的 Warn 级别消息。
func (l *GormSlogLogger) Warn(ctx context.Context, msg string, args ...interface{}) {
	if l.level < gormlogger.Warn {
		return
	}
	l.base.WarnContext(ctx, formatGormMessage(msg, args...))
}

// Error 输出 GORM 的 Error 级别消息。
func (l *GormSlogLogger) Error(ctx context.Context, msg string, args ...interface{}) {
	if l.level < gormlogger.Error {
		return
	}
	l.base.ErrorContext(ctx, formatGormMessage(msg, args...))
}

// Trace 输出 SQL 执行详情；这是 GORM 调用最频繁的方法，因此尽量避免反射。
//   - err 非 nil 且非 ErrRecordNotFound（when ignored）→ ERROR
//   - 耗时超过 slowThreshold → WARN，并标记 slow=true
//   - 其它 → DEBUG
func (l *GormSlogLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if l.level <= gormlogger.Silent {
		return
	}
	elapsed := time.Since(begin)

	switch {
	case err != nil && l.level >= gormlogger.Error &&
		(!l.ignoreRecordNotFoundError || !errors.Is(err, gormlogger.ErrRecordNotFound)):
		sql, rows := fc()
		l.base.LogAttrs(ctx, slog.LevelError, "gorm sql error",
			slog.String("error", err.Error()),
			slog.Int64("elapsed_ms", elapsed.Milliseconds()),
			slog.Int64("rows", rows),
			slog.String("sql", sql),
			slog.String("source", utils.FileWithLineNum()),
		)
	case l.slowThreshold > 0 && elapsed > l.slowThreshold && l.level >= gormlogger.Warn:
		sql, rows := fc()
		l.base.LogAttrs(ctx, slog.LevelWarn, "gorm slow sql",
			slog.Bool("slow", true),
			slog.Int64("elapsed_ms", elapsed.Milliseconds()),
			slog.Int64("rows", rows),
			slog.String("sql", sql),
			slog.String("source", utils.FileWithLineNum()),
		)
	default:
		if l.level < gormlogger.Info {
			return
		}
		sql, rows := fc()
		l.base.LogAttrs(ctx, slog.LevelDebug, "gorm sql",
			slog.Int64("elapsed_ms", elapsed.Milliseconds()),
			slog.Int64("rows", rows),
			slog.String("sql", sql),
			slog.String("source", utils.FileWithLineNum()),
		)
	}
}

func formatGormMessage(msg string, args ...interface{}) string {
	if len(args) == 0 {
		return msg
	}
	return fmt.Sprintf(msg, args...)
}
