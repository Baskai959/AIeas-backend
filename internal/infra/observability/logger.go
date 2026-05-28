package observability

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"aieas_backend/internal/config"

	charmlog "github.com/charmbracelet/log"
	"github.com/muesli/termenv"
)

// colorProfile 根据是否为 TTY 决定终端颜色档位。
// 非 TTY（容器、管道、CI）输出 Ascii，避免 ANSI 控制符泄漏到日志收集器。
func colorProfile(tty bool) termenv.Profile {
	if !tty {
		return termenv.Ascii
	}
	return termenv.ANSI256
}

// New 是兼容入口：默认 text + tty=true，方便老调用点和测试。
func New(level string) *slog.Logger {
	return NewWithOptions(level, "text", true)
}

// NewLogger 沿用历史签名：根据 ObservabilityConfig 构建 slog.Logger，输出到 os.Stdout。
func NewLogger(cfg config.ObservabilityConfig) *slog.Logger {
	return NewLoggerWithWriter(cfg, os.Stdout)
}

// NewLoggerWithWriter 显式指定 writer，主要用于测试。
func NewLoggerWithWriter(cfg config.ObservabilityConfig, out io.Writer) *slog.Logger {
	return newLogger(cfg.LogLevel, cfg.Format, false, out)
}

// NewWithOptions 是新签名：传入日志级别、format("text"/"json")、是否 TTY。
// 输出统一指向 os.Stdout，避免容器/管道场景下 stderr 收集不到。
func NewWithOptions(level string, format string, tty bool) *slog.Logger {
	return newLogger(level, format, tty, os.Stdout)
}

func newLogger(level, format string, tty bool, out io.Writer) *slog.Logger {
	lv := parseLevel(level)
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		// JSON handler 关闭 AddSource，避免与 GORM source 字段重复。
		// 仅在 JSON 模式下叠加 trace_id/span_id 注入：text 模式偏向人读的开发态，
		// 多两个 hex 字段会显著降低可读性（参见 traceContextHandler 注释）。
		base := slog.NewJSONHandler(out, &slog.HandlerOptions{Level: lv})
		return slog.New(WithTraceContext(base))
	default:
		// text 模式 → charmbracelet/log 提供的 slog handler
		handler := charmlog.NewWithOptions(out, charmlog.Options{
			TimeFormat:      time.RFC3339,
			ReportCaller:    true,
			ReportTimestamp: true,
			Prefix:          "",
		})
		handler.SetColorProfile(colorProfile(tty))
		handler.SetLevel(toCharmLevel(lv))
		return slog.New(handler)
	}
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func toCharmLevel(lv slog.Level) charmlog.Level {
	switch {
	case lv <= slog.LevelDebug:
		return charmlog.DebugLevel
	case lv <= slog.LevelInfo:
		return charmlog.InfoLevel
	case lv <= slog.LevelWarn:
		return charmlog.WarnLevel
	default:
		return charmlog.ErrorLevel
	}
}
