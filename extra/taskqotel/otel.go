package taskqotel

import (
	"github.com/binguo-casa/taskqv3/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/binguo-casa/taskqv3")

type OpenTelemetryHook struct{}

var _ taskq.ConsumerHook = (*OpenTelemetryHook)(nil)

func (h OpenTelemetryHook) BeforeProcessMessage(evt *taskq.ProcessMessageEvent) error {
	evt.Message.Ctx, _ = tracer.Start(evt.Message.Ctx, evt.Message.TaskName)
	return nil
}

func (h OpenTelemetryHook) AfterProcessMessage(evt *taskq.ProcessMessageEvent) error {
	ctx := evt.Message.Ctx

	span := trace.SpanFromContext(ctx)
	defer span.End()

	if err := evt.Message.Err; err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}

	return nil
}
