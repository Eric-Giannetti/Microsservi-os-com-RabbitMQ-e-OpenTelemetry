using Notifications.Worker;
using OpenTelemetry.Trace;
using OrderFlow.Shared.Telemetry;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<PaymentProcessedConsumer>();
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddOrderFlowTelemetry(
        "notifications-worker",
        t => t.AddSource(PaymentProcessedConsumer.ActivitySourceName)));

builder.Build().Run();
