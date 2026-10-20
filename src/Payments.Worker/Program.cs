using OpenTelemetry.Trace;
using OrderFlow.Shared.Messaging;
using OrderFlow.Shared.Telemetry;
using Payments.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<RabbitMqPublisher>();
builder.Services.AddHostedService<OrderCreatedConsumer>();
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddOrderFlowTelemetry(
        "payments-worker",
        t => t.AddSource(OrderCreatedConsumer.ActivitySourceName)));

builder.Build().Run();
