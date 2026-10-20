using System.Diagnostics;
using OpenTelemetry.Trace;
using OrderFlow.Shared.Contracts;
using OrderFlow.Shared.Messaging;
using OrderFlow.Shared.Telemetry;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<RabbitMqPublisher>();
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddOrderFlowTelemetry(
        "orders-api",
        t => t.AddAspNetCoreInstrumentation(o =>
            o.Filter = ctx => ctx.Request.Path != "/health")));

var app = builder.Build();

// Resolve eagerly so the app fails fast when the broker is unreachable.
app.Services.GetRequiredService<RabbitMqPublisher>();

app.MapGet("/health", () => Results.Ok(new { status = "healthy" }));

app.MapPost("/orders", (CreateOrderRequest request, RabbitMqPublisher publisher) =>
{
    if (string.IsNullOrWhiteSpace(request.Customer) || request.Amount <= 0)
    {
        return Results.BadRequest(new { error = "customer must not be empty and amount must be greater than zero." });
    }

    var order = new OrderCreated(Guid.NewGuid(), request.Customer, request.Amount);
    publisher.Publish(RoutingKeys.OrderCreated, order);

    return Results.Accepted($"/orders/{order.OrderId}", new
    {
        orderId = order.OrderId,
        traceId = Activity.Current?.TraceId.ToString()
    });
});

app.Run();

public record CreateOrderRequest(string? Customer, decimal Amount);
