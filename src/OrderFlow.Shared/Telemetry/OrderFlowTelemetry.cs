using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace OrderFlow.Shared.Telemetry;

public static class OrderFlowTelemetry
{
    public const string ActivitySourceName = "OrderFlow.Messaging";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    public static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

    public static TracerProviderBuilder AddOrderFlowTelemetry(
        this TracerProviderBuilder builder,
        string serviceName,
        Action<TracerProviderBuilder>? configure = null)
    {
        builder
            .ConfigureResource(r => r.AddService(serviceName))
            .AddSource(ActivitySourceName);

        configure?.Invoke(builder);

        return builder.AddOtlpExporter();
    }
}
