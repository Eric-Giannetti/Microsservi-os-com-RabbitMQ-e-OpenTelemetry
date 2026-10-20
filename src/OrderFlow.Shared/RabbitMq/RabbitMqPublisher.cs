using System.Diagnostics;
using System.Text.Json;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using OrderFlow.Shared.Telemetry;
using RabbitMQ.Client;

namespace OrderFlow.Shared.Messaging;

public sealed class RabbitMqPublisher : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly object _lock = new();

    public RabbitMqPublisher()
    {
        _connection = RabbitMq.Connect();
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(RabbitMq.Exchange, ExchangeType.Topic, durable: true);
    }

    public void Publish<T>(string routingKey, T message)
    {
        using var activity = OrderFlowTelemetry.ActivitySource.StartActivity(
            $"{routingKey} publish", ActivityKind.Producer);

        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination.name", RabbitMq.Exchange);
        activity?.SetTag("messaging.rabbitmq.destination.routing_key", routingKey);
        activity?.SetTag("messaging.operation", "publish");

        var body = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);

        lock (_lock)
        {
            var properties = _channel.CreateBasicProperties();
            properties.DeliveryMode = 2;
            properties.ContentType = "application/json";
            properties.Headers = new Dictionary<string, object>();

            if (activity is not null)
            {
                OrderFlowTelemetry.Propagator.Inject(
                    new PropagationContext(activity.Context, Baggage.Current),
                    properties,
                    static (props, key, value) => props.Headers[key] = value);
            }

            _channel.BasicPublish(RabbitMq.Exchange, routingKey, properties, body);
        }
    }

    public void Dispose()
    {
        _channel.Dispose();
        _connection.Dispose();
    }
}
