using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Trace;
using OrderFlow.Shared.Telemetry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OrderFlow.Shared.Messaging;

public abstract class RabbitMqConsumer<T> : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly ILogger _logger;
    private IConnection? _connection;
    private IModel? _channel;

    protected RabbitMqConsumer(ILogger logger) => _logger = logger;

    protected abstract string Queue { get; }
    protected abstract string RoutingKey { get; }

    protected abstract Task HandleAsync(T message, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        _connection = RabbitMq.Connect();
        _channel = _connection.CreateModel();
        _channel.ExchangeDeclare(RabbitMq.Exchange, ExchangeType.Topic, durable: true);
        _channel.QueueDeclare(Queue, durable: true, exclusive: false, autoDelete: false);
        _channel.QueueBind(Queue, RabbitMq.Exchange, RoutingKey);
        _channel.BasicQos(0, 10, false);

        var channel = _channel;
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += (_, ea) => OnReceivedAsync(channel, ea, stoppingToken);
        channel.BasicConsume(Queue, autoAck: false, consumer);

        _logger.LogInformation("Consuming {Queue} (binding {RoutingKey})", Queue, RoutingKey);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task OnReceivedAsync(IModel channel, BasicDeliverEventArgs ea, CancellationToken cancellationToken)
    {
        // RabbitMQ.Client 6.x returns header values as byte[]
        var parentContext = OrderFlowTelemetry.Propagator.Extract(default, ea.BasicProperties, static (props, key) =>
            props.Headers is not null && props.Headers.TryGetValue(key, out var value) && value is byte[] bytes
                ? new[] { Encoding.UTF8.GetString(bytes) }
                : Enumerable.Empty<string>());

        Baggage.Current = parentContext.Baggage;

        using var activity = OrderFlowTelemetry.ActivitySource.StartActivity(
            $"{RoutingKey} process", ActivityKind.Consumer, parentContext.ActivityContext);

        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.destination.name", RabbitMq.Exchange);
        activity?.SetTag("messaging.rabbitmq.destination.routing_key", ea.RoutingKey);
        activity?.SetTag("messaging.operation", "process");

        try
        {
            var message = JsonSerializer.Deserialize<T>(ea.Body.Span, JsonOptions)
                          ?? throw new InvalidOperationException("Message body is empty.");

            await HandleAsync(message, cancellationToken);
            channel.BasicAck(ea.DeliveryTag, multiple: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process message from {Queue}", Queue);
            activity?.RecordException(ex);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
        }
    }

    public override void Dispose()
    {
        _channel?.Dispose();
        _connection?.Dispose();
        base.Dispose();
    }
}
