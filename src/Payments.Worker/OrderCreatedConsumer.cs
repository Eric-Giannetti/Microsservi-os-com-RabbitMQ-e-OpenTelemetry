using System.Diagnostics;
using OrderFlow.Shared.Contracts;
using OrderFlow.Shared.Messaging;

namespace Payments.Worker;

public sealed class OrderCreatedConsumer : RabbitMqConsumer<OrderCreated>
{
    public const string ActivitySourceName = "Payments.Worker";

    private const decimal ApprovalLimit = 5000m;

    private static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private readonly RabbitMqPublisher _publisher;
    private readonly ILogger<OrderCreatedConsumer> _logger;

    public OrderCreatedConsumer(RabbitMqPublisher publisher, ILogger<OrderCreatedConsumer> logger)
        : base(logger)
    {
        _publisher = publisher;
        _logger = logger;
    }

    protected override string Queue => "payments.order-created";
    protected override string RoutingKey => RoutingKeys.OrderCreated;

    protected override async Task HandleAsync(OrderCreated message, CancellationToken cancellationToken)
    {
        using var activity = ActivitySource.StartActivity("charge customer");
        activity?.SetTag("order.id", message.OrderId);
        activity?.SetTag("order.amount", message.Amount);

        await Task.Delay(Random.Shared.Next(200, 601), cancellationToken);

        var approved = message.Amount <= ApprovalLimit;
        activity?.SetTag("payment.approved", approved);

        _logger.LogInformation(
            "Payment {Result} for order {OrderId} ({Customer}, {Amount})",
            approved ? "APPROVED" : "DECLINED", message.OrderId, message.Customer, message.Amount);

        _publisher.Publish(
            RoutingKeys.PaymentProcessed,
            new PaymentProcessed(message.OrderId, message.Customer, message.Amount, approved));
    }
}
