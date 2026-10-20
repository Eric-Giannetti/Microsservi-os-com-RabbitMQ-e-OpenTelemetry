using System.Diagnostics;
using OrderFlow.Shared.Contracts;
using OrderFlow.Shared.Messaging;

namespace Notifications.Worker;

public sealed class PaymentProcessedConsumer : RabbitMqConsumer<PaymentProcessed>
{
    public const string ActivitySourceName = "Notifications.Worker";

    private static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private readonly ILogger<PaymentProcessedConsumer> _logger;

    public PaymentProcessedConsumer(ILogger<PaymentProcessedConsumer> logger) : base(logger)
    {
        _logger = logger;
    }

    protected override string Queue => "notifications.payment-processed";
    protected override string RoutingKey => RoutingKeys.PaymentProcessed;

    protected override async Task HandleAsync(PaymentProcessed message, CancellationToken cancellationToken)
    {
        using var activity = ActivitySource.StartActivity("send notification");
        activity?.SetTag("order.id", message.OrderId);
        activity?.SetTag("payment.approved", message.Approved);

        await Task.Delay(150, cancellationToken);

        _logger.LogInformation(
            "Email sent to {Customer}: payment for order {OrderId} was {Result}",
            message.Customer, message.OrderId, message.Approved ? "approved" : "declined");
    }
}
