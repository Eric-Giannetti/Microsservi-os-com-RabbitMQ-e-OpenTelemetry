namespace OrderFlow.Shared.Contracts;

public record OrderCreated(Guid OrderId, string Customer, decimal Amount);

public record PaymentProcessed(Guid OrderId, string Customer, decimal Amount, bool Approved);

public static class RoutingKeys
{
    public const string OrderCreated = "order.created";
    public const string PaymentProcessed = "payment.processed";
}
