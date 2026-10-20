using System.Net.Sockets;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace OrderFlow.Shared.Messaging;

public static class RabbitMq
{
    public const string Exchange = "orderflow";

    private const int MaxAttempts = 10;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(2);

    public static IConnection Connect()
    {
        var factory = new ConnectionFactory
        {
            HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost",
            DispatchConsumersAsync = true
        };

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return factory.CreateConnection();
            }
            catch (Exception ex) when (attempt < MaxAttempts && ex is BrokerUnreachableException or SocketException)
            {
                Thread.Sleep(RetryDelay);
            }
        }
    }
}
