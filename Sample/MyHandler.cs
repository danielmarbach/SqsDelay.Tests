using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.Logging;

public class MyHandler :
    IHandleMessages<MyMessage>
{
    static ILog log = LogManager.GetLogger<MyHandler>();

    public Task Handle(MyMessage message, IMessageHandlerContext context)
    {
        Program.sentAndReceived.TryRemove(message.Attempt, out var _);
        return Task.CompletedTask;
    }
}