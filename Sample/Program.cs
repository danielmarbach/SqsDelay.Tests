using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using NServiceBus;

class Program
{
    public static ConcurrentDictionary<string, DateTime> sentAndReceived = new ConcurrentDictionary<string, DateTime>();
    
    public static ConcurrentBag<StatsEntry> stats = new ConcurrentBag<StatsEntry>();

    public struct StatsEntry
    {
        public string Id;
        public DateTime ScheduledFor;
        public DateTime ReceivedAt;

        public StatsEntry(string id, DateTime scheduledFor, DateTime receivedAt)
        {
            Id = id;
            ScheduledFor = scheduledFor;
            ReceivedAt = receivedAt;
        }
    }
    
    static async Task Main()
    {
        Console.Title = "Sqs.Delayed.Tests";

        Console.WriteLine("Purging queues");
        var client = new AmazonSQSClient();
        try
        {
            var inputQueue = await client.GetQueueUrlAsync("Sqs-Delayed-Tests");
            await client.PurgeQueueAsync(inputQueue.QueueUrl);
        }
        catch (QueueDoesNotExistException)
        {
        }

        try
        {
            var delayedQueue = await client.GetQueueUrlAsync("Sqs-Delayed-Tests-delay.fifo");
            await client.PurgeQueueAsync(delayedQueue.QueueUrl);
        }
        catch (QueueDoesNotExistException)
        {
        }
        Console.WriteLine("Queues purged.");
        
        var endpointConfiguration = new EndpointConfiguration("Sqs.Delayed.Tests");
        var transport = endpointConfiguration.UseTransport<SqsTransport>();
        transport.S3("nservicebus-sqs-delayed", "sqs-delayed");
        transport.UnrestrictedDurationDelayedDelivery();

        endpointConfiguration.SendFailedMessagesTo("error");
        endpointConfiguration.EnableInstallers();
        endpointConfiguration.UsePersistence<InMemoryPersistence>();

        var endpointInstance = await Endpoint.Start(endpointConfiguration)
            .ConfigureAwait(false);

        var cts = new CancellationTokenSource(TimeSpan.FromDays(1));
        var syncher = new TaskCompletionSource<bool>();
        
        var sendTask = Task.Run(() => Sending(endpointInstance, cts.Token, syncher), CancellationToken.None);
        var checkTask = Task.Run(() => DumpCurrentState(cts.Token), CancellationToken.None);
        
        await Task.WhenAll(sendTask, checkTask);

        await CheckState(syncher);
        
        await endpointInstance.Stop()
            .ConfigureAwait(false);

        Console.WriteLine("Press any key to exit.");
        Console.ReadKey();
    }

    static async Task Sending(IMessageSession endpointInstance, CancellationToken token, TaskCompletionSource<bool> syncher)
    {
        try
        {
            var attempt = 0;
            var random = new Random();

            while (!token.IsCancellationRequested)
            {
                var delayDeliveryWith = TimeSpan.FromMinutes(random.Next(1, 5) * random.Next(14, 17));

                for (var i = 0; i < random.Next(1, 10); i++)
                {
                    if (token.IsCancellationRequested)
                    {
                        return;
                    }

                    var now = DateTime.UtcNow;
                    var shouldBeReceivedAt = now + delayDeliveryWith;
                    var myMessage = new MyMessage
                    {
                        Attempt = $"MyMessageSmall/Attempt {attempt++}/Sent at '{now.ToString(CultureInfo.InvariantCulture)}'/Delayed with '{delayDeliveryWith}'"
                    };
                    var options = new SendOptions();
                    options.RouteToThisEndpoint();
                    options.DelayDeliveryWith(delayDeliveryWith);

                    await endpointInstance.Send(myMessage, options)
                        .ConfigureAwait(false);

                    sentAndReceived.AddOrUpdate(myMessage.Attempt, shouldBeReceivedAt, (s, v) => shouldBeReceivedAt);
                }

                delayDeliveryWith = TimeSpan.FromMinutes(random.Next(1, 5) * random.Next(14, 17));
                for (var i = 0; i < random.Next(1, 10); i++)
                {
                    if (token.IsCancellationRequested)
                    {
                        return;
                    }

                    var now = DateTime.UtcNow;
                    var shouldBeReceivedAt = now + delayDeliveryWith;
                    var myMessage = new MyMessage
                    {
                        Data = new byte[257 * 1024],
                        Attempt = $"MyMessageLarge/Attempt {attempt++}/Sent at '{now.ToString(CultureInfo.InvariantCulture)}'/Delayed with '{delayDeliveryWith}'"
                    };
                    var options = new SendOptions();
                    options.RouteToThisEndpoint();
                    options.DelayDeliveryWith(delayDeliveryWith);

                    await endpointInstance.Send(myMessage, options)
                        .ConfigureAwait(false);

                    sentAndReceived.AddOrUpdate(myMessage.Attempt, shouldBeReceivedAt, (s, v) => shouldBeReceivedAt);
                }

                await Task.Delay(TimeSpan.FromMinutes(random.Next(1, 16)), token);
            }
        }
        catch (OperationCanceledException)
        {
            // ignore
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Sending error {ex.Message}. Aborting");
        }
        finally
        {
            Console.WriteLine();
            Console.WriteLine("--- Sending ---");
            Console.WriteLine("Done sending...");
            Console.WriteLine("--- Sending ---");
            syncher.TrySetResult(true);
        }
    }

    static async Task DumpCurrentState(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            Console.Clear();
            Console.WriteLine("--- Current state ---");
            if (!sentAndReceived.IsEmpty)
            {
                foreach (var entry in sentAndReceived.OrderBy(e => e.Value))
                {
                    Console.WriteLine($"'{entry.Key}' to be received at '{entry.Value}'");
                }
            }
            else
            {
                Console.WriteLine("empty.");
            }
            Console.WriteLine("--- Current state ---");

            await WriteStats();
            
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(20), token);
            }
            catch (OperationCanceledException)
            {
            }
        }   
    }
    
    static async Task CheckState(TaskCompletionSource<bool> syncher)
    {
        await syncher.Task;
        
        while (!sentAndReceived.IsEmpty)
        {
            Console.Clear();
            Console.WriteLine("--- Not yet received ---");
            foreach (var entry in sentAndReceived.OrderBy(e => e.Value))
            {
                Console.WriteLine($"'{entry.Key}' to be received at '{entry.Value}'");
            }

            Console.WriteLine("--- Not yet received ---");
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(20));
            }
            catch (OperationCanceledException)
            {
            }
        }
        
        await WriteStats();
        
        Console.WriteLine();
        Console.WriteLine("--- Summary ---");
        Console.WriteLine("Received everything. Done");
        Console.WriteLine("--- Summary ---");
    }

    static async Task WriteStats()
    {
        using (var writer = new StreamWriter(@".\stats.csv", false))
        {
            await writer.WriteLineAsync($"{nameof(StatsEntry.Id)},{nameof(StatsEntry.ScheduledFor)},{nameof(StatsEntry.ReceivedAt)},Delta");
            foreach (var statsEntry in stats.OrderBy(s => s.ScheduledFor))
            {
                var delta = statsEntry.ReceivedAt - statsEntry.ScheduledFor;
                await writer.WriteLineAsync($"{statsEntry.Id},{statsEntry.ScheduledFor},{statsEntry.ReceivedAt},{delta}");
            }

            await writer.FlushAsync();
            writer.Close();
        }
    }
}