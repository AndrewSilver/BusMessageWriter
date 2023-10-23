using System.Text;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.DependencyInjection;

var serviceProvider = new ServiceCollection()
    .AddTransient<IBusConnection, BusConnection>()
    .AddTransient<BusMessageWriter>()
    .AddSingleton<App>()
    .BuildServiceProvider();

var app = serviceProvider.GetRequiredService<App>();
await app.RunAsync();

public class App
{
    private readonly BusMessageWriter _writer;

    public App(BusMessageWriter writer)
    {
        _writer = writer;
    }

    public async Task RunAsync()
    {
        var options = new ParallelOptions()
        {
            MaxDegreeOfParallelism = 2
        };

        //assuming we don't care about order of bytes here
        await Parallel.ForEachAsync(Enumerable.Range(0, 100), options, async (i, c) =>
        {
            await _writer.SendMessageAsync(Encoding.UTF8.GetBytes($"{i:00} "));
        });

        Console.WriteLine("==========");

        for (int i = 0; i < 100; i++)
        {
            _writer.SendMessageToBuffer(Encoding.UTF8.GetBytes($"{i:00} "));
        }

        await _writer.ConsumeAsync();

        Console.WriteLine("All done");
        Console.ReadKey();
    }
}

public class BusMessageWriter
{
    private readonly IBusConnection _connection;

    private readonly MemoryStream _buffer = new();

    private readonly SemaphoreSlim _semaphore;

    private readonly BatchBlock<byte[]> _batchBlock = new(10);

    public BusMessageWriter(IBusConnection connection)
    {
        _connection = connection;
        _semaphore = new SemaphoreSlim(1, 1);
    }

    #region original method with lock

    /// <summary>
    /// Sends a message with buffering.
    /// </summary>
    /// <param name="nextMessage">Message.</param>
    public async Task SendMessageAsync(byte[] nextMessage)
    {
        await _semaphore.WaitAsync();
        try
        {
            _buffer.Write(nextMessage, 0, nextMessage.Length);
            if (_buffer.Length > 100)
            {
                await _connection.PublishAsync(_buffer.ToArray());
                _buffer.SetLength(0);
            }
        }
        finally
        {
            _semaphore.Release();
        }
    }

    #endregion

    #region another way using producer/consumer approach

    /// <summary>
    /// Sends (produces) a message.
    /// </summary>
    /// <param name="nextMessage">Message.</param>
    public void SendMessageToBuffer(byte[] nextMessage)
    {
        _batchBlock.Post(nextMessage);
    }

    /// <summary>
    /// Consumes messages in batches of 10.
    /// </summary>
    public async Task ConsumeAsync()
    {
        while (await _batchBlock.OutputAvailableAsync())
        {
            if (_batchBlock.TryReceive(out var items))
            {
                var aggregate = items.SelectMany(it => it).ToArray();
                await _connection.PublishAsync(aggregate);
            }
        }
    }
    #endregion
}
