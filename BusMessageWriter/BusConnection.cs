using System.Text;

public class BusConnection : IBusConnection
{
    public Task PublishAsync(byte[] bytes)
    {
        //just a test stub
        Console.WriteLine(Encoding.UTF8.GetString(bytes));
        return Task.CompletedTask;
    }
}