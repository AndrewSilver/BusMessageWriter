public interface IBusConnection
{
    Task PublishAsync(byte[] bytes);
}