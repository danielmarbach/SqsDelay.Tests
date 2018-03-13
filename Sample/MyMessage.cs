using NServiceBus;

public class MyMessage :
    IMessage
{
    public byte[] Data { get; set; }
    public string Attempt { get; set; }
}