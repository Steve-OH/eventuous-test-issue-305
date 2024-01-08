using Eventuous;
using Microsoft.Data.SqlClient;
using System;
using System.Threading.Tasks;

namespace DoubledEvents;

public static class DummyInit
{
    public static async Task DropAll(string connectionString, string schemaName)
    {
        await drop(connectionString, schemaName, "Messages");
        await drop(connectionString, schemaName, "Streams");
        await drop(connectionString, schemaName, "Checkpoints");
        await drop(connectionString, schemaName, "Output");
        var sql = $"DROP TABLE IF EXISTS [{schemaName}].Output";

        await using var connection = new SqlConnection(connectionString);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await connection.OpenAsync();
        await command.ExecuteNonQueryAsync();
    }

    public static async Task CreateOutput(string connectionString, string schemaName)
    {
        var sql = $"CREATE TABLE [{schemaName}].Output (Id VARCHAR(64), Value INT)";

        await using var connection = new SqlConnection(connectionString);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await connection.OpenAsync();
        await command.ExecuteNonQueryAsync();
    }

    private static async Task drop(string connectionString, string schemaName, string tableName)
    {
        var sql = $"DROP TABLE IF EXISTS [{schemaName}].[{tableName}]";

        await using var connection = new SqlConnection(connectionString);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await connection.OpenAsync();
        await command.ExecuteNonQueryAsync();
    }
}

public class Dummy : Aggregate<DummyState>
{
    public void HandleCmd(DummyCommand cmd)
    {
        Apply(new DummyEvent(cmd.Id, cmd.Value * 2));
    }
}

public record DummyState : State<DummyState>
{
    public int Value { get; init; }

    public override DummyState When(object obj) => obj switch
    {
        DummyEvent evt => this with { Value = evt.Value },
        _ => this
    };
}

public record DummyId(string Value) : Id(Value);

public record DummyCommand(string Id, int Value);

[EventType("DummyEvent")]
public record DummyEvent(string Id, int Value);

public class DummyCommandService : CommandService<Dummy, DummyState, DummyId>
{
    public DummyCommandService(IAggregateStore store) : base(store)
    {
        On<DummyCommand>().InState(ExpectedState.Any).GetId(cmd => new DummyId(cmd.Id)).Act((dummy, cmd) =>
        {
            dummy.HandleCmd(cmd);
        });
    }
}

public class DummyEventHandler : Eventuous.Subscriptions.EventHandler
{
    public DummyEventHandler(string connectionString, string schemaName)
    {
        var sql = @$"SELECT * FROM [{schemaName}].Output";

        On<DummyEvent>(async context =>
        {
            Console.WriteLine($"{context.Message.Id}: {context.Message.Value}");
            try
            {
                await using var connection = new SqlConnection(connectionString);
                await using var command = connection.CreateCommand();
                await connection.OpenAsync();
                command.CommandText = sql;
                command.Parameters.AddWithValue("@id", context.Message.Id);
                command.Parameters.AddWithValue("@value", context.Message.Value);
                // ======== un-comment the following line to display the errant behavior
                await using var reader = await command.ExecuteReaderAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        });
    }
}
