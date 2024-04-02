using Eventuous;
using Eventuous.SqlServer;
using Eventuous.SqlServer.Subscriptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Threading;
using System.Threading.Tasks;

namespace DoubledEvents;

public class Program
{
    private const string connectionString = "Data Source=...;Initial Catalog=...;Integrated Security=true;Trust Server Certificate=true;MultipleActiveResultSets=True";
    private const string schemaName = "DUMMY";

    public static async Task Main(string[] args)
    {
        // clean up any previous runs
        await DummyInit.DropAll(connectionString, schemaName);
        await DummyInit.CreateOutput(connectionString, schemaName);

        // start everything up
        var host = await createHostBuilder(args).StartAsync();

        // execute some commands
        var commandService = (ICommandService<Dummy>)host.Services.GetRequiredService(typeof(ICommandService<Dummy>));

        await commandService.Handle(new DummyCommand("Foo", 42), CancellationToken.None);
        await commandService.Handle(new DummyCommand("Bar", 37), CancellationToken.None);
        await commandService.Handle(new DummyCommand("Foo", 31416), CancellationToken.None);

        // give the subscription a chance to run
        await Task.Delay(2000);

        // shut down
        await host.StopAsync();
    }

    private static IHostBuilder createHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureServices(services =>
            {
                // set up Eventuous config
                services.AddEventuousSqlServer(connectionString, schemaName, true);
                services.AddAggregateStore<SqlServerStore>();
                services.AddCheckpointStore(_ => new SqlServerCheckpointStore(new SqlServerCheckpointStoreOptions
                {
                    ConnectionString = connectionString,
                    Schema = schemaName
                }));
                services.AddCommandService<DummyCommandService, Dummy>();
                services.AddSubscription<SqlServerAllStreamSubscription, SqlServerAllStreamSubscriptionOptions>(
                    "DummyProjection", subBuilder => subBuilder.Configure(
                            options =>
                            {
                                options.ConnectionString = connectionString;
                                options.Schema = schemaName;
                            })
                        .AddEventHandler(_ => new DummyEventHandler(connectionString, schemaName)));

                TypeMap.RegisterKnownEventTypes();
            });
}
