using Eventuous;
using Eventuous.SqlServer;
using Eventuous.SqlServer.Subscriptions;
using Eventuous.Subscriptions.Filters;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DoubledEvents;

public class Program
{
    private const string connectionString = "Data Source=SAPAYOA;Initial Catalog=NAEPAssetsLocal;Integrated Security=true;Trust Server Certificate=true;MultipleActiveResultSets=True";
    private const string schemaName = "DUMMY";

    public static async Task Main(string[] args)
    {
        // clean up any previous runs
        await DummyInit.DropAll(connectionString, schemaName);
        await DummyInit.CreateOutput(connectionString, schemaName);

        var eventStore = new SqlServerStore(new SqlServerStoreOptions
        {
            ConnectionString = connectionString,
            InitializeDatabase = true,
            Schema = schemaName
        });
        var aggregateStore = new AggregateStore(eventStore);

        TypeMap.RegisterKnownEventTypes();
        var schema = new Schema(schemaName);
        await schema.CreateSchema(connectionString, null, CancellationToken.None);

        var checkpointStore = new SqlServerCheckpointStore(new SqlServerCheckpointStoreOptions
        {
            ConnectionString = connectionString,
            Schema = schemaName
        });
        var commandService = new DummyCommandService(aggregateStore);

        var pipe = new ConsumePipe();
        pipe.AddDefaultConsumer(new DummyEventHandler(connectionString, schemaName));

        var subscription = new SqlServerAllStreamSubscription(new SqlServerAllStreamSubscriptionOptions
        {
            SubscriptionId = "DummyProjection",
            ConnectionString = connectionString,
            Schema = schemaName
        }, checkpointStore, pipe);

        await subscription.Subscribe(subscriptionId =>
        {
            Console.WriteLine($"Subscription {subscriptionId} started");
        }, (subscriptionId, dropReason, e) =>
        {
            var suffix = e is not null ? $" with exception {e}" : "";
            Console.WriteLine($"{subscriptionId} dropped because {dropReason}{suffix}");
        }, CancellationToken.None);

        await commandService.Handle(new DummyCommand("Foo", 42), CancellationToken.None);
        await commandService.Handle(new DummyCommand("Bar", 37), CancellationToken.None);
        await commandService.Handle(new DummyCommand("Foo", 31416), CancellationToken.None);

        // give the subscription a chance to run
        await Task.Delay(2000);

        await subscription.Unsubscribe(subscriptionId =>
            {
                Console.WriteLine($"Subscription {subscriptionId} stopped");
            }, CancellationToken.None
        );
    }
}
