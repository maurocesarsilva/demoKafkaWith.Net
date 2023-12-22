using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DevStore.MessageBus.Serializador;

namespace Consumer
{
	public class MessageBus(string bootstrapserver, string schemaRegistry)
	{

		public async Task ConsumerAsync<T>(
			 string topic,
			 Func<T, Task> onMessage,
			 CancellationToken cancellation)
		{
			_ = Task.Factory.StartNew(async () =>
			{
				var schemaConfig = new SchemaRegistryConfig
				{
					Url = schemaRegistry
				};

				var schemaReg = new CachedSchemaRegistryClient(schemaConfig);

				var config = new ConsumerConfig
				{
					GroupId = "grupo-curso",
					BootstrapServers = bootstrapserver,
					EnableAutoCommit = false, //habilita para dar commit manual
					EnablePartitionEof = true, //Notifica que chegou ao fim da partição
					AutoOffsetReset = AutoOffsetReset.Earliest//lê a partir do mais novo
					//AutoOffsetReset = AutoOffsetReset.Latest //lê todos
				};

				using var consumer = new ConsumerBuilder<string, T>(config)
					.SetValueDeserializer(new AvroDeserializer<T>(schemaReg).AsSyncOverAsync())
					.Build();

				consumer.Subscribe(topic);

				while (!cancellation.IsCancellationRequested)
				{
					var result = consumer.Consume();

					if (result.IsPartitionEOF)
					{
						continue;
					}

					var headers = result.Message.Headers.HeaderToDictionary();

					await onMessage(result.Message.Value);

					consumer.Commit();
				}
			}, cancellation, TaskCreationOptions.LongRunning, TaskScheduler.Default);

			await Task.CompletedTask;
		}
	}
}
