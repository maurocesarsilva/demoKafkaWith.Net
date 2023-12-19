using Confluent.Kafka;
using DevStore.MessageBus.Serializador;

namespace Consumer
{
	public class MessageBus
	{
		private readonly string _bootstrapserver;

		public MessageBus(string bootstrapserver)
		{
			_bootstrapserver = bootstrapserver;
		}

		public async Task ConsumerAsync<T>(
			 string topic,
			 Func<T, Task> onMessage,
			 CancellationToken cancellation)
		{
			_ = Task.Factory.StartNew(async () =>
			{
				var config = new ConsumerConfig
				{
					GroupId = "grupo-curso",
					BootstrapServers = _bootstrapserver,
					EnableAutoCommit = false, //habilita para dar commit manual
					EnablePartitionEof = true, //Notifica que chegou ao fim da partição
					AutoOffsetReset = AutoOffsetReset.Earliest//lê a partir do mais novo
					//AutoOffsetReset = AutoOffsetReset.Latest //lê todos
				};

				using var consumer = new ConsumerBuilder<string, T>(config)
					.SetValueDeserializer(new Deserializer<T>())
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
