using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Producer
{
	public class MessageBus(string bootstrapserver, string schemaRegistry)
	{
		public async Task ProducerAsync<T>(string topic, T message)
		{
			var schemaConfig = new SchemaRegistryConfig
			{
				Url = schemaRegistry
			};

			var schemaReg = new CachedSchemaRegistryClient(schemaConfig);


			var config = new ProducerConfig
			{
				//Acknowledgements
				Acks = Acks.None,//Não aguarda confirmação do lado do broker
				//Acks = Acks.Leader,//Aguarda o broaker leader confirmar o recebimento
				//Acks = Acks.All,//aguarda todos os nodes confirmar

				BootstrapServers = bootstrapserver,
			};

			var headers = new Dictionary<string, string>();
			headers["transactionId"] = Guid.NewGuid().ToString();

			var producer = new ProducerBuilder<string, T>(config)
				.SetValueSerializer(new AvroSerializer<T>(schemaReg))
				.Build();

			var result = await producer.ProduceAsync(topic, new Message<string, T>
			{
				Key = Guid.NewGuid().ToString(),
				Value = message,
				Headers = headers.DictionaryToHeader()
			});

			await Task.CompletedTask;
		}
	}
}
