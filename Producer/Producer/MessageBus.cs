using Confluent.Kafka;

namespace Producer
{
	public class MessageBus
	{
		private readonly string _bootstrapserver;

		public MessageBus(string bootstrapserver)
		{
			_bootstrapserver = bootstrapserver;
		}

		public async Task ProducerAsync<T>(string topic, T message)
		{
			var config = new ProducerConfig
			{
				//Acknowledgements
				Acks = Acks.None,//Não aguarda confirmação do lado do broker
				//Acks = Acks.Leader,//Aguarda o broaker leader confirmar o recebimento
				//Acks = Acks.All,//aguarda todos os nodes confirmar

				BootstrapServers = _bootstrapserver,
			};

			var headers = new Dictionary<string, string>();
			headers["transactionId"] = Guid.NewGuid().ToString();

			var producer = new ProducerBuilder<string, T>(config)
				.SetValueSerializer(new Serializer<T>())
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
