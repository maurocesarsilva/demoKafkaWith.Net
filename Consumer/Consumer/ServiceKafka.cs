
using Producer.Avros;

namespace Consumer
{
	public class ServiceKafka : BackgroundService
	{
		private readonly MessageBus _messageBus;

		public ServiceKafka(MessageBus messageBus)
		{
			_messageBus = messageBus;
		}

		protected override async Task ExecuteAsync(CancellationToken stoppingToken)
		{
			await _messageBus.ConsumerAsync<Curso>("topicoTeste", ExecuteConsumer, stoppingToken);
		}

		public async Task ExecuteConsumer(object obj)
		{
			Console.WriteLine(obj.ToString());
		}
	}
}
