using Microsoft.AspNetCore.Mvc;

namespace Producer.Controllers
{
	[ApiController]
	[Route("[controller]")]
	public class ProducerKafkaController : ControllerBase
	{
		private readonly MessageBus _messageBus;

		public ProducerKafkaController(MessageBus messageBus)
		{
			_messageBus = messageBus;
		}

		[HttpGet]
		public async Task<IActionResult> Get()
		{
			await _messageBus.ProducerAsync("topicoTeste", new { Name = "Topico publicado" });

			return Ok();
		}
	}
}
