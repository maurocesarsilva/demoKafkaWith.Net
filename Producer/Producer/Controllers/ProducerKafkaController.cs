using Microsoft.AspNetCore.Mvc;
using Producer.Avros;

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

		[HttpPost]
		public async Task<IActionResult> Post([FromBody] Curso obj)
		{
			await _messageBus.ProducerAsync("topicoTeste", obj);

			return Ok();
		}
	}
}
