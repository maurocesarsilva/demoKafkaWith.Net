using Confluent.Kafka;
using System.Text;

namespace Producer
{
	public static class MessageBusExtensions
	{
		public static Headers DictionaryToHeader(this Dictionary<string, string> dictionary)
		{
			var headers = new Headers();

			if (dictionary is not null && dictionary.Count > 0)
			{
				foreach (var property in dictionary)
				{
					var header = new Header(
						property.Key,
						Encoding.UTF8.GetBytes(property.Value));

					headers.Add(header);
				}
			}

			return headers;
		}
	}

}
