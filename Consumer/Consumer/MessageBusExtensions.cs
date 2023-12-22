using Confluent.Kafka;
using System.Text;

namespace Consumer
{
	public static class MessageBusExtensions
	{
		public static Dictionary<string, string> HeaderToDictionary(this Headers headers)
		{
			var dictionary = new Dictionary<string, string>();

			if (headers is not null && headers.Count > 0)
			{
				foreach (var header in headers)
				{
					dictionary[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
				}
			}

			return dictionary;
		}
	
	}
}
