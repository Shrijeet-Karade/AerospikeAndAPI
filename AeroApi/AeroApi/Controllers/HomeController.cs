using System.Collections.Generic;
using System.Web.Http;
using Aerospike.Client;

namespace AeroApi.Controllers
{
    public class HomeController : ApiController
    {
        AerospikeClient aerospikeClient = new AerospikeClient("18.235.70.103", 3000);
        public string nameSpace = "AirEngine";
        public string setName = "Shrijeet";
      
        [HttpPut]
        [Route("GetDataByKey")]
        public List<Record> GetDataByKey([FromBody]List<string> data)
        {
            List<Record> result = new List<Record>();

            foreach (var uniqueKey in data)
            {
                var key = new Key(nameSpace, setName, uniqueKey.ToString());
                Record dataByKey = aerospikeClient.Get(new WritePolicy(), key);

               result.Add(dataByKey);
            }
            return result;
        }

        public void DeleteByKey([FromBody]string key)
        {
            aerospikeClient.Delete(new WritePolicy(), new Key(nameSpace, setName, key));
        }
        // Put: api/Trolls/ Updataing content of a tweet using their tweet id.
        public void Put([FromBody]Tweets tweet)
        {
            aerospikeClient.Put(new WritePolicy(), new Key(nameSpace, setName, tweet.Id), new Bin[] { new Bin("" + tweet.BinName, tweet.NewValue) });
        }

    }
}
