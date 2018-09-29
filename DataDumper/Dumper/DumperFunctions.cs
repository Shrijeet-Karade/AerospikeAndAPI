using System;
using System.Threading;
using Aerospike.Client;
using Microsoft.VisualBasic.FileIO;


namespace Dumper
{
    class DumperFunctions
    {
        static void Main(string[] args)
        {
            try
            {
                //using (var reader = new StreamReader(@"C:\Users\skarade\Desktop\2018-01-trump-twitter-wars\data\tweets\tweet4.csv"))
                //{
                //    List<string> listA = new List<string>();
                //    List<string> listB = new List<string>();
                //    int i = 0;
                //    while (!reader.EndOfStream && i < 20000)
                //    {
                //        var line = reader.ReadLine();
                //        var values = line.Split(';');

                //        listA.Add(values[0]);
                //        listB.Add(values[1]);
                //        i++;
                //    }
                //}
                var aerospikeClient = new AerospikeClient("18.235.70.103", 3000);
                string nameSpace="AirEngine";
                string sets = "Shrijeet";
                int columnFlag = 0;
                int count = 0;
                using (TextFieldParser textParser = new TextFieldParser(@"C:\tweets4.csv"))
                {
                    textParser.SetDelimiters(",");
                    while (!textParser.EndOfData && count <20000)
                    {
                        if(columnFlag==0)
                        {
                            textParser.ReadFields();
                            columnFlag = 1;
                            continue;
                        }
                        if (count <= 20000)
                        {
                            count++;
                            string[] field = textParser.ReadFields();
                            var key = new Key(nameSpace, sets, field[7]);
                            /*text	favorited	favoriteCount	replyToSN	created	truncated	replyToSID	id	replyToUID	statusSource	screenName	retweetCount	isRetweet	retweeted	longitude	latitude	timestamp	us_timestamp	date	last_name	first_name	birthday	gender	type	state	district	party	url	address	phone	contact_form	rss_url	twitter	facebook	youtube	youtube_id	bioguide_id	thomas_id	opensecrets_id	lis_id	cspan_id	govtrack_id	votesmart_id
*/                          
                            aerospikeClient.Put(new WritePolicy(), key, new Bin[] 
                            {
                                new Bin("text",field[0]),
                                new Bin("favorited",field[1]),
                                new Bin("favoriteCount",field[2]),
                                new Bin("replyToSN",field[3]),
                                new Bin("created",field[4]),
                                new Bin("truncated",field[5]),
                                new Bin("replyToSID",field[6]),
                                new Bin("id",field[7]),
                                new Bin("replyToUID",field[8]),
                                new Bin("statusSource",field[9]),
                                new Bin("screenName",field[10]),
                                new Bin("retweetCount",field[11]),
                                new Bin("isRetweet",field[12]),
                                new Bin("retweeted",field[13]),
                                new Bin("longitude",field[14]),
                                new Bin("latitude",field[15]),
                                new Bin("timestamp",field[16]),
                                new Bin("us_timestamp",field[17]),
                                new Bin("date",field[18]),
                                new Bin("last_name",field[19]),
                                new Bin("first_name", field[20]),
                                new Bin("birthday", field[21]),
                                new Bin("gender", field[22]),
                                new Bin("type", field[23]),
                                new Bin("state", field[24]),
                                new Bin("district", field[25]),
                                new Bin("party",field[26]),
                                new Bin("url", field[27]),
                                new Bin("address", field[28]),
                                new Bin("phone", field[29]),
                                new Bin("contact_form", field[30]),
                                new Bin("rss_url", field[31]),
                                new Bin("twitter", field[32]),
                                new Bin("facebook", field[33]),
                                new Bin("youtube", field[34]),
                                new Bin("youtube_id", field[35]),
                                new Bin("bioguide_id", field[36]),
                                new Bin("thomas_id", field[37]),
                                new Bin("opensecrets_id", field[38]),
                                new Bin("lis_id", field[39]),
                                new Bin("cspan_id", field[40]),
                                new Bin("govtrack_id", field[41]),
                                new Bin("votesmart_id", field[42])
                            });
                       
                        }
                    }

                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
