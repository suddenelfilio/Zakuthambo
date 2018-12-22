using System;

namespace Zakuthambo
{
    public class CosmosDbOptions
    {
        public Uri CosmosDbEndpointUri { get; set; }

        public string Key { get; set; }

        public string Database { get; set; }
    }
}