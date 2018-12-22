using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;

namespace Zakuthambo
{
    public class CosmosDbRepository<T> : IDisposable where T : class
    {
        private readonly CosmosDbOptions _options;
        private bool disposedValue = false;
        private readonly CosmosClient _client;

        public CosmosDatabase Database { get; private set; }
        public CosmosContainer Container { get; private set; }

        public string ContainerName { get; }
        public string PartitionPath { get; }

        public CosmosDbRepository(IOptions<CosmosDbOptions> options, CosmosClient client, string container, string partitionPath)
        : this(options?.Value, client, container, partitionPath)
        {
        }

        public CosmosDbRepository(CosmosDbOptions options, CosmosClient client, string container, string partitionPath)
        {
            if (string.IsNullOrWhiteSpace(container))
                throw new ArgumentException("message", nameof(container));
            if (string.IsNullOrWhiteSpace(partitionPath))
                throw new ArgumentException("message", nameof(partitionPath));

            _options = options ?? throw new ArgumentNullException(nameof(options));
            ValidateOptions();
            _client = client;
            ContainerName = container;
        }

        public async Task EnsureContainer(int? throughput = null, CosmosRequestOptions requestOptions = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            Database = await _client.Databases.CreateDatabaseIfNotExistsAsync(_options.Database, throughput, requestOptions, cancellationToken);
            CosmosContainerSettings containerDefinition = new CosmosContainerSettings(id: ContainerName, partitionKeyPath: PartitionPath);
            containerDefinition.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });
            Container = await Database.Containers.CreateContainerIfNotExistsAsync(containerSettings: containerDefinition, throughput, requestOptions, cancellationToken);
        }

        public async Task<T> GetAsync(string partitionKey, string id)
        {
            CosmosItemResponse<T> response = await Container.Items.ReadItemAsync<T>(partitionKey, id);
            if (response.StatusCode == HttpStatusCode.NotFound) return default(T);
            return (T)response;
        }

        [Obsolete("Use the GetAsync(string partitionKey,string id) method instead. Non-partitioned containers will eventually disappear and query will be done on partitions.")]
        public async Task<T> GetAsync(string id)
        {
            var query = new CosmosSqlQueryDefinition("SELECT * from c where c.id=@id").UseParameter("@id", id);
            IEnumerable<T> response = await GetItemsAsync(query, maxItemCount: 1);
            if (response == null || !response.Any()) return default(T);

            return response.First();
        }

        public async Task<IEnumerable<T>> GetItemsAsync(string query, int concurrency = 0, int maxBufferedItemCount = 0, int? maxItemCount = null)
        {
            return await GetItemsAsync(new CosmosSqlQueryDefinition(query), concurrency, maxBufferedItemCount, maxItemCount);
        }

        public async Task<IEnumerable<T>> GetItemsAsync(string query, string partitionKey, int maxBufferedItemCount = 0, int? maxItemCount = null)
        {
            return await GetItemsAsync(new CosmosSqlQueryDefinition(query), partitionKey, maxBufferedItemCount, maxItemCount);
        }

        public async Task<IEnumerable<T>> GetItemsAsync(CosmosSqlQueryDefinition query, int concurrency = 0, int maxBufferedItemCount = 0, int? maxItemCount = null)
        {
            var itemQuery = Container.Items.CreateItemQuery<T>(
                query,
                maxItemCount: maxItemCount,
                maxConcurrency: concurrency,
                requestOptions: new CosmosQueryRequestOptions { MaxBufferedItemCount = maxBufferedItemCount });
            List<T> items = new List<T>();
            while (itemQuery.HasMoreResults)
            {
                foreach (T item in await itemQuery.FetchNextSetAsync())
                {
                    items.Add(item);
                }
            }

            return items;
        }

        public async Task<IEnumerable<T>> GetItemsAsync(CosmosSqlQueryDefinition query, string paritionKey, int maxBufferedItemCount = 0, int? maxItemCount = null)
        {
            var itemQuery = Container.Items.CreateItemQuery<T>(
                query,
                maxItemCount: maxItemCount,
                partitionKey: paritionKey,
                requestOptions: new CosmosQueryRequestOptions { MaxBufferedItemCount = maxBufferedItemCount });
            List<T> items = new List<T>();
            while (itemQuery.HasMoreResults)
            {
                foreach (T item in await itemQuery.FetchNextSetAsync())
                {
                    items.Add(item);
                }
            }

            return items;
        }

        public async Task<T> Create(T item, string partitionKey, CosmosItemRequestOptions requestOptions = default(CosmosItemRequestOptions), CancellationToken cancellationToken = default(CancellationToken))
        {
            var response = await Container.Items.CreateItemAsync(partitionKey, item, requestOptions: requestOptions, cancellationToken: cancellationToken);
            return response.Resource;
        }

        public async Task<T> Upsert(T item, string partitionKey, CosmosItemRequestOptions requestOptions = default(CosmosItemRequestOptions), CancellationToken cancellationToken = default(CancellationToken))
        {
            var response = await Container.Items.UpsertItemAsync(partitionKey, item, requestOptions: requestOptions, cancellationToken: cancellationToken);
            return response.Resource;
        }

        public async Task<T> Replace(T item, string partitionKey, string id, CosmosItemRequestOptions requestOptions = default(CosmosItemRequestOptions), CancellationToken cancellationToken = default(CancellationToken))
        {
            var response = await Container.Items.ReplaceItemAsync(partitionKey, id, item, requestOptions: requestOptions, cancellationToken: cancellationToken);
            return response.Resource;
        }

        public async Task Delete(string partitionKey, string id, CosmosItemRequestOptions requestOptions = default(CosmosItemRequestOptions), CancellationToken cancellationToken = default(CancellationToken))
        {
            await Container.Items.DeleteItemAsync<T>(partitionKey, id, requestOptions: requestOptions, cancellationToken: cancellationToken);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _client.Dispose();
                }
                disposedValue = true;
            }
        }

        private void ValidateOptions()
        {
            if (_options == null)
            {
                throw new ArgumentNullException("options");
            }

            if (_options.CosmosDbEndpointUri == null) throw new ArgumentException($"The {nameof(_options.CosmosDbEndpointUri)} cannot be null.", nameof(_options.CosmosDbEndpointUri));
            if (string.IsNullOrWhiteSpace(_options.Key)) throw new ArgumentException($"The {nameof(_options.Key)} cannot be null.", nameof(_options.Key));
            if (string.IsNullOrWhiteSpace(_options.Database)) throw new ArgumentException($"The {nameof(_options.Database)} cannot be null.", nameof(_options.Database));


        }
    }
}
