﻿// -----------------------------------------------------------------------
//  <copyright file="SubscriptionsController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Database.Actions;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers
{
	public class SubscriptionsController : RavenDbApiController
	{
		[HttpPost]
		[Route("subscriptions/create")]
		[Route("databases/{databaseName}/subscriptions/create")]
		public async Task<HttpResponseMessage> Create()
		{
			var subscriptionCriteria = await ReadJsonObjectAsync<SubscriptionCriteria>();

			if(subscriptionCriteria == null)
				throw new InvalidOperationException("Criteria cannot be null");

			var id = Database.Subscriptions.CreateSubscription(subscriptionCriteria);

			return GetMessageWithObject(id, HttpStatusCode.Created);
		}

		[HttpPost]
		[Route("subscriptions/open")]
		[Route("databases/{databaseName}/subscriptions/open")]
		public async Task<HttpResponseMessage> Open(long id)
		{
			if (Database.Subscriptions.GetSubscriptionDocument(id) == null)
				return GetMessageWithString("Cannot find a subscription for the specified id: " + id, HttpStatusCode.NotFound);

			var options = await ReadJsonObjectAsync<SubscriptionBatchOptions>();

			if (options == null)
				throw new InvalidOperationException("Options cannot be null");

			string connectionId;

			if (Database.Subscriptions.TryOpenSubscription(id, options, out connectionId) == false)
				return GetMessageWithString("Subscription is already in use. There can be only a single open subscription connection per subscription.", HttpStatusCode.Gone);

			return GetMessageWithString(connectionId);
		}

		[HttpGet]
		[Route("subscriptions/pull")]
		[Route("databases/{databaseName}/subscriptions/pull")]
		public HttpResponseMessage Pull(long id, string connection)
		{
			Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);

			var pushStreamContent = new PushStreamContent((stream, content, transportContext) => StreamToClient(id, Database.Subscriptions, stream))
			{
				Headers =
				{
					ContentType = new MediaTypeHeaderValue("application/json")
					{
						CharSet = "utf-8"
					}
				}
			};

			return new HttpResponseMessage(HttpStatusCode.OK)
			{
				Content = pushStreamContent
			};
		}

		[HttpPost]
		[Route("subscriptions/acknowledgeBatch")]
		[Route("databases/{databaseName}/subscriptions/acknowledgeBatch")]
		public HttpResponseMessage AcknowledgeBatch(long id, string lastEtag, string connection)
		{
			Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);

			try
			{
				Database.Subscriptions.AcknowledgeBatchProcessed(id, Etag.Parse(lastEtag));
			}
			catch (TimeoutException)
			{
				return GetMessageWithString("The subscription cannot be acknowledged because the timeout has been reached.", HttpStatusCode.RequestTimeout);
			}

			return GetEmptyMessage();
		}

		[HttpPost]
		[Route("subscriptions/close")]
		[Route("databases/{databaseName}/subscriptions/close")]
		public HttpResponseMessage Close(long id, string connection)
		{
			Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);

			Database.Subscriptions.ReleaseSubscription(id);

			return GetEmptyMessage();
		}

		private void StreamToClient(long id, SubscriptionActions subscriptions, Stream stream)
		{
			var sentDocuments = false;

			using (var streamWriter = new StreamWriter(stream))
			using (var writer = new JsonTextWriter(streamWriter))
			{
				var options = subscriptions.GetBatchOptions(id);

				writer.WriteStartObject();
				writer.WritePropertyName("Results");
				writer.WriteStartArray();

				using (var cts = new CancellationTokenSource())
				using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
				{
					Etag lastProcessedDocEtag = null;
					var batchSize = 0;
					var batchDocCount = 0;
					var hasMoreDocs = false;

					var subscriptionDocument = subscriptions.GetSubscriptionDocument(id);
					var startEtag = subscriptionDocument.AckEtag;
					var criteria = subscriptionDocument.Criteria;

					do
					{
						Database.TransactionalStorage.Batch(accessor =>
						{
							// we may be sending a LOT of documents to the user, and most 
							// of them aren't going to be relevant for other ops, so we are going to skip
							// the cache for that, to avoid filling it up very quickly
							using (DocumentCacher.SkipSettingDocumentsInDocumentCache())
							{
								Database.Documents.GetDocuments(-1, options.MaxDocCount - batchDocCount, startEtag, cts.Token, doc =>
								{
									timeout.Delay();

									if (options.MaxSize.HasValue && batchSize >= options.MaxSize)
										return;

									if (batchDocCount >= options.MaxDocCount)
										return;

									lastProcessedDocEtag = doc.Etag;

									if (doc.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
										return;

									if (MatchCriteria(criteria, doc) == false) 
										return;

									doc.ToJson().WriteTo(writer);
									writer.WriteRaw(Environment.NewLine);

									batchSize += doc.SerializedSizeOnDisk;
									batchDocCount++;
								});
							}

							if (lastProcessedDocEtag == null)
								hasMoreDocs = false;
							else
							{
								var lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
								hasMoreDocs = EtagUtil.IsGreaterThan(lastDocEtag, lastProcessedDocEtag);

								startEtag = lastProcessedDocEtag;
							}
						});
					} while (hasMoreDocs && batchDocCount < options.MaxDocCount && (options.MaxSize.HasValue == false || batchSize < options.MaxSize));

					writer.WriteEndArray();

					if (batchDocCount > 0)
					{
						writer.WritePropertyName("LastProcessedEtag");
						writer.WriteValue(lastProcessedDocEtag.ToString());

						sentDocuments = true;
					}

					writer.WriteEndObject();
					writer.Flush();
				}
			}

			if (sentDocuments)
				subscriptions.UpdateBatchSentTime(id);
		}

		private static bool MatchCriteria(SubscriptionCriteria criteria, JsonDocument doc)
		{
			if (criteria.BelongsToCollection != null &&
			    criteria.BelongsToCollection.Equals(doc.Metadata.Value<string>(Constants.RavenEntityName), StringComparison.OrdinalIgnoreCase) == false)
				return false;

			if (criteria.KeyStartsWith != null && doc.Key.StartsWith(criteria.KeyStartsWith) == false)
				return false;

			if (criteria.PropertiesMatch != null)
			{
				foreach (var match in criteria.PropertiesMatch)
				{
					RavenJToken value;
					if (doc.DataAsJson.TryGetValue(match.Key, out value) == false)
						return false;

					if (RavenJToken.DeepEquals(value, match.Value) == false)
						return false;
				}
			}

			if (criteria.PropertiesNotMatch != null)
			{
				foreach (var notMatch in criteria.PropertiesNotMatch)
				{
					RavenJToken value;
					if (doc.DataAsJson.TryGetValue(notMatch.Key, out value))
					{
						if (RavenJToken.DeepEquals(value, notMatch.Value))
							return false;
					}
				}
			}

			return true;
		}
	}
}