﻿using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Confluent.Kafka;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.ETL.Queue;
using Sparrow.Logging;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public static class QueueHelper
{
    public static IProducer<string, byte[]> CreateKafkaClient(QueueConnectionString connectionString, string transactionalId, Logger logger,
        X509Certificate2 certificate = null)
    {
        ProducerConfig config = new()
        {
            BootstrapServers = connectionString.KafkaConnectionSettings.BootstrapServers,
            TransactionalId = transactionalId,
            ClientId = transactionalId,
            EnableIdempotence = true
        };

        if (connectionString.KafkaConnectionSettings.UseRavenCertificate && certificate != null)
        {
            config.SslCertificatePem = Convert.ToBase64String(certificate.Export(X509ContentType.Cert));
            config.SslKeyPem = Convert.ToBase64String(certificate.Export(X509ContentType.Pkcs7));
            config.SecurityProtocol = SecurityProtocol.Ssl;
        }

        foreach (KeyValuePair<string, string> option in connectionString.KafkaConnectionSettings.ConnectionOptions)
        {
            config.Set(option.Key, option.Value);
        }

        IProducer<string, byte[]> producer = new ProducerBuilder<string, byte[]>(config)
            .SetErrorHandler((producer, error) =>
            {
                logger.Info($"Kafka producer error: {error.Reason}");
            })
            .Build();

        return producer;
    }

    public static IConnection CreateRabbitMqConnection(QueueConnectionString connectionString)
    {
        var connectionFactory = new ConnectionFactory() { Uri = new Uri(connectionString.RabbitMqConnectionSettings.ConnectionString) };
        return connectionFactory.CreateConnection();
    }
}
