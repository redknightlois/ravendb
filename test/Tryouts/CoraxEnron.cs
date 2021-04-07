using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Corax;
using MimeKit;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Tryouts
{
    class CoraxEnron
    {
        public static string DirectoryEnron = "corax-enron";

        public static void IndexEnronInCorax(bool recreateDatabase = true)
        {
            var path = Path.Join("..", Enron.DatasetFile);

            if (Directory.Exists(DirectoryEnron))
                Directory.Delete(DirectoryEnron, true);

            using var options = StorageEnvironmentOptions.ForPath(DirectoryEnron);
            using var env = new StorageEnvironment(options);

            var sp = Stopwatch.StartNew();

            using var tar = SharpCompress.Readers.Tar.TarReader.Open(File.OpenRead(path));

            using var ctx = JsonOperationContext.ShortTermSingleUse();
            using var indexWriter = new IndexWriter(env);

            int i = 0;
            while (tar.MoveToNextEntry())
            {
                if (tar.Entry.IsDirectory)
                    continue;

                using var s = tar.OpenEntryStream();
                var msg = MimeMessage.Load(s);

                var value = new DynamicJsonValue
                {
                    ["Bcc"] = (msg.Bcc ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString()).ToArray(),
                    ["Cc"] = (msg.Cc ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString()).ToArray(),
                    ["To"] = (msg.To ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString()).ToArray(),
                    ["From"] = msg.From?.FirstOrDefault()?.ToString(),
                    ["ReplyTo"] = msg.ReplyTo?.FirstOrDefault()?.ToString(),
                    ["Body"] = msg.GetTextBody(MimeKit.Text.TextFormat.Plain).Split(' '),
                    ["References"] = (msg.References ?? Enumerable.Empty<string>()).ToArray(),
                    ["Subject"] = msg.Subject.Split(' '),
                    ["MessageId"] = msg.MessageId,
                    ["Date"] = msg.Date.ToString("O"),
                    ["Importance"] = msg.Importance.ToString(),
                    ["Priority"] = msg.Priority.ToString(),
                };

                foreach (var item in msg.Headers ?? new HeaderList())
                {
                    if (item.Value.Length > 512)
                        continue;

                    string headerName = item.Id.ToHeaderName();
                    if (headerName.Length < 128)
                        value[headerName] = item.Value;
                }

                if (msg.Sender != null)
                    value["Sender"] = msg.Sender.ToString();

                if (msg.InReplyTo != null)
                    value["InReplyTo"] = msg.InReplyTo;

                var entry = ctx.ReadObject(value, $"entry/{i}");
                indexWriter.Index($"entry/{i}", entry);

                if (i % 4096 * 8 == 0)
                {
                    indexWriter.Commit();
                }

                i++;
            }

            indexWriter.Commit();
            Console.WriteLine(sp.ElapsedMilliseconds);
        }
    }
}
