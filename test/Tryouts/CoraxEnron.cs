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
        public const string DirectoryEnron = "enron-corax";
        private static char[] trimChars = {' ', ',', '\t', '\n'};

        private static IEnumerable<string> NormalizeEmails(IEnumerable<string> emails)
        {
            foreach (var email in emails)
            {
                if (email.Contains(','))
                {
                    foreach (var nEmail in email.Split(','))
                        yield return nEmail.Trim(trimChars);
                }
                else
                    yield return email.Trim(trimChars);
            }
        }

        public static void Index(bool recreateDatabase = true, string outputDirectory = ".")
        {
            var path = Path.Join("..", Enron.DatasetFile);

            string storagePath = Path.Join(outputDirectory, DirectoryEnron);
            if (Directory.Exists(storagePath))
                Directory.Delete(storagePath, true);

            using var options = StorageEnvironmentOptions.ForPath(storagePath);
            using var env = new StorageEnvironment(options);

            var sp = Stopwatch.StartNew();
            var indexOnlySp = new Stopwatch();

            using var tar = SharpCompress.Readers.Tar.TarReader.Open(File.OpenRead(path));

            var indexWriter = new IndexWriter(env);

            int i = 0;
            long ms = 0;
            long justIndex = 0;
            
            var ctx = JsonOperationContext.ShortTermSingleUse();
            while (tar.MoveToNextEntry())
            {
                if (tar.Entry.IsDirectory)
                    continue;

                using var s = tar.OpenEntryStream();
                var msg = MimeMessage.Load(s);

                var value = new DynamicJsonValue
                {
                    ["Bcc"] = new DynamicJsonArray(NormalizeEmails((msg.Bcc ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString()))),
                    ["Cc"] = new DynamicJsonArray(NormalizeEmails((msg.Cc ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString()))),
                    ["To"] = new DynamicJsonArray(NormalizeEmails((msg.To ?? Enumerable.Empty<InternetAddress>()).Select(x => x.ToString()))),
                    ["From"] = msg.From?.FirstOrDefault()?.ToString(),
                    ["ReplyTo"] = msg.ReplyTo?.FirstOrDefault()?.ToString(),
                    ["Body"] = new DynamicJsonArray(msg.GetTextBody(MimeKit.Text.TextFormat.Plain).Split(trimChars)),
                    ["References"] = new DynamicJsonArray((msg.References ?? Enumerable.Empty<string>()).ToArray()),
                    ["Subject"] = new DynamicJsonArray(msg.Subject.Split(' ')),
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

                indexOnlySp.Restart();
                indexWriter.Index($"entry/{i}", entry);
                justIndex += indexOnlySp.ElapsedMilliseconds;

                if (i % 1024 * 16 == 0)
                {
                    ms += sp.ElapsedMilliseconds;
                    Console.WriteLine(sp.ElapsedMilliseconds);

                    sp.Restart();

                    indexWriter.Commit();
                    indexWriter.Dispose();
                    
                    indexWriter = new IndexWriter(env);
                    ctx = JsonOperationContext.ShortTermSingleUse();
                }

                i++;
            }

            indexWriter.Commit();
            indexWriter.Dispose();

            Console.WriteLine($"Indexing time: {justIndex}");
            Console.WriteLine($"Total execution time: {ms}");
        }
    }
}
