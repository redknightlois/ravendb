using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using MimeKit;
using Voron;

namespace Tryouts
{
    class LuceneEnron
    {
        public static string EnronLucene = "enron.lucene";

        public static void IndexEnronInLucene(bool recreateDatabase = true)
        {
            var path = Enron.DatasetFile;
            var sp = Stopwatch.StartNew();

            using var dir = FSDirectory.Open(EnronLucene);
            var analyzer = new StandardAnalyzer(Lucene.Net.Util.LuceneVersion.LUCENE_48);
            using var writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.Net.Util.LuceneVersion.LUCENE_48, analyzer));

            using var tar = SharpCompress.Readers.Tar.TarReader.Open(File.OpenRead(path));
            while (tar.MoveToNextEntry())
            {
                if (tar.Entry.IsDirectory)
                    continue;
                using var s = tar.OpenEntryStream();
                var msg = MimeMessage.Load(s);

                var doc = new Document();
                foreach (var item in msg.Bcc ?? Enumerable.Empty<InternetAddress>())
                {
                    doc.Add(new StringField("Bcc", item.ToString(), Field.Store.NO));
                }

                foreach (var item in msg.Cc ?? Enumerable.Empty<InternetAddress>())
                {
                    doc.Add(new StringField("Cc", item.ToString(), Field.Store.NO));
                }

                foreach (var item in msg.To ?? Enumerable.Empty<InternetAddress>())
                {
                    doc.Add(new StringField("To", item.ToString(), Field.Store.NO));
                }

                foreach (var item in msg.From ?? Enumerable.Empty<InternetAddress>())
                {
                    doc.Add(new StringField("From", item.ToString(), Field.Store.NO));
                }

                foreach (var item in msg.ReplyTo ?? Enumerable.Empty<InternetAddress>())
                {
                    doc.Add(new StringField("ReplyTo", item.ToString(), Field.Store.NO));
                }

                foreach (var item in msg.References ?? Enumerable.Empty<string>())
                {
                    doc.Add(new StringField("References", item, Field.Store.NO));
                }

                foreach (var item in msg.Headers ?? new HeaderList())
                {
                    if (item.Value.Length > 512)
                        continue;
                    doc.Add(new StringField(item.Id.ToHeaderName(), item.Value, Field.Store.NO));
                }

                if (msg.Sender != null)
                    doc.Add(new StringField("Sender", msg.Sender.ToString(), Field.Store.NO));

                doc.Add(new TextField("Body", msg.GetTextBody(MimeKit.Text.TextFormat.Plain), Field.Store.NO));
                doc.Add(new TextField("Subject", msg.Subject, Field.Store.NO));
                doc.Add(new StringField("MessageId", msg.MessageId, Field.Store.NO));
                if (msg.InReplyTo != null)
                    doc.Add(new StringField("InReplyTo", msg.InReplyTo, Field.Store.NO));
                doc.Add(new StringField("Date", msg.Date.ToString("O"), Field.Store.NO));
                doc.Add(new StringField("Importance", msg.Importance.ToString(), Field.Store.NO));
                doc.Add(new StringField("Priority", msg.Priority.ToString(), Field.Store.NO));

                writer.AddDocument(doc);
            }

            writer.Flush(true, true);
            Console.WriteLine(sp.ElapsedMilliseconds);
        }
    }
}
