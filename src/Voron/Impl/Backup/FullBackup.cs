﻿// -----------------------------------------------------------------------
//  <copyright file="FullBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using Sparrow;
using Sparrow.Backups;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron.Data.Tables;
using Voron.Global;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;
using Voron.Util.Settings;

namespace Voron.Impl.Backup
{
    public sealed unsafe class FullBackup
    {
        public sealed class StorageEnvironmentInformation
        {
            public string Folder { set; get; }
            public string Name { get; set; }
            public StorageEnvironment Env { get; set; }
        }

        public void ToFile(
            StorageEnvironment env,
            VoronPathSetting backupPath,
            SnapshotBackupCompressionAlgorithm compressionAlgorithm,
            CompressionLevel compressionLevel = CompressionLevel.Optimal,
            Action<(string Message, int FilesCount)> infoNotify = null)
        {
            infoNotify ??= _ => { };
            
            infoNotify(("Voron backup db started", 0));
            
            using (var file = SafeFileStream.Create(backupPath.FullPath, FileMode.Create))
            {
                using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
                {
                    infoNotify(("Voron backup started", 0));
                    var dataPager = env.DataPager;
                    var copier = new DataCopier(Constants.Storage.PageSize * 16);
                    Backup(env, compressionAlgorithm, compressionLevel, dataPager, package, string.Empty, copier, infoNotify);
            
                    file.Flush(true); // make sure that we fully flushed to disk
            
                }
            }
            
            infoNotify(("Voron backup db finished", 0));
        }
        /// <summary>
        /// Do a full backup of a set of environments. Note that the order of the environments matter!
        /// </summary>
        public void ToFile(IEnumerable<StorageEnvironmentInformation> envs,
            ZipArchive archive,
            SnapshotBackupCompressionAlgorithm compressionAlgorithm,
            CompressionLevel compressionLevel,
            Action<(string Message, int FilesCount)> infoNotify = null,
            CancellationToken cancellationToken = default)
        {
            infoNotify ??= (_ => { });
            infoNotify(("Voron backup db started", 0));

            foreach (var e in envs)
            {
                infoNotify(($"Voron backup {e.Name} started", 0));
                var basePath = Path.Combine(e.Folder, e.Name);

                throw new NotImplementedException();
                // var env = e.Env;
                // var dataPager = env.Options.DataPager;
                // var copier = new DataCopier(Constants.Storage.PageSize * 16);
                // Backup(env, compressionAlgorithm, compressionLevel, dataPager, archive, basePath, copier, infoNotify, cancellationToken);
            }

            infoNotify(("Voron backup db finished", 0));
        }

        private static void Backup(
            StorageEnvironment env,
            SnapshotBackupCompressionAlgorithm compressionAlgorithm,
            CompressionLevel compressionLevel,
            Pager2 dataPager,
            ZipArchive zipArchive,
            string basePath,
            DataCopier copier,
            Action<(string Message, int FilesCount)> infoNotify,
            CancellationToken cancellationToken = default)
        {
            var usedJournals = new List<JournalFile>();
            long lastWrittenLogPage = -1;
            long lastWrittenLogFile = -1;
            LowLevelTransaction txr = null;
            var backupSuccess = false;

            var package = new BackupZipArchive(zipArchive, compressionAlgorithm, compressionLevel);

            try
            {
                long allocatedPages;
                var writePersistentContext = new TransactionPersistentContext(true);
                var readPersistentContext = new TransactionPersistentContext(true);
                using (env.Journal.Applicator.TakeFlushingLock()) // prevent from running JournalApplicator.UpdateDatabaseStateAfterSync() concurrently
                using (var txw = env.NewLowLevelTransaction(writePersistentContext, TransactionFlags.ReadWrite)) // so no new journal files will be created
                {
                    txr = env.NewLowLevelTransaction(readPersistentContext, TransactionFlags.Read);// now have snapshot view
                    allocatedPages = txw.DataPagerState.NumberOfAllocatedPages;

                    Debug.Assert(HeaderAccessor.HeaderFileNames.Length == 2);
                    infoNotify(($"Voron copy headers for {basePath}", 2));
                    env.HeaderAccessor.CopyHeaders(package, copier, env.Options, basePath);

                    env._forTestingPurposes?.ActionToCallDuringFullBackupRighAfterCopyHeaders?.Invoke();

                    // journal files snapshot
                    var files = env.Journal.Files; // thread safety copy

                    JournalInfo journalInfo = env.HeaderAccessor.Get(ptr => ptr->Journal);
                    var startingJournal = journalInfo.LastSyncedJournal;
                    if (env.Options.JournalExists(startingJournal) == false &&
                        journalInfo.Flags.HasFlag(JournalInfoFlags.IgnoreMissingLastSyncJournal) ||
                        startingJournal == -1)
                    {
                        startingJournal++;
                    }

                    for (var journalNum = startingJournal;
                        journalNum <= journalInfo.CurrentJournal;
                        journalNum++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var journalFile = files.FirstOrDefault(x => x.Number == journalNum);
                        // first check journal files currently being in use
                        if (journalFile == null)
                        {
                            long journalSize;
                            using (var pager = env.Options.OpenJournalPager(journalNum, journalInfo))
                            {
                                journalSize = Bits.PowerOf2(pager.NumberOfAllocatedPages * Constants.Storage.PageSize);
                            }

                            journalFile = new JournalFile(env, env.Options.CreateJournalWriter(journalNum, journalSize), journalNum);
                        }

                        journalFile.AddRef();
                        usedJournals.Add(journalFile);
                    }

                    if (env.Journal.CurrentFile != null)
                    {
                        lastWrittenLogFile = env.Journal.CurrentFile.Number;
                        lastWrittenLogPage = env.Journal.CurrentFile.WritePosIn4KbPosition - 1;
                    }

                    // txw.Commit(); intentionally not committing
                }

                // data file backup
                var dataPart = package.CreateEntry(Path.Combine(basePath, Constants.DatabaseFilename));
                Debug.Assert(dataPart != null);

                if (allocatedPages > 0) //only true if dataPager is still empty at backup start
                {
                    using (var dataStream = dataPart.Open())
                    {
                        // now can copy everything else
                        copier.ToStream(dataPager, txr, 0, allocatedPages, dataStream, message => infoNotify((message, 0)), cancellationToken);
                    }
                    infoNotify(("Voron copy data file", 1));
                }

                try
                {
                    if (env.Options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                    {
                        foreach (var recoveryFile in Directory.GetFiles(env.Options.BasePath.FullPath, TableValueCompressor.CompressionRecoveryExtensionGlob))
                        {
                            FileStream src;
                            try
                            {
                                src = File.Open(recoveryFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                            }
                            catch
                            {
                                // we can ignore a failure to read these, they are useful, but 
                                // not critical for the snapshot backup
                                continue;
                            }

                            using (src)
                            {
                                var srcFileName = Path.GetFileName(recoveryFile);

                                var recoveryPart = package.CreateEntry(Path.Combine(basePath, srcFileName));
                                using var dst = recoveryPart.Open();
                                src.CopyTo(dst);
                            }

                        }
                    }

                    long lastBackedupJournal = 0;
                    foreach (var journalFile in usedJournals)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var entryName = StorageEnvironmentOptions.JournalName(journalFile.Number);
                        var journalPart = package.CreateEntry(Path.Combine(basePath, entryName));

                        Debug.Assert(journalPart != null);

                        long pagesToCopy = journalFile.JournalWriter.NumberOfAllocated4Kb;
                        if (journalFile.Number == lastWrittenLogFile)
                            pagesToCopy = lastWrittenLogPage + 1;

                        using (var stream = journalPart.Open())
                        {
                            copier.ToStream(env, journalFile, 0, pagesToCopy, stream, message => infoNotify((message, 0)), cancellationToken);
                        }
                        infoNotify(($"Voron copy journal file {entryName}", 1));

                        lastBackedupJournal = journalFile.Number;
                    }

                    if (env.Options.IncrementalBackupEnabled)
                    {
                        env.HeaderAccessor.Modify(header =>
                        {
                            header->IncrementalBackup.LastBackedUpJournal = lastBackedupJournal;

                            //since we backed-up everything, no need to start next incremental backup from the middle
                            header->IncrementalBackup.LastBackedUpJournalPage = -1;
                        });
                    }
                    backupSuccess = true;
                }
                catch (Exception)
                {
                    backupSuccess = false;
                    throw;
                }
                finally
                {
                    var lastSyncedJournal = env.HeaderAccessor.Get(header => header->Journal).LastSyncedJournal;
                    foreach (var journalFile in usedJournals)
                    {
                        if (backupSuccess) // if backup succeeded we can remove journals
                        {
                            if (journalFile.Number < lastWrittenLogFile &&  // prevent deletion of the current journal and journals with a greater number
                                journalFile.Number < lastSyncedJournal) // prevent deletion of journals that aren't synced with the data file
                            {
                                journalFile.DeleteOnClose = true;
                            }
                        }

                        journalFile.Release();
                    }
                }
            }
            finally
            {
                txr?.Dispose();
            }
        }

        public void Restore(VoronPathSetting backupPath,
            VoronPathSetting voronDataDir,
            VoronPathSetting journalDir = null,
            Action<string> onProgress = null,
            CancellationToken cancellationToken = default)
        {
            using (var zip = ZipFile.Open(backupPath.FullPath, ZipArchiveMode.Read, System.Text.Encoding.UTF8))
                Restore(zip.Entries, voronDataDir, journalDir, onProgress, cancellationToken);
        }

        public void Restore(IEnumerable<ZipArchiveEntry> entries,
            VoronPathSetting voronDataDir,
            VoronPathSetting journalDir = null,
            Action<string> onProgress = null,
            CancellationToken cancellationToken = default)
        {
            journalDir ??= voronDataDir.Combine("Journals");

            if (Directory.Exists(voronDataDir.FullPath) == false)
                Directory.CreateDirectory(voronDataDir.FullPath);

            if (Directory.Exists(journalDir.FullPath) == false)
                Directory.CreateDirectory(journalDir.FullPath);

            onProgress?.Invoke("Starting snapshot restore");

            foreach (var entry in entries)
            {
                var dst = string.Equals(Path.GetExtension(entry.Name), ".journal", StringComparison.OrdinalIgnoreCase)
                    ? journalDir
                    : voronDataDir;

                var sw = Stopwatch.StartNew();
                var swForProgress = Stopwatch.StartNew();
                long totalRead = 0;

                if (Directory.Exists(dst.FullPath) == false)
                    Directory.CreateDirectory(dst.FullPath);

                using (var zipEntry = entry.Open())
                using (var decompressionStream = GetDecompressionStream(zipEntry))
                using (var output = SafeFileStream.Create(dst.Combine(entry.Name).FullPath, FileMode.CreateNew))
                {
                    // we don't know the uncompressed size of the file for the zstd stream (the uncompressed size is stored at the end of the file).
                    var isZstd = decompressionStream is ZstdStream;
                    decompressionStream.CopyTo(output, readCount =>
                    {
                        totalRead += readCount;
                        if (swForProgress.ElapsedMilliseconds > 5000)
                        {
                            swForProgress.Restart();
                            onProgress?.Invoke($"Restoring file: '{entry.Name}', {new Size(totalRead, SizeUnit.Bytes)}{(isZstd ? string.Empty : "/" + new Size(entry.Length, SizeUnit.Bytes))}");
                        }
                    }, cancellationToken);
                }

                onProgress?.Invoke($"Restored file: '{entry.Name}' to: '{dst}', " +
                                   $"size: {new Size(totalRead, SizeUnit.Bytes)}, " +
                                   $"took: {sw.ElapsedMilliseconds:#,#;;0}ms");
            }
        }

        internal static Stream GetDecompressionStream(Stream stream)
        {
            var buffer = new byte[4];

            var read = stream.Read(buffer, 0, buffer.Length);
            if (read == 0)
                throw new InvalidOperationException("Empty stream");

            var backupStream = new BackupStream(stream, buffer);

            if (read != buffer.Length)
                return backupStream;

            const uint zstdMagicNumber = 0xFD2FB528;
            var readMagicNumber = BitConverter.ToUInt32(buffer);
            if (readMagicNumber == zstdMagicNumber)
                return ZstdStream.Decompress(backupStream);

            return backupStream;
        }
    }
}
