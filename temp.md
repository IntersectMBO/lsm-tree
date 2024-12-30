TASTY_TIMEOUT=5m cabal run lsm-tree-test -- -p "Internal.FS" --quickcheck-replay="(SMGen 10070302427855069393 7106224782885533715,42)"

```
lsm-tree
  Test.Database.LSMTree.Internal.FS
    prop_fsRoundtripSnapshot: FAIL (36.36s)
      *** Failed! Falsified (after 1 test and 149 shrinks):
      TestErrors {createSnapshotErrors = Errors {hGetBufSomeAtE = [Just (Left FsReachedEOF)] ++ ...}, openSnapshotErrors = Errors {}}
      Positive {getPositive = Small {getSmall = 1}}
      [(0,Delete),(0,Delete),(1,InsertWithBlob 0 0),(0,Delete),(0,Delete),(0,Delete),(0,Delete),(0,Delete),(0,Delete),(0,Delete),(0,Delete)]
      createSnapshot result: Left (FsError {fsErrorType = FsReachedEOF, fsErrorPath = active/7.blobs, fsErrorString = "simulated error: hGetBufSomeAt", fsErrorNo = Nothing, fsErrorStack = CallStack (from HasCallStack):
        prettyCallStack, called at src/System/FS/Sim/Error.hs:764:27 in fs-sim-0.3.1.0-4fa60cf9efc161a88ca35d5fd08c059c9457164b29c3ebf03caa22b1763b1ff5:System.FS.Sim.Error
        hGetBufSomeAtWithErr, called at src/System/FS/Sim/Error.hs:556:27 in fs-sim-0.3.1.0-4fa60cf9efc161a88ca35d5fd08c059c9457164b29c3ebf03caa22b1763b1ff5:System.FS.Sim.Error
        hGetBufSomeAt, called at src/System/FS/API.hs:183:5 in fs-api-0.3.0.1-688e690e27fe207b7c7acbf68a7570aecf40bb72d006ea1c22d49f43537a73ea:System.FS.API
        a use of `hGetBufSomeAt', called at src/System/FS/API.hs:297:24 in fs-api-0.3.0.1-688e690e27fe207b7c7acbf68a7570aecf40bb72d006ea1c22d49f43537a73ea:System.FS.API
        hGetBufExactlyAt, called at src/Database/LSMTree/Internal/BlobFile.hs:94:10 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.BlobFile
        readBlobRaw, called at src/Database/LSMTree/Internal/BlobRef.hs:165:5 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.BlobRef
        readRawBlobRef, called at src/Database/LSMTree/Internal/ChecksumHandle.hs:193:13 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.ChecksumHandle
        copyBlob, called at src/Database/LSMTree/Internal/RunBuilder.hs:134:22 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.RunBuilder
        addKeyOp, called at src/Database/LSMTree/Internal/Merge.hs:378:7 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.Merge
        writeSerialisedEntry, called at src/Database/LSMTree/Internal/Merge.hs:345:7 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.Merge
        writeReaderEntry, called at src/Database/LSMTree/Internal/Merge.hs:270:15 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.Merge
        steps, called at src/Database/LSMTree/Internal/MergingRun.hs:463:36 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.MergingRun
        stepMerge, called at src/Database/LSMTree/Internal/MergingRun.hs:327:27 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.MergingRun
        supplyCredits, called at src/Database/LSMTree/Internal/MergeSchedule.hs:801:11 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.MergeSchedule
        supplyCredits, called at src/Database/LSMTree/Internal.hs:1181:11 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal
        createSnapshot, called at test/Test/Database/LSMTree/Internal/FS.hs:63:41 in lsm-tree-0.1.0.0-inplace-lsm-tree-test:Test.Database.LSMTree.Internal.FS, fsLimitation = False})
      openSnapshot result: Left (ErrSnapshotNotExists "snap")
      close exception: Left (CommitActionRegistryError (ActionError RefDoubleRelease RefId 455819 CallStack (from HasCallStack):
        mkAction, called at src-control/Control/ActionRegistry.hs:546:40 in lsm-tree-0.1.0.0-inplace-control:Control.ActionRegistry
        delayedCommit, called at src/Database/LSMTree/Internal/MergeSchedule.hs:342:41 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.MergeSchedule
        releaseIncomingRun, called at src/Database/LSMTree/Internal/MergeSchedule.hs:321:7 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.MergeSchedule
        releaseLevels, called at src/Database/LSMTree/Internal/MergeSchedule.hs:141:5 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal.MergeSchedule
        releaseTableContent, called at src/Database/LSMTree/Internal.hs:781:11 in lsm-tree-0.1.0.0-inplace:Database.LSMTree.Internal
        close, called at test/Test/Database/LSMTree/Internal/FS.hs:113:21 in lsm-tree-0.1.0.0-inplace-lsm-tree-test:Test.Database.LSMTree.Internal.FS
        withTableTry, called at test/Test/Database/LSMTree/Internal/FS.hs:59:11 in lsm-tree-0.1.0.0-inplace-lsm-tree-test:Test.Database.LSMTree.Internal.FS :| []))
      Use --quickcheck-replay="(SMGen 10070302427855069393 7106224782885533715,42)" to reproduce.

1 out of 1 tests failed (36.36s)
```

cabal run control-test -- -p "prop_modifyWithActionRegistry"